// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
oms is openM++ JSON web-service which is also used as simple web-server for openM++ UI html pages.

Web-service allow to view and update model database(s) and run openM++ models from models/bin subdirectory.
Web-server allow to serve static html (css, images, javascipt) content from html subdirectory.

Arguments for oms can be specified on command line or through .ini file:
  oms -ini my.ini
Command line arguments take precedence over ini-file options.

Following arguments supporetd by oms:

  -oms.ApiOnly false
if true then API only web-service, it is false by default and oms also act as http server for openM++ UI.

  -oms.RootDir some/path
root directory, default: current directory, must have html subdirectory unless -oms.ApiOnly true specified.

  -oms.ModelDir models/bin
models directory, default: models/bin, if relative then must be relative to root directory.
It must contain model.sqlite database files and model executables.

  -l localhost:4040
  -oms.Listen localhost:4040
address to listen, default: localhost:4040.
Use -l :4040 if you need to access oms web-service from other computer (make sure firewall configured properly).

  -oms.LogRequest false
if true then log HTTP requests on console and/or log file.

  -oms.ModelLogDir ../log
models log directory, default: if relative then must be relative to models directory.
Default value will produce model run log files in models/log subdirectory.

  -oms.MaxRowCount 100
default number of rows to return from read parameters or output tables, default: 100.
This value is used if web-service method call does not provide explicit number of rows to read.

  -oms.Languages en
comma-separated list of supported languages, default: en.
That list is matched with request language list and model language list in order to return proper text results.

  -oms.DoubleFormat %.15g
format to convert float or double value to string, default: %.15g.
OpenM++ is using hash digest to compare models, input parameters and output values.
By default float and double values converted into text with "%.15g" format.

  -oms.CodePage
"code page" to convert source file into utf-8, for example: windows-1252.
It is used only for compatibility with old Windows files.

Also oms support OpenM++ standard log settings (described in wiki at http://www.openmpp.org/wiki/):
  -OpenM.LogToConsole:     if true then log to standard output, default: true
  -OpenM.LogToFile:        if true then log to file
  -OpenM.LogFilePath:      path to log file, default = current/dir/exeName.log
  -OpenM.LogUseDailyStamp: if true then use dayily stamp in log file name (rotate log files dayily)
  -OpenM.LogUseTs:         if true then use time-stamp in log file name
  -OpenM.LogUsePid:        if true then use pid-stamp in log file name
  -OpenM.LogNoMsgTime:     if true then do not prefix log messages with date-time
  -OpenM.LogSql:           if true then log sql statements into log file
*/
package main

import (
	"errors"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/alexbrainman/odbc"
	"github.com/husobee/vestigo"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/text/language"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/omppLog"
)

// config keys to get values from ini-file or command line arguments.
const (
	rootDirArgKey      = "oms.RootDir"      // root directory, expected subdir: html
	modelDirArgKey     = "oms.ModelDir"     // models directory, if relative then must be relative to root directory
	modelLogDirArgKey  = "oms.ModelLogDir"  // models log directory, if relative then must be relative to models directory
	listenArgKey       = "oms.Listen"       // address to listen, default: localhost:4040
	listenShortKey     = "l"                // address to listen (short form)
	logRequestArgKey   = "oms.LogRequest"   // if true then log http request
	apiOnlyArgKey      = "oms.ApiOnly"      // if true then API only web-service, no UI
	uiLangsArgKey      = "oms.Languages"    // list of supported languages
	encodingArgKey     = "oms.CodePage"     // code page for converting source files, e.g. windows-1252
	pageSizeAgrKey     = "oms.MaxRowCount"  // max number of rows to return from read parameters or output tables
	doubleFormatArgKey = "oms.DoubleFormat" // format to convert float or double value to string, e.g. %.15g
)

// front-end UI subdirectory with html and javascript
const htmlSubDir = "html"

// matcher to find UI supported language corresponding to request
var uiLangMatcher language.Matcher

// if true then log http requests
var isLogRequest bool

// default "page" size: row count to read parameters or output tables
var pageMaxSize int64 = 100

// format to convert float or double value to string
var doubleFmt string = "%.15g"

// main entry point: wrapper to handle errors
func main() {
	defer exitOnPanic() // fatal error handler: log and exit

	err := mainBody(os.Args)
	if err != nil {
		omppLog.Log(err.Error())
		os.Exit(1)
	}
	omppLog.Log("Done.") // compeleted OK
}

// actual main body
func mainBody(args []string) error {

	// set command line argument keys and ini-file keys
	_ = flag.String(rootDirArgKey, "", "root directory, default: current directory")
	_ = flag.String(modelDirArgKey, "models/bin", "models directory, if relative then must be relative to root directory")
	_ = flag.String(modelLogDirArgKey, "../log", "models log directory, if relative then must be relative to models directory")
	_ = flag.String(listenArgKey, "localhost:4040", "address to listen")
	_ = flag.String(listenShortKey, "localhost:4040", "address to listen (short form of "+listenArgKey+")")
	_ = flag.Bool(logRequestArgKey, false, "if true then log HTTP requests")
	_ = flag.Bool(apiOnlyArgKey, false, "if true then API only web-service, no UI")
	_ = flag.String(uiLangsArgKey, "en", "comma-separated list of supported languages")
	_ = flag.String(encodingArgKey, "", "code page to convert source file into utf-8, e.g.: windows-1252")
	_ = flag.Int64(pageSizeAgrKey, pageMaxSize, "max number of rows to return from read parameters or output tables")
	_ = flag.String(doubleFormatArgKey, doubleFmt, "format to convert float or double value to string")

	// pairs of full and short argument names to map short name to full name
	var optFs = []config.FullShort{
		config.FullShort{Full: listenArgKey, Short: listenShortKey},
	}

	// parse command line arguments and ini-file
	runOpts, logOpts, extraArgs, err := config.New(encodingArgKey, optFs)
	if err != nil {
		return errors.New("Invalid arguments: " + err.Error())
	}
	if len(extraArgs) > 0 {
		return errors.New("Invalid arguments: " + strings.Join(extraArgs, " "))
	}
	isLogRequest = runOpts.Bool(logRequestArgKey)
	isApiOnly := runOpts.Bool(apiOnlyArgKey)
	pageMaxSize = runOpts.Int64(pageSizeAgrKey, pageMaxSize)
	doubleFmt = runOpts.String(doubleFormatArgKey)

	rootDir := runOpts.String(rootDirArgKey) // server root directory

	// if UI required then server root directory must have html subdir
	if !isApiOnly {
		htmlDir := filepath.Join(rootDir, htmlSubDir)
		if err := isDirExist(htmlDir); err != nil {
			return err
		}
	}

	// change to root directory
	if rootDir != "" && rootDir != "." {
		if err := os.Chdir(rootDir); err != nil {
			return errors.New("Error: unable to change directory: " + err.Error())
		}
	}
	omppLog.New(logOpts) // adjust log options, log path can be relative to root directory

	if rootDir != "" && rootDir != "." {
		omppLog.Log("Changing directory to: ", rootDir)
	}

	// model directory required to build initial list of model sqlite files
	modelDir := runOpts.String(modelDirArgKey)
	if modelDir == "" {
		return errors.New("Error: model directory argument cannot be empty")
	}
	omppLog.Log("Model directory: ", modelDir)

	if err := theCatalog.RefreshSqlite(modelDir); err != nil {
		return err
	}

	// refresh run state catalog
	modelLogDir := runOpts.String(modelLogDirArgKey)
	digestLst := theCatalog.AllModelDigests()

	if err := theRunStateCatalog.RefreshCatalog(digestLst, modelDir, modelLogDir); err != nil {
		return err
	}

	// set UI languages to find model text in browser language
	ll := strings.Split(runOpts.String(uiLangsArgKey), ",")
	var lt []language.Tag
	for _, ls := range ll {
		if ls != "" {
			lt = append(lt, language.Make(ls))
		}
	}
	if len(lt) <= 0 {
		lt = append(lt, language.English)
	}
	uiLangMatcher = language.NewMatcher(lt)

	// setup router and start server
	router := vestigo.NewRouter()

	router.SetGlobalCors(&vestigo.CorsAccessControl{
		AllowOrigin:      []string{"*"},
		AllowCredentials: true,
		AllowHeaders:     []string{"Content-Type"},
		ExposeHeaders:    []string{"Content-Type", "Content-Location"},
	})

	apiGetRoutes(router)      // web-service /api routes to get metadata
	apiReadRoutes(router)     // web-service /api routes to read values
	apiReadCsvRoutes(router)  // web-service /api routes to read values into csv stream
	apiUpdateRoutes(router)   // web-service /api routes to update metadata
	apiRunModelRoutes(router) // web-service /api routes to run the model

	// set web root handler: UI web pages or "not found" if this is web-service mode
	if !isApiOnly {
		router.Get("/*", homeHandler, logRequest) // serve UI web pages
	} else {
		router.Get("/*", http.NotFound) // only /api, any other pages not found
	}

	addr := runOpts.String(listenArgKey)
	omppLog.Log("Listen at " + addr)
	if !isApiOnly {
		if !strings.HasPrefix(addr, ":") {
			omppLog.Log("To start open in your browser: " + addr)
		} else {
			omppLog.Log("To start open in your browser: ", "localhost", addr)
		}
	}
	omppLog.Log("To finish press Ctrl+C")

	err = http.ListenAndServe(addr, router)
	return err
}

// exitOnPanic log error message and exit with return = 2
func exitOnPanic() {
	r := recover()
	if r == nil {
		return // not in panic
	}
	switch e := r.(type) {
	case error:
		omppLog.Log(e.Error())
	case string:
		omppLog.Log(e)
	default:
		omppLog.Log("FAILED")
	}
	os.Exit(2) // final exit
}

// add http GET web-service /api routes to get metadata
func apiGetRoutes(router *vestigo.Router) {

	//
	// GET model definition
	//

	// GET /api/model-list
	router.Get("/api/model-list", modelListHandler, logRequest)

	// GET /api/model-list/text
	// GET /api/model-list/text/lang/:lang
	// GET /api/model-list-text?lang=en
	router.Get("/api/model-list/text", modelTextListHandler, logRequest)
	router.Get("/api/model-list/text/lang/:lang", modelTextListHandler, logRequest)
	router.Get("/api/model-list-text", modelTextListHandler, logRequest)

	// GET /api/model/:model
	// GET /api/model?model=modelNameOrDigest
	router.Get("/api/model/:model", modelMetaHandler, logRequest)
	router.Get("/api/model", modelMetaHandler, logRequest)

	// GET /api/model/:model/text
	// GET /api/model/:model/text/lang/:lang
	// GET /api/model-text?model=modelNameOrDigest&lang=en
	router.Get("/api/model/:model/text", modelTextHandler, logRequest)
	router.Get("/api/model/:model/text/lang/:lang", modelTextHandler, logRequest)
	router.Get("/api/model-text", modelTextHandler, logRequest)

	// GET /api/model/:model/text/all
	// GET /api/model-text-all?model=modelNameOrDigest
	router.Get("/api/model/:model/text/all", modelAllTextHandler, logRequest)
	router.Get("/api/model-text-all", modelAllTextHandler, logRequest)

	//
	// GET model extra: languages, groups, profile(s)
	//

	// GET /api/model/:model/lang-list
	// GET /api/lang-list?model=modelNameOrDigest
	router.Get("/api/model/:model/lang-list", langListHandler, logRequest)
	router.Get("/api/lang-list", langListHandler, logRequest)

	// GET /api/model/:model/word-list
	// GET /api/model/:model/word-list/lang/:lang
	// GET /api/word-list?model=modelNameOrDigest&lang=en
	router.Get("/api/model/:model/word-list", wordListHandler, logRequest)
	router.Get("/api/model/:model/word-list/lang/:lang", wordListHandler, logRequest)
	router.Get("/api/word-list", wordListHandler, logRequest)

	// GET /api/model/:model/group
	// GET /api/model-group?model=modelNameOrDigest
	router.Get("/api/model/:model/group", modelGroupHandler, logRequest)
	router.Get("/api/model-group", modelGroupHandler, logRequest)

	// GET /api/model/:model/group/text
	// GET /api/model/:model/group/text/lang/:lang
	// GET /api/model-group-text?model=modelNameOrDigest&lang=en
	router.Get("/api/model/:model/group/text", modelGroupTextHandler, logRequest)
	router.Get("/api/model/:model/group/text/lang/:lang", modelGroupTextHandler, logRequest)
	router.Get("/api/model-group-text", modelGroupTextHandler, logRequest)

	// GET /api/model/:model/group/text/all
	// GET /api/model-group-text-all?model=modelNameOrDigest
	router.Get("/api/model/:model/group/text/all", modelGroupAllTextHandler, logRequest)
	router.Get("/api/model-group-text-all", modelGroupAllTextHandler, logRequest)

	// GET /api/model/:model/profile/:profile
	// GET /api/model-profile?model=modelNameOrDigest&profile=profileName
	router.Get("/api/model/:model/profile/:profile", modelProfileHandler, logRequest)
	router.Get("/api/model-profile", modelProfileHandler, logRequest)

	//
	// GET model run results
	//

	// GET /api/model/:model/run-list
	// GET /api/run-list?model=modelNameOrDigest
	router.Get("/api/model/:model/run-list", runListHandler, logRequest)
	router.Get("/api/run-list", runListHandler, logRequest)

	// GET /api/model/:model/run-list/text
	// GET /api/model/:model/run-list/text/lang/:lang
	// GET /api/run-list-text?model=modelNameOrDigest&lang=en
	router.Get("/api/model/:model/run-list/text", runListTextHandler, logRequest)
	router.Get("/api/model/:model/run-list/text/lang/:lang", runListTextHandler, logRequest)
	router.Get("/api/run-list-text", runListTextHandler, logRequest)

	// GET /api/model/:model/run/:run/status
	// GET /api/run-status?model=modelNameOrDigest&run=runNameOrDigest
	router.Get("/api/model/:model/run/:run/status", runStatusHandler, logRequest)
	router.Get("/api/run-status", runStatusHandler, logRequest)

	// GET /api/model/:model/run/status/first
	// GET /api/run-first-status?model=modelNameOrDigest
	router.Get("/api/model/:model/run/status/first", firstRunStatusHandler, logRequest)
	router.Get("/api/run-first-status", firstRunStatusHandler, logRequest)

	// GET /api/model/:model/run/status/last
	// GET /api/run-last-status?model=modelNameOrDigest
	router.Get("/api/model/:model/run/status/last", lastRunStatusHandler, logRequest)
	router.Get("/api/run-last-status", lastRunStatusHandler, logRequest)

	// GET /api/model/:model/run/status/last-completed
	// GET /api/run-last-completed-status?model=modelNameOrDigest
	router.Get("/api/model/:model/run/status/last-completed", lastCompletedRunStatusHandler, logRequest)
	router.Get("/api/run-last-completed-status", lastCompletedRunStatusHandler, logRequest)

	// GET /api/model/:model/run/:run/text
	// GET /api/model/:model/run/:run/text/lang/:lang
	// GET /api/run-text?model=modelNameOrDigest&run=runNameOrDigest&lang=en
	router.Get("/api/model/:model/run/:run/text", runTextHandler, logRequest)
	router.Get("/api/model/:model/run/:run/text/lang/:lang", runTextHandler, logRequest)
	router.Get("/api/run-text", runTextHandler, logRequest)
	router.Get("/api/model/:model/run/:run/", http.NotFound)
	router.Get("/api/model/:model/run/:run/text/", http.NotFound)
	router.Get("/api/model/:model/run/:run/text/lang/", http.NotFound)

	// GET /api/model/:model/run/:run/text/all
	// GET /api/run-text-all?model=modelNameOrDigest&run=runNameOrDigest
	router.Get("/api/model/:model/run/:run/text/all", runAllTextHandler, logRequest)
	router.Get("/api/run-text-all", runAllTextHandler, logRequest)

	// GET /api/model/:model/run/last-completed/text
	// GET /api/model/:model/run/last-completed/text/lang/:lang
	// GET /api/run-last-completed-text?model=modelNameOrDigest&lang=en
	router.Get("/api/model/:model/run/last-completed/text", lastCompletedRunTextHandler, logRequest)
	router.Get("/api/model/:model/run/last-completed/text/lang/:lang", lastCompletedRunTextHandler, logRequest)
	router.Get("/api/run-last-completed-text", lastCompletedRunTextHandler, logRequest)

	// GET /api/model/:model/run/last-completed/text/all
	// GET /api/run-last-completed-text-all?model=modelNameOrDigest
	router.Get("/api/model/:model/run/last-completed/text/all", lastCompletedRunAllTextHandler, logRequest)
	router.Get("/api/run-last-completed-text-all", lastCompletedRunAllTextHandler, logRequest)

	//
	// GET model set of input parameters (workset)
	//

	// GET /api/model/:model/workset-list
	// GET /api/workset-list?model=modelNameOrDigest
	router.Get("/api/model/:model/workset-list", worksetListHandler, logRequest)
	router.Get("/api/workset-list", worksetListHandler, logRequest)

	// GET /api/model/:model/workset-list/text
	// GET /api/model/:model/workset-list/text/lang/:lang
	// GET /api/workset-list-text?model=modelNameOrDigest&lang=en
	router.Get("/api/model/:model/workset-list/text", worksetListTextHandler, logRequest)
	router.Get("/api/model/:model/workset-list/text/lang/:lang", worksetListTextHandler, logRequest)
	router.Get("/api/workset-list-text", worksetListTextHandler, logRequest)

	// GET /api/model/:model/workset/:set
	// GET /api/workset-status?model=modelNameOrDigest&set=setName
	router.Get("/api/model/:model/workset/:set/status", worksetStatusHandler, logRequest)
	router.Get("/api/workset-status", worksetStatusHandler, logRequest)

	// GET /api/model/:model/workset/status/default
	// GET /api/workset-default-status?model=modelNameOrDigest
	router.Get("/api/model/:model/workset/status/default", worksetDefaultStatusHandler, logRequest)
	router.Get("/api/workset-default-status", worksetDefaultStatusHandler, logRequest)

	// GET /api/model/:model/workset/:set/text
	// GET /api/model/:model/workset/:set/text/lang/:lang
	// GET /api/workset-text?model=modelNameOrDigest&set=setName&lang=en
	router.Get("/api/model/:model/workset/:set/text", worksetTextHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/text/lang/:lang", worksetTextHandler, logRequest)
	router.Get("/api/workset-text", worksetTextHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/text/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/text/lang/", http.NotFound)

	// GET /api/model/:model/workset/:set/text/all
	// GET /api/workset-text-all?model=modelNameOrDigest&set=setName
	router.Get("/api/model/:model/workset/:set/text/all", worksetAllTextHandler, logRequest)
	router.Get("/api/workset-text-all", worksetAllTextHandler, logRequest)

	//
	// GET modeling tasks and task run history
	//

	// GET /api/model/:model/task-list
	// GET /api/task-list?model=modelNameOrDigest
	router.Get("/api/model/:model/task-list", taskListHandler, logRequest)
	router.Get("/api/task-list", taskListHandler, logRequest)

	// GET /api/model/:model/task-list/text
	// GET /api/model/:model/task-list/text/lang/:lang
	// GET /api/task-list-text?model=modelNameOrDigest&lang=en
	router.Get("/api/model/:model/task-list/text", taskListTextHandler, logRequest)
	router.Get("/api/model/:model/task-list/text/lang/:lang", taskListTextHandler, logRequest)
	router.Get("/api/task-list-text", taskListTextHandler, logRequest)

	// GET /api/model/:model/task/:task/sets
	// GET /api/task-sets?model=modelNameOrDigest&task=taskName
	router.Get("/api/model/:model/task/:task/sets", taskSetsHandler, logRequest)
	router.Get("/api/task-sets", taskSetsHandler, logRequest)

	// GET /api/model/:model/task/:task/runs
	// GET /api/task-runs?model=modelNameOrDigest&task=taskName
	router.Get("/api/model/:model/task/:task/runs", taskRunsHandler, logRequest)
	router.Get("/api/task-runs", taskRunsHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/run/:run-name
	// GET /api/task-run-status?model=modelNameOrDigest&task=taskName&run-name=runName
	router.Get("/api/model/:model/task/:task/run-status/run/:run-name", taskRunStatusHandler, logRequest)
	router.Get("/api/task-run-status", taskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/first
	// GET /api/task-first-run-status?model=modelNameOrDigest&task=taskName
	router.Get("/api/model/:model/task/:task/run-status/first", firstTaskRunStatusHandler, logRequest)
	router.Get("/api/task-first-run-status", firstTaskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/last
	// GET /api/task-last-run-status?model=modelNameOrDigest&task=taskName
	router.Get("/api/model/:model/task/:task/run-status/last", lastTaskRunStatusHandler, logRequest)
	router.Get("/api/task-last-run-status", lastTaskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/last-completed
	// GET /api/task-last-completed-run-status?model=modelNameOrDigest&task=taskName
	router.Get("/api/model/:model/task/:task/run-status/last-completed", lastCompletedTaskRunStatusHandler, logRequest)
	router.Get("/api/task-last-completed-run-status", lastCompletedTaskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/text
	// GET /api/model/:model/task/:task/text/lang/:lang
	// GET /api/task-text?model=modelNameOrDigest&task=taskName&lang=en
	router.Get("/api/model/:model/task/:task/text", taskTextHandler, logRequest)
	router.Get("/api/model/:model/task/:task/text/lang/:lang", taskTextHandler, logRequest)
	router.Get("/api/task-text", taskTextHandler, logRequest)
	router.Get("/api/model/:model/task/:task/", http.NotFound)
	router.Get("/api/model/:model/task/:task/text/", http.NotFound)
	router.Get("/api/model/:model/task/:task/text/lang/", http.NotFound)

	// GET /api/model/:model/task/:task/text/all
	// GET /api/task-text-all?model=modelNameOrDigest&task=taskName
	router.Get("/api/model/:model/task/:task/text/all", taskAllTextHandler, logRequest)
	router.Get("/api/task-text-all", taskAllTextHandler, logRequest)
}

// add http GET or POST web-service /api routes to read parameters or output tables
func apiReadRoutes(router *vestigo.Router) {

	// POST /api/model/:model/workset/:set/parameter/value
	router.Post("/api/model/:model/workset/:set/parameter/value", worksetParameterPageReadHandler, logRequest)

	// POST /api/model/:model/workset/:set/parameter/value-id
	router.Post("/api/model/:model/workset/:set/parameter/value-id", worksetParameterIdPageReadHandler, logRequest)

	// POST /api/model/:model/run/:run/parameter/value
	router.Post("/api/model/:model/run/:run/parameter/value", runParameterPageReadHandler, logRequest)

	// POST /api/model/:model/run/:run/parameter/value-id
	router.Post("/api/model/:model/run/:run/parameter/value-id", runParameterIdPageReadHandler, logRequest)

	// POST /api/model/:model/run/:run/table/value
	router.Post("/api/model/:model/run/:run/table/value", runTablePageReadHandler, logRequest)

	// POST /api/model/:model/run/:run/table/value-id
	router.Post("/api/model/:model/run/:run/table/value-id", runTableIdPageReadHandler, logRequest)

	// GET /api/model/:model/workset/:set/parameter/:name/value
	// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start
	// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start/count/:count
	// GET /api/workset-parameter-value?model=modelNameOrDigest&set=setName&name=parameterName&start=0&count=100
	router.Get("/api/model/:model/workset/:set/parameter/:name/value", worksetParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start", worksetParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start/count/:count", worksetParameterPageGetHandler, logRequest)
	router.Get("/api/workset-parameter-value", worksetParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/parameter/:name/value
	// GET /api/model/:model/run/:run/parameter/:name/value/start/:start
	// GET /api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count
	// GET /api/run-parameter-value?model=modelNameOrDigest&run=runNameOrDigest&name=parameterName&start=0&count=100
	router.Get("/api/model/:model/run/:run/parameter/:name/value", runParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start", runParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count", runParameterPageGetHandler, logRequest)
	router.Get("/api/run-parameter-value", runParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/expr
	// GET /api/model/:model/run/:run/table/:name/expr/start/:start
	// GET /api/model/:model/run/:run/table/:name/expr/start/:start/count/:count
	// GET /api/run-table-expr?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&start=0&count=100
	router.Get("/api/model/:model/run/:run/table/:name/expr", runTableExprPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start", runTableExprPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start/count/:count", runTableExprPageGetHandler, logRequest)
	router.Get("/api/run-table-expr", runTableExprPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/acc/start/:start
	// GET /api/model/:model/run/:run/table/:name/acc/start/:start/count/:count
	// GET /api/run-table-acc?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&start=0&count=100
	router.Get("/api/model/:model/run/:run/table/:name/acc", runTableAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start", runTableAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start/count/:count", runTableAccPageGetHandler, logRequest)
	router.Get("/api/run-table-acc", runTableAccPageGetHandler, logRequest)
	// router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/all-acc
	// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start
	// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count
	// GET /api/run-table-all-acc?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&start=0&count=100
	router.Get("/api/model/:model/run/:run/table/:name/all-acc", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/run-table-all-acc", runTableAllAccPageGetHandler, logRequest)
	// router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start/count/", http.NotFound)
}

// add http GET or POST web-service /api routes to read parameters or output tables as csv stream
func apiReadCsvRoutes(router *vestigo.Router) {

	// GET /api/model/:model/workset/:set/parameter/:name/csv
	// GET /api/workset-parameter-csv?model=modelNameOrDigest&set=setName&name=parameterName&bom=true
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv", worksetParameterCsvGetHandler, logRequest)
	router.Get("/api/workset-parameter-csv", worksetParameterCsvGetHandler, logRequest)

	// GET /api/model/:model/workset/:set/parameter/:name/csv-bom
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv-bom", worksetParameterCsvBomGetHandler, logRequest)

	// GET /api/model/:model/workset/:set/parameter/:name/csv-id
	// GET /api/workset-parameter-csv-id?model=modelNameOrDigest&set=setName&name=parameterName&bom=true
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv-id", worksetParameterIdCsvGetHandler, logRequest)
	router.Get("/api/workset-parameter-csv-id", worksetParameterIdCsvGetHandler, logRequest)

	// GET /api/model/:model/workset/:set/parameter/:name/csv-id-bom
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv-id-bom", worksetParameterIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv
	// GET /api/run-parameter-csv?model=modelNameOrDigest&run=runNameOrDigest&name=parameterName&bom=true
	router.Get("/api/model/:model/run/:run/parameter/:name/csv", runParameterCsvGetHandler, logRequest)
	router.Get("/api/run-parameter-csv", runParameterCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv-bom
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-bom", runParameterCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv-id
	// GET /api/run-parameter-csv-id?model=modelNameOrDigest&run=runNameOrDigest&name=parameterName&bom=true
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-id", runParameterIdCsvGetHandler, logRequest)
	router.Get("/api/run-parameter-csv-id", runParameterIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv-id-bom
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-id-bom", runParameterIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv
	// GET /api/run-table-expr-csv?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv", runTableExprCsvGetHandler, logRequest)
	router.Get("/api/run-table-expr-csv", runTableExprCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv-bom
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-bom", runTableExprCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv-id
	// GET /api/run-table-expr-csv-id?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-id", runTableExprIdCsvGetHandler, logRequest)
	router.Get("/api/run-table-expr-csv-id", runTableExprIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-id-bom", runTableExprIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv
	// GET /api/run-table-acc-csv?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv", runTableAccCsvGetHandler, logRequest)
	router.Get("/api/run-table-acc-csv", runTableAccCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv-bom
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-bom", runTableAccCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv-id
	// GET /api/run-table-acc-csv-id?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-id", runTableAccIdCsvGetHandler, logRequest)
	router.Get("/api/run-table-acc-csv-id", runTableAccIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-id-bom", runTableAccIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv
	// GET /api/run-table-all-acc-csv?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv", runTableAllAccCsvGetHandler, logRequest)
	router.Get("/api/run-table-all-acc-csv", runTableAllAccCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-bom
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-bom", runTableAllAccCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id
	// GET /api/run-table-all-acc-csv-id?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-id", runTableAllAccIdCsvGetHandler, logRequest)
	router.Get("/api/run-table-all-acc-csv-id", runTableAllAccIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-id-bom", runTableAllAccIdCsvBomGetHandler, logRequest)
}

// add web-service /api routes to update metadata
func apiUpdateRoutes(router *vestigo.Router) {

	//
	// update profile
	//

	// PATCH /api/model/:model/profile
	// POST /api/model-profile?model=modelNameOrDigest
	router.Patch("/api/model/:model/profile", profileReplaceHandler, logRequest)
	router.Post("/api/model-profile", profileReplaceHandler, logRequest)

	// DELETE /api/model/:model/profile/:profile
	// POST /api/model-profile-delete?model=modelNameOrDigest&profile=profileName
	router.Delete("/api/model/:model/profile/:profile", profileDeleteHandler, logRequest)
	router.Post("/api/model-profile-delete", profileDeleteHandler, logRequest)

	// POST /api/model/:model/profile/:profile/key/:key/value/:value
	router.Post("/api/model/:model/profile/:profile/key/:key/value/:value", profileOptionReplaceHandler, logRequest)

	// DELETE /api/model/:model/profile/:profile/key/:key
	// POST /api/model-profile-key-delete?model=modelNameOrDigest&profile=profileName&key=someKey
	router.Delete("/api/model/:model/profile/:profile/key/:key", profileOptionDeleteHandler, logRequest)
	router.Post("/api/model-profile-key-delete", profileOptionDeleteHandler, logRequest)

	//
	// update model set of input parameters (workset)
	//

	// POST /api/model/:model/workset/:set/readonly/:readonly
	// POST /api/workset-readonly?model=modelNameOrDigest&set=setName&readonly=true
	router.Post("/api/model/:model/workset/:set/readonly/:readonly", worksetReadonlyUpdateHandler, logRequest)
	router.Post("/api/workset-readonly", worksetReadonlyUpdateHandler, logRequest)

	// PUT  /api/workset-new
	// POST /api/workset-new
	router.Put("/api/workset-new", worksetReplaceHandler, logRequest)
	router.Post("/api/workset-new", worksetReplaceHandler, logRequest)

	// PATCH /api/workset
	// POST  /api/workset
	router.Patch("/api/workset", worksetMergeHandler, logRequest)
	router.Post("/api/workset", worksetMergeHandler, logRequest)

	// DELETE /api/model/:model/workset/:set
	// POST   /api/workset-delete?model=modelNameOrDigest&set=setName
	router.Delete("/api/model/:model/workset/:set", worksetDeleteHandler, logRequest)
	router.Post("/api/workset-delete", worksetDeleteHandler, logRequest)

	// PATCH /api/model/:model/workset/:set/parameter/:name/new/value
	// POST  /api/workset-parameter-new-value?model=modelNameOrDigest&set=setName&name=parameterName
	router.Patch("/api/model/:model/workset/:set/parameter/:name/new/value", parameterPageUpdateHandler, logRequest)
	router.Post("/api/workset-parameter-new-value", parameterPageUpdateHandler, logRequest)

	// PATCH /api/model/:model/workset/:set/parameter/:name/new/value-id
	// POST  /api/workset-parameter-new-value-id?model=modelNameOrDigest&set=setName&name=parameterName
	router.Patch("/api/model/:model/workset/:set/parameter/:name/new/value-id", parameterIdPageUpdateHandler, logRequest)
	router.Post("/api/workset-parameter-new-value-id", parameterIdPageUpdateHandler, logRequest)

	// DELETE /api/model/:model/workset/:set/parameter/:name
	// POST   /api/workset-parameter-delete?model=modelNameOrDigest&set=setName&parameter=name
	router.Delete("/api/model/:model/workset/:set/parameter/:name", worksetParameterDeleteHandler, logRequest)
	router.Post("/api/workset-parameter-delete", worksetParameterDeleteHandler, logRequest)

	// PUT  /api/model/:model/workset/:set/copy/parameter/:name/from-run/:run
	// POST /api/copy-parameter-from-run?model=modelNameOrDigest&set=setName&name=parameterName&run=runNameOrDigest"
	router.Put("/api/model/:model/workset/:set/copy/parameter/:name/from-run/:run", worksetParameterRunCopyHandler, logRequest)
	router.Post("/api/copy-parameter-from-run", worksetParameterRunCopyHandler, logRequest)

	// PUT /api/model/:model/workset/:set/copy/parameter/:name/from-workset/:from-set
	// POST /api/copy-parameter-from-workset?model=modelNameOrDigest&set=dstSetName&name=parameterName&from-set=srcSetName"
	router.Put("/api/model/:model/workset/:set/copy/parameter/:name/from-workset/:from-set", worksetParameterCopyFromWsHandler, logRequest)
	router.Post("/api/copy-parameter-from-workset", worksetParameterCopyFromWsHandler, logRequest)

	//
	// update model run
	//

	// PATCH /api/run/text
	// POST  /api/run/text
	router.Patch("/api/run/text", runTextMergeHandler, logRequest)
	router.Post("/api/run/text", runTextMergeHandler, logRequest)

	// DELETE /api/model/:model/run/:run
	// POST   /api/run/delete?model=modelNameOrDigest&run=runNameOrDigest
	router.Delete("/api/model/:model/run/:run", runDeleteHandler, logRequest)
	router.Post("/api/run-delete", runDeleteHandler, logRequest)

	//
	// update modeling task and task run history
	//

	// PUT  /api/task-new
	// POST /api/task-new
	router.Put("/api/task-new", taskDefReplaceHandler, logRequest)
	router.Post("/api/task-new", taskDefReplaceHandler, logRequest)

	// PATCH /api/task
	// POST  /api/task
	router.Patch("/api/task", taskDefMergeHandler, logRequest)
	router.Post("/api/task", taskDefMergeHandler, logRequest)

	// DELETE /api/model/:model/task/:task
	// POST   /api/task-delete?model=modelNameOrDigest&task=taskName
	router.Delete("/api/model/:model/task/:task", taskDeleteHandler, logRequest)
	router.Post("/api/task-delete", taskDeleteHandler, logRequest)
}

// add web-service /api routes to run the model and monitor progress
func apiRunModelRoutes(router *vestigo.Router) {

	// POST /api/run
	router.Post("/api/run", runModelHandler, logRequest)

	// GET /api/run/log/model/:model/stamp/:stamp
	// GET /api/run/log/model/:model/stamp/:stamp/start/:start/count/:count
	// GET /api/run-log?model=modelNameOrDigest&stamp=runStamp&start=0&count=0
	router.Get("/api/run/log/model/:model/stamp/:stamp", runModelLogPageHandler, logRequest)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start", runModelLogPageHandler, logRequest)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start/count/:count", runModelLogPageHandler, logRequest)
	router.Get("/api/run-log", runModelLogPageHandler, logRequest)
	router.Get("/api/run/log/model/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start/count/", http.NotFound)
}
