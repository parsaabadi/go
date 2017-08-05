// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/husobee/vestigo"
	"golang.org/x/text/language"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/omppLog"
)

// config keys to get values from ini-file or command line arguments.
const (
	rootDirArgKey    = "oms.RootDir"     // root directory, expected subdir: html
	modelDirArgKey   = "oms.ModelDir"    // models directory, if relative then must be relative to root directory
	listenArgKey     = "oms.Listen"      // address to listen, default: localhost:4040
	listenShortKey   = "l"               // address to listen (short form)
	logRequestArgKey = "oms.LogRequest"  // if true then log http request
	apiOnlyArgKey    = "oms.ApiOnly"     // if true then API only web-service, no UI
	uiLangsArgKey    = "oms.Languages"   // list of supported languages
	encodingArgKey   = "oms.CodePage"    // code page for converting source files, e.g. windows-1252
	pageSizeAgrKey   = "oms.MaxRowCount" // max number of rows to return from read parameters or output tables
)

// front-end UI subdirectory with html and javascript
const htmlSubDir = "html"

// matcher to find UI supported language corresponding to request
var uiLangMatcher language.Matcher

// if true then log http requests
var isLogRequest bool

// default "page" size: row count to read parameters or output tables
var pageMaxSize int64 = 100

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
	_ = flag.String(listenArgKey, "localhost:4040", "address to listen")
	_ = flag.String(listenShortKey, "localhost:4040", "address to listen (short form of "+listenArgKey+")")
	_ = flag.Bool(logRequestArgKey, false, "if true then log HTTP requests")
	_ = flag.Bool(apiOnlyArgKey, false, "if true then API only web-service, no UI")
	_ = flag.String(uiLangsArgKey, "en", "comma-separated list of supported languages")
	_ = flag.String(encodingArgKey, "", "code page to convert source file into utf-8, e.g.: windows-1252")
	_ = flag.Int64(pageSizeAgrKey, pageMaxSize, "max number of rows to return from read parameters or output tables")

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

	apiGetRoutes(router)    // web-service /api routes to get metadata
	apiReadRoutes(router)   // web-service /api routes to read values
	apiUpdateRoutes(router) // web-service /api routes to update metadata

	// set web root handler: UI web pages or "not found" if this is web-service mode
	if !isApiOnly {
		router.Get("/*", homeHandler, logRequest) // serve UI web pages
	} else {
		router.Get("/*", http.NotFound) // only /api, any other pages not found
	}

	addr := runOpts.String(listenArgKey)
	omppLog.Log("Listen at " + addr)
	if !isApiOnly {
		omppLog.Log("To start open in your browser: " + addr)
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
	// GET /api/model-list/
	router.Get("/api/model-list", modelListHandler, logRequest)
	router.Get("/api/model-list/", modelListHandler, logRequest)

	// GET /api/model-list-text?lang=en
	// GET /api/model-list-text/
	// GET /api/model-list/text
	// GET /api/model-list/text/lang/:lang
	router.Get("/api/model-list-text", modelTextListHandler, logRequest)
	router.Get("/api/model-list-text/", modelTextListHandler, logRequest)
	router.Get("/api/model-list/text", modelTextListHandler, logRequest)
	router.Get("/api/model-list/text/lang/:lang", modelTextListHandler, logRequest)

	// GET /api/model?dn=a1b2c3d
	// GET /api/model?dn=modelName
	// GET /api/model/:dn
	router.Get("/api/model", modelMetaHandler, logRequest)
	router.Get("/api/model/:dn", modelMetaHandler, logRequest)

	// GET /api/model-text?dn=a1b2c3d
	// GET /api/model-text?dn=modelName&lang=en
	// GET /api/model/:dn/text
	// GET /api/model/:dn/text/lang/:lang
	router.Get("/api/model-text", modelTextHandler, logRequest)
	router.Get("/api/model/:dn/text", modelTextHandler, logRequest)
	router.Get("/api/model/:dn/text/lang/:lang", modelTextHandler, logRequest)

	// GET /api/model-text-all?dn=a1b2c3d
	// GET /api/model-text-all?dn=modelName
	// GET /api/model/:dn/text/all
	router.Get("/api/model-text-all", modelAllTextHandler, logRequest)
	router.Get("/api/model/:dn/text/all", modelAllTextHandler, logRequest)

	//
	// GET model extra: languages, groups, profile(s)
	//

	// GET /api/lang-list?dn=a1b2c3d
	// GET /api/lang-list?dn=modelName
	// GET /api/model/:dn/lang-list
	router.Get("/api/lang-list", langListHandler, logRequest)
	router.Get("/api/model/:dn/lang-list", langListHandler, logRequest)

	// GET /api/model-group?dn=a1b2c3d
	// GET /api/model-group?dn=modelName
	// GET /api/model/:dn/group
	router.Get("/api/model-group", modelGroupHandler, logRequest)
	router.Get("/api/model/:dn/group", modelGroupHandler, logRequest)

	// GET /api/model-group-text?dn=a1b2c3d
	// GET /api/model-group-text?dn=modelName&lang=en
	// GET /api/model/:dn/group/text
	// GET /api/model/:dn/group/text/lang/:lang
	router.Get("/api/model-group-text", modelGroupTextHandler, logRequest)
	router.Get("/api/model/:dn/group/text", modelGroupTextHandler, logRequest)
	router.Get("/api/model/:dn/group/text/lang/:lang", modelGroupTextHandler, logRequest)

	// GET /api/model-group-text-all?dn=a1b2c3d
	// GET /api/model-group-text-all?dn=modelName
	// GET /api/model/:dn/group/text/all
	router.Get("/api/model-group-text-all", modelGroupAllTextHandler, logRequest)
	router.Get("/api/model/:dn/group/text/all", modelGroupAllTextHandler, logRequest)

	// GET /api/model-profile?digest=a1b2c3d&name=profileName
	// GET /api/model/:digest/profile/:name
	router.Get("/api/model-profile", modelProfileHandler, logRequest)
	router.Get("/api/model/:digest/profile/:name", modelProfileHandler, logRequest)

	//
	// GET model run results
	//

	// GET /api/run-list?dn=a1b2c3d
	// GET /api/run-list?dn=modelName
	// GET /api/model/:dn/run-list
	router.Get("/api/run-list", runListHandler, logRequest)
	router.Get("/api/model/:dn/run-list", runListHandler, logRequest)

	// GET /api/run-list-text?dn=a1b2c3d
	// GET /api/run-list-text?dn=modelName&lang=en
	// GET /api/model/:dn/run-list/text
	// GET /api/model/:dn/run-list/text/lang/:lang
	router.Get("/api/run-list-text", runListTextHandler, logRequest)
	router.Get("/api/model/:dn/run-list/text", runListTextHandler, logRequest)
	router.Get("/api/model/:dn/run-list/text/lang/:lang", runListTextHandler, logRequest)

	// GET /api/run-status?dn=a1b2c3d&rdn=1f2e3d4
	// GET /api/run-status?dn=modelName&rdn=runName
	// GET /api/model/:dn/run/:rdn/status
	router.Get("/api/run-status", runStatusHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/status", runStatusHandler, logRequest)

	// GET /api/run-first-status?dn=a1b2c3d
	// GET /api/run-first-status?dn=modelName
	// GET /api/model/:dn/run/status/first
	router.Get("/api/run-first-status", firstRunStatusHandler, logRequest)
	router.Get("/api/model/:dn/run/status/first", firstRunStatusHandler, logRequest)

	// GET /api/run-last-status?dn=a1b2c3d
	// GET /api/run-last-status?dn=modelName
	// GET /api/model/:dn/run/status/last
	router.Get("/api/run-last-status", lastRunStatusHandler, logRequest)
	router.Get("/api/model/:dn/run/status/last", lastRunStatusHandler, logRequest)

	// GET /api/run-last-completed-status?dn=a1b2c3d
	// GET /api/run-last-completed-status?dn=modelName
	// GET /api/model/:dn/run/status/last/completed
	router.Get("/api/run-last-completed-status", lastCompletedRunStatusHandler, logRequest)
	router.Get("/api/model/:dn/run/status/last/completed", lastCompletedRunStatusHandler, logRequest)

	// GET /api/run-last-completed-text?dn=a1b2c3d
	// GET /api/run-last-completed-text?dn=modelName&lang=en
	// GET /api/model/:dn/run/last/completed/text
	// GET /api/model/:dn/run/last/completed/text/lang/:lang
	router.Get("/api/run-last-completed-text", lastCompletedRunTextHandler, logRequest)
	router.Get("/api/model/:dn/run/last/completed/text", lastCompletedRunTextHandler, logRequest)
	router.Get("/api/model/:dn/run/last/completed/text/lang/:lang", lastCompletedRunTextHandler, logRequest)

	// GET /api/run-last-completed-text-all?dn=a1b2c3d
	// GET /api/run-last-completed-text-all?dn=modelName
	// GET /api/model/:dn/run/last/completed/text/all
	router.Get("/api/run-last-completed-text-all", lastCompletedRunAllTextHandler, logRequest)
	router.Get("/api/model/:dn/run/last/completed/text/all", lastCompletedRunAllTextHandler, logRequest)

	// GET /api/run-text?dn=a1b2c3d&rdn=1f2e3d4
	// GET /api/run-text?dn=modelName&rdn=runName&lang=en
	// GET /api/model/:dn/run/:rdn/text
	// GET /api/model/:dn/run/:rdn/text/
	// GET /api/model/:dn/run/:rdn/text/lang/
	// GET /api/model/:dn/run/:rdn/text/lang/:lang
	router.Get("/api/run-text", runTextHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/text", runTextHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/text/", runTextHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/text/lang/", runTextHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/text/lang/:lang", runTextHandler, logRequest)

	// GET /api/run-text-all?dn=a1b2c3d&rdn=1f2e3d4
	// GET /api/run-text-all?dn=modelName&rdn=runName
	// GET /api/model/:dn/run/:rdn/text/all
	router.Get("/api/run-text-all", runAllTextHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/text/all", runAllTextHandler, logRequest)

	//
	// GET model set of input parameters (workset)
	//

	// GET /api/workset-list?dn=a1b2c3d
	// GET /api/workset-list?dn=modelName
	// GET /api/model/:dn/workset-list
	router.Get("/api/workset-list", worksetListHandler, logRequest)
	router.Get("/api/model/:dn/workset-list", worksetListHandler, logRequest)

	// GET /api/workset-list-text?dn=a1b2c3d
	// GET /api/workset-list-text?dn=modelName&lang=en
	// GET /api/model/:dn/workset-list/text
	// GET /api/model/:dn/workset-list/text/lang/:lang
	router.Get("/api/workset-list-text", worksetListTextHandler, logRequest)
	router.Get("/api/model/:dn/workset-list/text", worksetListTextHandler, logRequest)
	router.Get("/api/model/:dn/workset-list/text/lang/:lang", worksetListTextHandler, logRequest)

	// GET /api/workset-status?dn=a1b2c3d&wsn=mySet
	// GET /api/workset-status?dn=modelName&wsn=mySet
	// GET /api/model/:dn/workset/:wsn/status
	router.Get("/api/workset-status", worksetStatusHandler, logRequest)
	router.Get("/api/model/:dn/workset/:wsn/status", worksetStatusHandler, logRequest)

	// GET /api/workset-default-status?dn=a1b2c3d
	// GET /api/workset-default-status?dn=modelName
	// GET /api/model/:dn/workset/status/default
	router.Get("/api/workset-default-status", worksetDefaultStatusHandler, logRequest)
	router.Get("/api/model/:dn/workset/status/default", worksetDefaultStatusHandler, logRequest)

	// GET /api/workset-text?dn=a1b2c3d&wsn=mySet
	// GET /api/workset-text?dn=modelName&wsn=mySet&lang=en
	// GET /api/model/:dn/workset/:wsn/text
	// GET /api/model/:dn/workset/:wsn/text/
	// GET /api/model/:dn/workset/:wsn/text/lang/
	// GET /api/model/:dn/workset/:wsn/text/lang/:lang
	router.Get("/api/workset-text", worksetTextHandler, logRequest)
	router.Get("/api/model/:dn/workset/:wsn/text", worksetTextHandler, logRequest)
	router.Get("/api/model/:dn/workset/:wsn/text/", worksetTextHandler, logRequest)
	router.Get("/api/model/:dn/workset/:wsn/text/lang/", worksetTextHandler, logRequest)
	router.Get("/api/model/:dn/workset/:wsn/text/lang/:lang", worksetTextHandler, logRequest)

	// GET /api/workset-text-all?dn=a1b2c3d&wsn=mySet
	// GET /api/workset-text-all?dn=modelName&wsn=mySet
	// GET /api/model/:dn/workset/:wsn/text/all
	router.Get("/api/workset-text-all", worksetAllTextHandler, logRequest)
	router.Get("/api/model/:dn/workset/:wsn/text/all", worksetAllTextHandler, logRequest)
}

// add http GET or POST web-service /api routes to read parameters or output tables
func apiReadRoutes(router *vestigo.Router) {

	// GET /api/workset-parameter-value-id?dn=modelOne&wsn=mySet&name=ageSex
	// GET /api/workset-parameter-value-id?dn=modelOne&wsn=mySet&name=ageSex&start=0
	// GET /api/workset-parameter-value-id?dn=modelOne&wsn=mySet&name=ageSex&start=0&count=100
	// GET /api/model/:dn/workset/:wsn/parameter/:name/value-id
	// GET /api/model/:dn/workset/:wsn/parameter/:name/value-id/start/:start
	// GET /api/model/:dn/workset/:wsn/parameter/:name/value-id/start/:start/count/:count
	router.Get("/api/workset-parameter-value-id", worksetParameterIdReadGetHandler, logRequest)
	router.Get("/api/model/:dn/workset/:wsn/parameter/:name/value-id", worksetParameterIdReadGetHandler, logRequest)
	router.Get("/api/model/:dn/workset/:wsn/parameter/:name/value-id/start/:start", worksetParameterIdReadGetHandler, logRequest)
	router.Get("/api/model/:dn/workset/:wsn/parameter/:name/value-id/start/:start/count/:count", worksetParameterIdReadGetHandler, logRequest)

	// POST /api/model/:dn/workset/:wsn/parameter/value-id
	router.Post("/api/model/:dn/workset/:wsn/parameter/value-id", worksetParameterIdReadHandler, logRequest)

	// POST /api/model/:dn/workset/:wsn/parameter/value
	router.Post("/api/model/:dn/workset/:wsn/parameter/value", worksetParameterCodeReadHandler, logRequest)

	// GET /api/run-parameter-value-id?dn=a1b2c3d&rdn=1f2e3d4&name=ageSex
	// GET /api/run-parameter-value-id?dn=modelOne&rdn=myRun&name=ageSex
	// GET /api/run-parameter-value-id?dn=modelOne&rdn=1f2e3d4&name=ageSex&start=0
	// GET /api/run-parameter-value-id?dn=modelOne&rdn=myRun&name=ageSex&start=0&count=100
	// GET /api/model/:dn/run/:rdn/parameter/:name/value-id
	// GET /api/model/:dn/run/:rdn/parameter/:name/value-id/start/:start
	// GET /api/model/:dn/run/:rdn/parameter/:name/value-id/start/:start/count/:count
	router.Get("/api/run-parameter-value-id", runParameterIdReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/parameter/:name/value-id", runParameterIdReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/parameter/:name/value-id/start/:start", runParameterIdReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/parameter/:name/value-id/start/:start/count/:count", runParameterIdReadGetHandler, logRequest)

	// POST /api/model/:dn/run/:rdn/parameter/value-id
	router.Post("/api/model/:dn/run/:rdn/parameter/value-id", runParameterIdReadHandler, logRequest)

	// GET /api/run-table-expr-id?dn=modelOne&rdn=myRun&name=salarySex
	// GET /api/run-table-expr-id?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0
	// GET /api/run-table-expr-id?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0&count=100
	// GET /api/model/:dn/run/:rdn/table/:name/expr-id
	// GET /api/model/:dn/run/:rdn/table/:name/expr-id/start/:start
	// GET /api/model/:dn/run/:rdn/table/:name/expr-id/start/:start/count/:count
	router.Get("/api/run-table-expr-id", runTableIdExprReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/table/:name/expr-id", runTableIdExprReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/table/:name/expr-id/start/:start", runTableIdExprReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/table/:name/expr-id/start/:start/count/:count", runTableIdExprReadGetHandler, logRequest)

	// GET /api/run-table-acc-id?dn=modelOne&rdn=myRun&name=salarySex
	// GET /api/run-table-acc-id?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0
	// GET /api/run-table-acc-id?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0&count=100
	// GET /api/model/:dn/run/:rdn/table/:name/acc-id
	// GET /api/model/:dn/run/:rdn/table/:name/acc-id/start/:start
	// GET /api/model/:dn/run/:rdn/table/:name/acc-id/start/:start/count/:count
	router.Get("/api/run-table-acc-id", runTableIdAccReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/table/:name/acc-id", runTableIdAccReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/table/:name/acc-id/start/:start", runTableIdAccReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/table/:name/acc-id/start/:start/count/:count", runTableIdAccReadGetHandler, logRequest)

	// GET /api/run-table-all-acc-id?dn=modelOne&rdn=myRun&name=salarySex
	// GET /api/run-table-all-acc-id?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0
	// GET /api/run-table-all-acc-id?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0&count=100
	// GET /api/model/:dn/run/:rdn/table/:name/all-acc-id
	// GET /api/model/:dn/run/:rdn/table/:name/all-acc-id/start/:start
	// GET /api/model/:dn/run/:rdn/table/:name/all-acc-id/start/:start/count/:count
	router.Get("/api/run-table-all-acc-id", runTableIdAllAccReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/table/:name/all-acc-id", runTableIdAllAccReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/table/:name/all-acc-id/start/:start", runTableIdAllAccReadGetHandler, logRequest)
	router.Get("/api/model/:dn/run/:rdn/table/:name/all-acc-id/start/:start/count/:count", runTableIdAllAccReadGetHandler, logRequest)

	// POST /api/model/:dn/run/:rdn/table/value-id
	router.Post("/api/model/:dn/run/:rdn/table/value-id", runTableIdReadHandler, logRequest)
}

// add http POST web-service /api routes to update metadata
func apiUpdateRoutes(router *vestigo.Router) {

	// POST /api/workset-readonly
	// POST /api/model/:dn/workset/:wsn/readonly/:val
	router.Post("/api/workset-readonly", worksetReadonlyHandler, logRequest)
	router.Post("/api/model/:dn/workset/:wsn/readonly/:val", worksetReadonlyUrlHandler, logRequest)

	// POST /api/workset-meta
	router.Post("/api/workset-meta", worksetUpdateHandler, logRequest)
}
