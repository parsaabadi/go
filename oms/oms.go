// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
oms is openM++ JSON web-service which is also used as simple web-server for openM++ UI html pages.

Web-service allow to view and update model database(s) and run openM++ models from models/bin subdirectory.
Web-server allow to serve static html (css, images, javascipt) content from html subdirectory.

Arguments for oms can be specified on command line or through .ini file:
  oms -ini my-oms.ini
  oms -OpenM.IniFile my-oms.ini
Command line arguments take precedence over ini-file options.

Following arguments supporetd by oms:

  -oms.RootDir om/root
oms root directory, default: current directory.
Expected to have models/bin/ subdirectory unless other location specified: -oms.ModelDir /dir/models/bin.
Recommended to have models/log/ subdirectory unless other location specified: -oms.ModelLogDir /dir/models/log.
Expected to have html/ subdirectory unless -oms.ApiOnly true specified.
Expected to have etc/ subdirectory with templates to run models on MPI cluster.
Recommended to have log/ subdirectory to store oms web-service log files.

  -oms.ModelDir models/bin
models executable and model.sqlite database files directory, default: models/bin,
If relative then must be relative to oms root directory.

  -oms.ModelLogDir models/log
models log directory, default: models/log, if relative then must be relative to oms root directory.

  -oms.HomeDir models/home
user personal home directory to store files and settings.
If relative then must be relative to oms root directory.
Default value is empty "" string and it is disable use of home directory.

  -oms.HomeRootDir home
root of users home directories to store files and settings.
If relative then must be relative to oms root directory.
Default value is empty "" string and it is disable use of home directories.

  -oms.AllowDownload false
if true then allow download from user home/out/download directory.

  -l localhost:4040
  -oms.Listen localhost:4040
address to listen, default: localhost:4040.
Use -l :4040 if you need to access oms web-service from other computer (make sure firewall configured properly).

  -oms.UrlSaveTo someModel.ui.url.txt
file path to save oms URL which can be used to open web UI in browser.
Default: empty value, URL is not saved in a file by default.
Example of URL file content: http://localhost:4040

  -oms.ApiOnly false
if true then API only web-service, it is false by default and oms also act as http server for openM++ UI.

  -oms.LogRequest false
if true then log HTTP requests on console and/or log file.

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

  -oms.MaxRunHistory 100
max number of model runs to keep in run list history, default: 100.

Also oms support OpenM++ standard log settings (described in openM++ wiki):
  -OpenM.LogToConsole:     if true then log to standard output, default: true
  -v:                      short form of: -OpenM.LogToConsole
  -OpenM.LogToFile:        if true then log to file
  -OpenM.LogFilePath:      path to log file, default = current/dir/exeName.log
  -OpenM.LogUseDailyStamp: if true then use dayily stamp in log file name (rotate log files dayily)
  -OpenM.LogUseTs:         if true then use time-stamp in log file name
  -OpenM.LogUsePid:        if true then use pid-stamp in log file name
  -OpenM.LogSql:           if true then log sql statements into log file
*/
package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/husobee/vestigo"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/text/language"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/omppLog"
)

// config keys to get values from ini-file or command line arguments.
const (
	rootDirArgKey        = "oms.RootDir"       // oms root directory, expected subfoldesr: html, etc, log
	modelDirArgKey       = "oms.ModelDir"      // models executable and model.sqlite directory, if relative then must be relative to oms root directory
	modelLogDirArgKey    = "oms.ModelLogDir"   // models log directory, if relative then must be relative to oms root directory
	homeDirArgKey        = "oms.HomeDir"       // user personal home directory, if relative then must be relative to oms root directory
	homeRootDirArgKey    = "oms.HomeRootDir"   // root of users home directories, if relative then must be relative to oms root directory
	isDownloadArgKey     = "oms.AllowDownload" // if true then allow download from user home sub-directory: home/out/download
	listenArgKey         = "oms.Listen"        // address to listen, default: localhost:4040
	listenShortKey       = "l"                 // address to listen (short form)
	urlFileArgKey        = "oms.UrlSaveTo"     // file path to save oms URL in form of: http://localhost:4040, if relative then must be relative to oms root directory
	logRequestArgKey     = "oms.LogRequest"    // if true then log http request
	apiOnlyArgKey        = "oms.ApiOnly"       // if true then API only web-service, no web UI
	uiLangsArgKey        = "oms.Languages"     // list of supported languages
	encodingArgKey       = "oms.CodePage"      // code page for converting source files, e.g. windows-1252
	pageSizeAgrKey       = "oms.MaxRowCount"   // max number of rows to return from read parameters or output tables
	runHistorySizeAgrKey = "oms.MaxRunHistory" // max number of model runs to keep in run list history
	doubleFormatArgKey   = "oms.DoubleFormat"  // format to convert float or double value to string, e.g. %.15g
)

// front-end UI subdirectory with html and javascript
const htmlDir = "html"

// configuration subdirectory with template(s) to run model on MPI cluster
const etcDir = "etc"

// max number of model run states to keep in run list history
const runHistoryDefaultSize int = 100

// server run configuration
var theCfg = struct {
	rootDir           string            // server root directory
	isSingleUser      bool              // if true then it is a single user mode
	homeDir           string            // user(s) home directory
	downloadDir       string            // if download allowed then it is home/out/download directory
	downloadOutDir    string            // if download allowed then it is home/out directory
	dbcopyPath        string            // if download allowed then it is path to dbcopy.exe
	pageMaxSize       int64             // default "page" size: row count to read parameters or output tables
	runHistoryMaxSize int               // max number of model run states to keep in run list history
	doubleFmt         string            // format to convert float or double value to string
	env               map[string]string // server config environmemt variables
}{
	pageMaxSize:       100,
	isSingleUser:      false,
	homeDir:           "",
	downloadDir:       "",
	runHistoryMaxSize: runHistoryDefaultSize,
	doubleFmt:         "%.15g",
	env:               map[string]string{},
}

// if true then log http requests
var isLogRequest bool

// matcher to find UI supported language corresponding to request
var uiLangMatcher language.Matcher

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
	_ = flag.String(modelLogDirArgKey, "models/log", "models log directory, if relative then must be relative to root directory")
	_ = flag.String(homeDirArgKey, "", "user personal home directory, if relative then must be relative to root directory")
	_ = flag.String(homeRootDirArgKey, "", "this option is currently disabled")
	_ = flag.Bool(isDownloadArgKey, false, "if true then allow download from user home/out/download directory")
	_ = flag.String(listenArgKey, "localhost:4040", "address to listen")
	_ = flag.String(listenShortKey, "localhost:4040", "address to listen (short form of "+listenArgKey+")")
	_ = flag.Bool(logRequestArgKey, false, "if true then log HTTP requests")
	_ = flag.String(urlFileArgKey, "", "file path to save oms URL, if relative then must be relative to root directory")
	_ = flag.Bool(apiOnlyArgKey, false, "if true then API only web-service, no web UI")
	_ = flag.String(uiLangsArgKey, "en", "comma-separated list of supported languages")
	_ = flag.String(encodingArgKey, "", "code page to convert source file into utf-8, e.g.: windows-1252")
	_ = flag.Int64(pageSizeAgrKey, theCfg.pageMaxSize, "max number of rows to return from read parameters or output tables")
	_ = flag.Int(runHistorySizeAgrKey, runHistoryDefaultSize, "max number of model runs to keep in run list history")
	_ = flag.String(doubleFormatArgKey, theCfg.doubleFmt, "format to convert float or double value to string")

	// pairs of full and short argument names to map short name to full name
	optFs := []config.FullShort{
		{Full: listenArgKey, Short: listenShortKey},
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

	theCfg.pageMaxSize = runOpts.Int64(pageSizeAgrKey, theCfg.pageMaxSize)
	theCfg.doubleFmt = runOpts.String(doubleFormatArgKey)

	theCfg.runHistoryMaxSize = runOpts.Int(runHistorySizeAgrKey, runHistoryDefaultSize)
	if theCfg.runHistoryMaxSize <= 0 {
		theCfg.runHistoryMaxSize = runHistoryDefaultSize
	}

	theCfg.rootDir = runOpts.String(rootDirArgKey) // server root directory

	// get server config environmemt variables
	env := os.Environ()
	for _, e := range env {
		if strings.HasPrefix(e, "OM_CFG_") {
			if kv := strings.SplitN(e, "=", 2); len(kv) == 2 {
				theCfg.env[kv[0]] = kv[1]
			}
		}
	}

	// change to root directory
	if theCfg.rootDir != "" && theCfg.rootDir != "." {
		if err := os.Chdir(theCfg.rootDir); err != nil {
			return errors.New("Error: unable to change directory: " + err.Error())
		}
	}
	omppLog.New(logOpts) // adjust log options, log path can be relative to root directory

	if theCfg.rootDir != "" && theCfg.rootDir != "." {
		omppLog.Log("Changing directory to: ", theCfg.rootDir)
	}

	// if UI required then server root directory must have html subdir
	if !isApiOnly {
		if err := isDirExist(htmlDir); err != nil {
			isApiOnly = true
			omppLog.Log("Warning: serving API only because UI directory not found: ", filepath.Join(theCfg.rootDir, htmlDir))
		}
	}

	// check if it is single user run mode and use of home directory enabled
	if theCfg.homeDir = runOpts.String(homeDirArgKey); theCfg.homeDir != "" {
		if err := isDirExist(theCfg.homeDir); err != nil {
			omppLog.Log("Warning: user home directory not found: ", theCfg.homeDir)
			theCfg.homeDir = ""
		}
		theCfg.isSingleUser = theCfg.homeDir != ""
	}
	if runOpts.String(homeRootDirArgKey) != "" {
		return errors.New("Error: this option is currently disabled: " + homeRootDirArgKey)
	}

	// check download option: home/out/download directory must exist and dbcopy.exe must exist
	isDownload := false
	if runOpts.Bool(isDownloadArgKey) {
		if theCfg.homeDir != "" {

			theCfg.downloadOutDir = filepath.Join(theCfg.homeDir, "out")          // download directory for web-server, to serve static content
			theCfg.downloadDir = filepath.Join(theCfg.downloadOutDir, "download") // download directory UI

			if err = isDirExist(theCfg.downloadDir); err == nil {
				theCfg.dbcopyPath = dbcopyPath(args[0])
			}
		}
		isDownload = theCfg.downloadDir != "" && theCfg.downloadOutDir != "" && theCfg.dbcopyPath != ""
		if !isDownload {
			theCfg.downloadDir = ""
			theCfg.downloadOutDir = ""
			theCfg.dbcopyPath = ""
			omppLog.Log("Warning: user home download directory not found or dbcopy not found, download disabled")
		}
	}

	// model directory required to build initial list of model sqlite files
	modelLogDir := runOpts.String(modelLogDirArgKey)
	modelDir := runOpts.String(modelDirArgKey)
	if modelDir == "" {
		return errors.New("Error: model directory argument cannot be empty")
	}
	omppLog.Log("Model directory: ", modelDir)

	if err := theCatalog.refreshSqlite(modelDir, modelLogDir); err != nil {
		return err
	}

	// etc subdirectory required to run MPI models
	if err := isDirExist(etcDir); err != nil {
		omppLog.Log("Warning: templates directory not found, it is required to run models on MPI cluster: ", filepath.Join(theCfg.rootDir, etcDir))
	}

	// refresh run state catalog and start scanning model log files
	if err := theRunCatalog.refreshCatalog(etcDir); err != nil {
		return err
	}

	doneScanC := make(chan bool)
	go scanModelLogDirs(doneScanC)

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

	apiGetRoutes(router)     // web-service /api routes to get metadata
	apiReadRoutes(router)    // web-service /api routes to read values
	apiReadCsvRoutes(router) // web-service /api routes to read values into csv stream
	if isDownload {
		apiDownloadRoutes(router) // web-service /api routes to download files from home/out/download
	}
	apiUpdateRoutes(router)   // web-service /api routes to update metadata
	apiRunModelRoutes(router) // web-service /api routes to run the model
	apiUserRoutes(router)     // web-service /api routes for user-specific requests
	apiAdminRoutes(router)    // web-service /api routes for administrative tasks

	// serve static content from home/out/download folder
	if isDownload {
		router.Get("/download/*", downloadHandler, logRequest)
	}

	// set web root handler: UI web pages or "not found" if this is web-service mode
	if !isApiOnly {
		router.Get("/*", homeHandler, logRequest) // serve UI web pages
	} else {
		router.Get("/*", http.NotFound) // only /api, any other pages not found
	}

	// initialize server
	addr := runOpts.String(listenArgKey)
	srv := http.Server{Addr: addr, Handler: router}

	// add shutdown handler, it does not wait for requests, it does reset connections and exit
	// PUT /api/admin/shutdown
	ctx, cancel := context.WithCancel((context.Background()))
	defer cancel()

	adminShutdownHandler := func(w http.ResponseWriter, r *http.Request) {

		// close models catalog
		omppLog.Log("Shutdown server...")
		if err := theCatalog.close(); err != nil {
			omppLog.Log(err)
		}

		// send response: confirm shutdown
		srv.SetKeepAlivesEnabled(false)
		w.Write([]byte("Shutdown completed"))

		cancel() // send shutdown completed to the main
	}
	router.Put("/api/admin/shutdown", adminShutdownHandler, logRequest)

	// start to listen at specified TCP address
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		omppLog.Log("Error at start of TCP listen: ", addr)
		return err
	}
	ta, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return errors.New("Error: unable to find TCP port of: " + addr)
	}
	localUrl := "http://localhost:" + strconv.Itoa(ta.Port)

	// if url file path specified then write oms url into that url file
	if urlFile := runOpts.String(urlFileArgKey); urlFile != "" {
		if err = os.WriteFile(urlFile, []byte(localUrl), 0644); err != nil {
			omppLog.Log("Error at writing into: ", urlFile)
			return err
		}
	}

	// initialization completed, notify user and start the server
	omppLog.Log("Listen at ", addr)
	if !isApiOnly {
		omppLog.Log("To start open in your browser: ", localUrl)
	}
	omppLog.Log("To finish press Ctrl+C")

	go func() {
		if err = srv.Serve(ln); err != nil {
			// if err = srv.ListenAndServe(); err != nil {
			// send completed by error to the main
			// error may be http.ErrServerClosed by shutdown which is not an actual error
			cancel()
		}
	}()

	// wait for shutdown or Ctrl+C interupt signal
	<-ctx.Done()
	if e := srv.Shutdown(context.Background()); e != nil && e != http.ErrServerClosed {
		omppLog.Log("Shutdown error: ", e)
	} else {
		// shutdown completed without error: clean main error code
		if err == http.ErrServerClosed {
			err = nil
		}
	}

	doneScanC <- true
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

// homeHandler is static pages handler for front-end UI served on web / root.
// Only GET requests expected.
func homeHandler(w http.ResponseWriter, r *http.Request) {
	setContentType(http.FileServer(http.Dir(htmlDir))).ServeHTTP(w, r)
}

// downloadHandler is static file download handler from user home/out/download folder.
// Only GET requests expected.
func downloadHandler(w http.ResponseWriter, r *http.Request) {
	setContentType(http.FileServer(http.Dir(theCfg.downloadOutDir))).ServeHTTP(w, r)
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
	// GET model extra: languages, profile(s)
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

	// GET /api/model/:model/profile/:profile
	// GET /api/model-profile?model=modelNameOrDigest&profile=profileName
	router.Get("/api/model/:model/profile/:profile", modelProfileHandler, logRequest)
	router.Get("/api/model-profile", modelProfileHandler, logRequest)

	// GET /api/model/:model/profile-list
	// GET /api/model-profile-list?model=modelNameOrDigest
	router.Get("/api/model/:model/profile-list", modelProfileListHandler, logRequest)
	router.Get("/api/model-profile-list", modelProfileListHandler, logRequest)

	//
	// GET model run results
	//

	// GET /api/model/:model/run-list
	// GET /api/model-run-list?model=modelNameOrDigest
	router.Get("/api/model/:model/run-list", runListHandler, logRequest)
	router.Get("/api/model-run-list", runListHandler, logRequest)

	// GET /api/model/:model/run-list/text
	// GET /api/model/:model/run-list/text/lang/:lang
	// GET /api/model-run-list-text?model=modelNameOrDigest&lang=en
	router.Get("/api/model/:model/run-list/text", runListTextHandler, logRequest)
	router.Get("/api/model/:model/run-list/text/lang/:lang", runListTextHandler, logRequest)
	router.Get("/api/model-run-list-text", runListTextHandler, logRequest)

	// GET /api/model/:model/run/:run/status
	// GET /api/model-run-status?model=modelNameOrDigest&run=runDigestOrStampOrName
	router.Get("/api/model/:model/run/:run/status", runStatusHandler, logRequest)
	router.Get("/api/model-run-status", runStatusHandler, logRequest)

	// GET /api/model/:model/run/:run/status/list
	// GET /api/model-run-status-list?model=modelNameOrDigest&run=runDigestOrStampOrName
	router.Get("/api/model/:model/run/:run/status/list", runStatusListHandler, logRequest)
	router.Get("/api/model-run-status-list", runStatusListHandler, logRequest)

	// GET /api/model/:model/run/status/first
	// GET /api/model-run-first-status?model=modelNameOrDigest
	router.Get("/api/model/:model/run/status/first", firstRunStatusHandler, logRequest)
	router.Get("/api/model-run-first-status", firstRunStatusHandler, logRequest)

	// GET /api/model/:model/run/status/last
	// GET /api/model-run-last-status?model=modelNameOrDigest
	router.Get("/api/model/:model/run/status/last", lastRunStatusHandler, logRequest)
	router.Get("/api/model-run-last-status", lastRunStatusHandler, logRequest)

	// GET /api/model/:model/run/status/last-completed
	// GET /api/model-run-last-completed-status?model=modelNameOrDigest
	router.Get("/api/model/:model/run/status/last-completed", lastCompletedRunStatusHandler, logRequest)
	router.Get("/api/model-run-last-completed-status", lastCompletedRunStatusHandler, logRequest)

	// GET /api/model/:model/run/:run
	// GET /api/model-run?model=modelNameOrDigest&run=runDigestOrStampOrName
	router.Get("/api/model/:model/run/:run", runFullHandler, logRequest)
	router.Get("/api/model-run", runFullHandler, logRequest)

	router.Get("/api/model/:model/run/:run/", http.NotFound) // reject if request ill-formed

	// GET /api/model/:model/run/:run/text
	// GET /api/model/:model/run/:run/text/lang/:lang
	// GET /api/model-run-text?model=modelNameOrDigest&run=runDigestOrStampOrName&lang=en
	router.Get("/api/model/:model/run/:run/text", runTextHandler, logRequest)
	router.Get("/api/model/:model/run/:run/text/lang/:lang", runTextHandler, logRequest)
	router.Get("/api/model-run-text", runTextHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/text/", http.NotFound)
	router.Get("/api/model/:model/run/:run/text/lang/", http.NotFound)

	// GET /api/model/:model/run/:run/text/all
	// GET /api/model-run-text-all?model=modelNameOrDigest&run=runDigestOrStampOrName
	router.Get("/api/model/:model/run/:run/text/all", runAllTextHandler, logRequest)
	router.Get("/api/model-run-text-all", runAllTextHandler, logRequest)

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

	// GET /api/model/:model/workset/:set/status
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
	// reject if request ill-formed
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

	// GET /api/model/:model/task/:task/run-status/run/:run
	// GET /api/task-run-status?model=modelNameOrDigest&task=taskName&run=taskRunStampOrName
	router.Get("/api/model/:model/task/:task/run-status/run/:run", taskRunStatusHandler, logRequest)
	router.Get("/api/task-run-status", taskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/list/:run
	// GET /api/task-run-status-list?model=modelNameOrDigest&task=taskName&run=taskRunStampOrName
	router.Get("/api/model/:model/task/:task/run-status/list/:run", taskRunStatusListHandler, logRequest)
	router.Get("/api/task-run-status-list", taskRunStatusListHandler, logRequest)

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
	// reject if request ill-formed
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
	// reject if request ill-formed
	router.Get("/api/model/:model/workset/:set/parameter/:name/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/parameter/:name/value
	// GET /api/model/:model/run/:run/parameter/:name/value/start/:start
	// GET /api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count
	// GET /api/run-parameter-value?model=modelNameOrDigest&run=runDigestOrStampOrName&name=parameterName&start=0&count=100
	router.Get("/api/model/:model/run/:run/parameter/:name/value", runParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start", runParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count", runParameterPageGetHandler, logRequest)
	router.Get("/api/run-parameter-value", runParameterPageGetHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/parameter/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/expr
	// GET /api/model/:model/run/:run/table/:name/expr/start/:start
	// GET /api/model/:model/run/:run/table/:name/expr/start/:start/count/:count
	// GET /api/run-table-expr?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&start=0&count=100
	router.Get("/api/model/:model/run/:run/table/:name/expr", runTableExprPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start", runTableExprPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start/count/:count", runTableExprPageGetHandler, logRequest)
	router.Get("/api/run-table-expr", runTableExprPageGetHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/acc/start/:start
	// GET /api/model/:model/run/:run/table/:name/acc/start/:start/count/:count
	// GET /api/run-table-acc?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&start=0&count=100
	router.Get("/api/model/:model/run/:run/table/:name/acc", runTableAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start", runTableAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start/count/:count", runTableAccPageGetHandler, logRequest)
	router.Get("/api/run-table-acc", runTableAccPageGetHandler, logRequest)
	// reject if request ill-formed
	// router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/all-acc
	// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start
	// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count
	// GET /api/run-table-all-acc?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&start=0&count=100
	router.Get("/api/model/:model/run/:run/table/:name/all-acc", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/run-table-all-acc", runTableAllAccPageGetHandler, logRequest)
	// reject if request ill-formed
	// router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start/count/", http.NotFound)
}

// add http GET web-service /api routes to read parameters or output tables as csv stream
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
	// GET /api/run-parameter-csv?model=modelNameOrDigest&run=runDigestOrStampOrName&name=parameterName&bom=true
	router.Get("/api/model/:model/run/:run/parameter/:name/csv", runParameterCsvGetHandler, logRequest)
	router.Get("/api/run-parameter-csv", runParameterCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv-bom
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-bom", runParameterCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv-id
	// GET /api/run-parameter-csv-id?model=modelNameOrDigest&run=runDigestOrStampOrName&name=parameterName&bom=true
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-id", runParameterIdCsvGetHandler, logRequest)
	router.Get("/api/run-parameter-csv-id", runParameterIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv-id-bom
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-id-bom", runParameterIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv
	// GET /api/run-table-expr-csv?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv", runTableExprCsvGetHandler, logRequest)
	router.Get("/api/run-table-expr-csv", runTableExprCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv-bom
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-bom", runTableExprCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv-id
	// GET /api/run-table-expr-csv-id?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-id", runTableExprIdCsvGetHandler, logRequest)
	router.Get("/api/run-table-expr-csv-id", runTableExprIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-id-bom", runTableExprIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv
	// GET /api/run-table-acc-csv?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv", runTableAccCsvGetHandler, logRequest)
	router.Get("/api/run-table-acc-csv", runTableAccCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv-bom
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-bom", runTableAccCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv-id
	// GET /api/run-table-acc-csv-id?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-id", runTableAccIdCsvGetHandler, logRequest)
	router.Get("/api/run-table-acc-csv-id", runTableAccIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-id-bom", runTableAccIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv
	// GET /api/run-table-all-acc-csv?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv", runTableAllAccCsvGetHandler, logRequest)
	router.Get("/api/run-table-all-acc-csv", runTableAllAccCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-bom
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-bom", runTableAllAccCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id
	// GET /api/run-table-all-acc-csv-id?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&bom=true
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-id", runTableAllAccIdCsvGetHandler, logRequest)
	router.Get("/api/run-table-all-acc-csv-id", runTableAllAccIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-id-bom", runTableAllAccIdCsvBomGetHandler, logRequest)
}

// add http GET and POST web-service /api routes to download files from home/out/download folder
func apiDownloadRoutes(router *vestigo.Router) {

	// GET /api/download/log/all
	router.Get("/api/download/log/all", allLogDownloadGetHandler, logRequest)

	// GET /api/download/log/model/:model
	router.Get("/api/download/log/model/:model", modelLogDownloadGetHandler, logRequest)

	// GET /api/download/log/file/:name
	router.Get("/api/download/log/file/:name", fileLogDownloadGetHandler, logRequest)

	// GET /api/download/file-tree/:folder
	router.Get("/api/download/file-tree/:folder", fileTreeDownloadGetHandler, logRequest)

	// POST /api/download/model/:model
	router.Post("/api/download/model/:model", modelDownloadPostHandler, logRequest)

	// POST /api/download/model/:model/run/:run
	router.Post("/api/download/model/:model/run/:run", runDownloadPostHandler, logRequest)

	// POST /api/download/model/:model/workset/:set
	router.Post("/api/download/model/:model/workset/:set", worksetDownloadPostHandler, logRequest)

	// DELETE /api/download/delete/:folder
	router.Delete("/api/download/delete/:folder", downloadDeleteHandler, logRequest)
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

	// PUT  /api/workset-create
	router.Put("/api/workset-create", worksetCreateHandler, logRequest)

	// PUT  /api/workset-new
	router.Put("/api/workset-new", worksetReplaceHandler, logRequest)

	// PATCH /api/workset
	router.Patch("/api/workset", worksetMergeHandler, logRequest)

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
	// POST /api/copy-parameter-from-run?model=modelNameOrDigest&set=setName&name=parameterName&run=runDigestOrStampOrName"
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
	router.Patch("/api/run/text", runTextMergeHandler, logRequest)

	// DELETE /api/model/:model/run/:run
	// POST   /api/run/delete?model=modelNameOrDigest&run=runDigestOrStampOrName
	router.Delete("/api/model/:model/run/:run", runDeleteHandler, logRequest)
	router.Post("/api/run-delete", runDeleteHandler, logRequest)

	//
	// update modeling task and task run history
	//

	// PUT  /api/task-new
	router.Put("/api/task-new", taskDefReplaceHandler, logRequest)

	// PATCH /api/task
	router.Patch("/api/task", taskDefMergeHandler, logRequest)

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

	// reject run log if request ill-formed
	router.Get("/api/run/log/model/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start/count/", http.NotFound)
}

// add web-service /api routes for user-specific request
func apiUserRoutes(router *vestigo.Router) {

	// GET /api/user/view/model/:model
	router.Get("/api/user/view/model/:model", userViewGetHandler, logRequest)

	// PUT  /api/user/view/model/:model
	router.Put("/api/user/view/model/:model", userViewPutHandler, logRequest)

	// DELETE /api/user/view/model/:model
	router.Delete("/api/user/view/model/:model", userViewDeleteHandler, logRequest)
}

// add web-service /api routes for administrative tasks
func apiAdminRoutes(router *vestigo.Router) {

	// GET /api/service/config
	router.Get("/api/service/config", serviceConfigHandler, logRequest)

	// GET /api/service/state
	router.Get("/api/service/state", http.NotFound)

	// POST /api/admin/all-models/refresh
	router.Post("/api/admin/all-models/refresh", allModelsRefreshHandler, logRequest)

	// POST /api/admin/all-models/close
	router.Post("/api/admin/all-models/close", allModelsCloseHandler, logRequest)
}
