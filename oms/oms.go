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
Recommended to have log/ subdirectory to store oms web-service log files.

  -oms.ModelDir models/bin
models executable and model.sqlite database files directory, default: models/bin,
If relative then must be relative to oms root directory.

  -oms.ModelLogDir models/log
models log directory, default: models/log, if relative then must be relative to oms root directory.

  -oms.HtmlDir html
front-end UI directory, default: html.
If relative then must be relative to oms root directory.
It is not used if -oms.ApiOnly specified.

  -oms.EtcDir etc
configuration files directory, default: etc.
If relative then must be relative to oms root directory.
It is an optional directory, it may contain configuration files,for example, templates to run models on MPI cluster.

  -oms.JobDir job
jobs control directory.
If relative then must be relative to oms root directory.
Jobs control allow to manage computational resources (e.g. CPUs) and organize model run queue.
Default value is empty "" string and it is disable jobs control.

  -oms.Name someName
instance name which used for job control.

-oms.HomeDir models/home
user personal home directory to store files and settings.
If relative then must be relative to oms root directory.
Default value is empty "" string and it is disable use of home directory.

  -oms.AllowDownload false
if true then allow download from user home/io/download directory.

  -oms.AllowUpload false
if true then allow upload to user home/io/upload directory.

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
max number of completed model runs to keep in run list history, default: 100.

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
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// config keys to get values from ini-file or command line arguments.
const (
	listenArgKey         = "oms.Listen"        // address to listen, default: localhost:4040
	listenShortKey       = "l"                 // address to listen (short form)
	omsNameArgKey        = "oms.Name"          // oms instance name, if empty then derived from address to listen
	urlFileArgKey        = "oms.UrlSaveTo"     // file path to save oms URL in form of: http://localhost:4040, if relative then must be relative to oms root directory
	rootDirArgKey        = "oms.RootDir"       // oms root directory, expected to contain log subfolder
	modelDirArgKey       = "oms.ModelDir"      // models executable and model.sqlite directory, if relative then must be relative to oms root directory
	modelLogDirArgKey    = "oms.ModelLogDir"   // models log directory, if relative then must be relative to oms root directory
	etcDirArgKey         = "oms.EtcDir"        // configuration files directory, if relative then must be relative to oms root directory
	htmlDirArgKey        = "oms.HtmlDir"       // front-end UI directory, if relative then must be relative to oms root directory
	jobDirArgKey         = "oms.JobDir"        // job control directory, if relative then must be relative to oms root directory
	homeDirArgKey        = "oms.HomeDir"       // user personal home directory, if relative then must be relative to oms root directory
	isDownloadArgKey     = "oms.AllowDownload" // if true then allow download from user home sub-directory: home/io/download
	isUploadArgKey       = "oms.AllowUpload"   // if true then allow upload to user home sub-directory: home/io/upload
	logRequestArgKey     = "oms.LogRequest"    // if true then log http request
	apiOnlyArgKey        = "oms.ApiOnly"       // if true then API only web-service, no web UI
	uiLangsArgKey        = "oms.Languages"     // list of supported languages
	encodingArgKey       = "oms.CodePage"      // code page for converting source files, e.g. windows-1252
	pageSizeAgrKey       = "oms.MaxRowCount"   // max number of rows to return from read parameters or output tables
	runHistorySizeAgrKey = "oms.MaxRunHistory" // max number of completed model runs to keep in run list history
	doubleFormatArgKey   = "oms.DoubleFormat"  // format to convert float or double value to string, e.g. %.15g
)

// max number of completed model run states to keep in run list history
const runHistoryDefaultSize int = 1000

// server run configuration
var theCfg = struct {
	rootDir           string            // server root directory
	htmlDir           string            // front-end UI directory with html and javascript
	etcDir            string            // configuration files directory
	isHome            bool              // if true then it is a single user mode
	homeDir           string            // user home directory
	downloadDir       string            // if download allowed then it is home/io/download directory
	uploadDir         string            // if upload allowed then it is home/io/upload directory
	inOutDir          string            // if download or upload allowed then it is home/io directory
	isJobControl      bool              // if true then do job control: model run queue and resource allocation
	jobDir            string            // job control directory
	omsName           string            // oms instance name, if empty then derived from address to listen
	dbcopyPath        string            // if download or upload allowed then it is path to dbcopy.exe
	pageMaxSize       int64             // default "page" size: row count to read parameters or output tables
	runHistoryMaxSize int               // max number of completed model run states to keep in run list history
	doubleFmt         string            // format to convert float or double value to string
	codePage          string            // "code page" to convert source file into utf-8, for example: windows-1252
	env               map[string]string // server config environmemt variables
}{
	pageMaxSize:       100,
	htmlDir:           "html",
	etcDir:            "etc",
	isHome:            false,
	homeDir:           "",
	downloadDir:       "",
	uploadDir:         "",
	isJobControl:      false,
	jobDir:            "",
	omsName:           "",
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
	_ = flag.String(etcDirArgKey, theCfg.etcDir, "configuration files directory, if relative then must be relative to oms root director")
	_ = flag.String(htmlDirArgKey, theCfg.htmlDir, "front-end UI directory, if relative then must be relative to root directory")
	_ = flag.String(homeDirArgKey, "", "user personal home directory, if relative then must be relative to root directory")
	_ = flag.Bool(isDownloadArgKey, false, "if true then allow download from user home/io/download directory")
	_ = flag.Bool(isUploadArgKey, false, "if true then allow upload to user home/io/upload directory")
	_ = flag.String(jobDirArgKey, "", "job control directory, if relative then must be relative to root directory")
	_ = flag.String(listenArgKey, "localhost:4040", "address to listen")
	_ = flag.String(listenShortKey, "localhost:4040", "address to listen (short form of "+listenArgKey+")")
	_ = flag.String(omsNameArgKey, "", "instance name, automatically generated if empty")
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
	theCfg.codePage = runOpts.String(encodingArgKey)

	// get server config environmemt variables
	env := os.Environ()
	for _, e := range env {
		if strings.HasPrefix(e, "OM_CFG_") {
			if kv := strings.SplitN(e, "=", 2); len(kv) == 2 {
				theCfg.env[kv[0]] = kv[1]
			}
		}
	}

	omsAbsPath, err := filepath.Abs(args[0])
	if err != nil {
		return errors.New("Error: unable to make absolute path to oms: " + err.Error())
	}

	// change to root directory
	theCfg.rootDir = runOpts.String(rootDirArgKey) // server root directory

	if theCfg.rootDir != "" && theCfg.rootDir != "." {
		if err := os.Chdir(theCfg.rootDir); err != nil {
			return errors.New("Error: unable to change directory: " + err.Error())
		}
	}
	omppLog.New(logOpts) // adjust log options, log path can be relative to root directory

	if theCfg.rootDir != "" && theCfg.rootDir != "." {
		omppLog.Log("Change directory to: ", theCfg.rootDir)
	}

	// model directory required to build initial list of model sqlite files
	modelLogDir := runOpts.String(modelLogDirArgKey)
	modelDir := filepath.Clean(runOpts.String(modelDirArgKey))
	if modelDir == "" || modelDir == "." {
		return errors.New("Error: model directory argument cannot be empty or . dot")
	}
	omppLog.Log("Models directory:    ", modelDir)

	if err := theCatalog.refreshSqlite(modelDir, modelLogDir); err != nil {
		return err
	}

	// check if it is single user run mode and use of home directory enabled
	if theCfg.homeDir = runOpts.String(homeDirArgKey); theCfg.homeDir != "" {
		if err := dirExist(theCfg.homeDir); err != nil {
			omppLog.Log("Warning: user home directory not found: ", theCfg.homeDir)
			theCfg.homeDir = ""
		}
		theCfg.isHome = theCfg.homeDir != ""
		if theCfg.isHome {
			omppLog.Log("User directory:      ", theCfg.homeDir)
		}
	}

	// check download and upload options:
	// home/io/download or home/io/upload directory must exist and dbcopy.exe must exist
	isInOut := runOpts.Bool(isDownloadArgKey) || runOpts.Bool(isUploadArgKey)
	isDownload := false
	isUpload := false

	if isInOut {
		if theCfg.homeDir != "" {

			theCfg.inOutDir = filepath.Join(theCfg.homeDir, "io") // download and upload directory for web-server, to serve static content

			if err = dirExist(theCfg.inOutDir); err == nil {
				theCfg.dbcopyPath = dbcopyPath(omsAbsPath)
			}
		}
		isInOut = theCfg.inOutDir != "" && theCfg.dbcopyPath != ""
		if !isInOut {
			theCfg.inOutDir = ""
			theCfg.dbcopyPath = ""
		}
	}
	if runOpts.Bool(isDownloadArgKey) {
		if isInOut && theCfg.inOutDir != "" {

			theCfg.downloadDir = filepath.Join(theCfg.inOutDir, "download") // download directory UI

			if err = dirExist(theCfg.downloadDir); err != nil {
				theCfg.downloadDir = ""
			}
		}
		isDownload = isInOut && theCfg.downloadDir != ""
		if !isDownload {
			theCfg.downloadDir = ""
			omppLog.Log("Warning: user home download directory not found or dbcopy not found, download disabled")
		} else {
			omppLog.Log("Download directory:  ", theCfg.downloadDir)
		}
	}
	if runOpts.Bool(isUploadArgKey) {
		if isInOut && theCfg.inOutDir != "" {

			theCfg.uploadDir = filepath.Join(theCfg.inOutDir, "upload") // upload directory UI

			if err = dirExist(theCfg.uploadDir); err != nil {
				theCfg.uploadDir = ""
			}
		}
		isUpload = isInOut && theCfg.uploadDir != ""
		if !isUpload {
			theCfg.uploadDir = ""
			omppLog.Log("Warning: user home upload directory not found or dbcopy not found, upload disabled")
		} else {
			omppLog.Log("Upload directory:    ", theCfg.uploadDir)
		}
	}

	// if UI required then server root directory must have html subdir
	if !isApiOnly {
		theCfg.htmlDir = runOpts.String(htmlDirArgKey)
		if err := dirExist(theCfg.htmlDir); err != nil {
			isApiOnly = true
			omppLog.Log("Warning: serving API only because UI directory not found: ", theCfg.htmlDir)
		} else {
			omppLog.Log("HTML UI directory:   ", theCfg.htmlDir)
		}
	}

	// check if job control is required:
	theCfg.jobDir = runOpts.String(jobDirArgKey)
	if err := jobDirValid(theCfg.jobDir); err != nil {
		return errors.New("Error: invalid job control directory: " + err.Error())
	}
	theCfg.isJobControl = theCfg.jobDir != ""
	if theCfg.isJobControl {
		omppLog.Log("Jobs directory:      ", theCfg.jobDir)
	}

	// etc subdirectory required to run MPI models
	theCfg.etcDir = runOpts.String(etcDirArgKey)
	if err := dirExist(theCfg.etcDir); err != nil {
		omppLog.Log("Warning: configuration files directory not found, it is required to run models on MPI cluster: ", filepath.Join(theCfg.etcDir))
	} else {
		omppLog.Log("Etc directory:       ", theCfg.etcDir)
	}

	// make instance name, use address to listen if name not specified
	theCfg.omsName = runOpts.String(omsNameArgKey)
	if theCfg.omsName == "" {
		theCfg.omsName = runOpts.String(listenArgKey)
	}
	theCfg.omsName = helper.CleanPath(theCfg.omsName)
	omppLog.Log("Oms instance name:   ", theCfg.omsName)

	// refresh run state catalog and start scanning model log files
	jsc, _ := jobStateRead()
	if err := theRunCatalog.refreshCatalog(theCfg.etcDir, jsc); err != nil {
		return err
	}

	doneLogScanC := make(chan bool)
	go scanModelLogDirs(doneLogScanC)

	// start scanning for model run jobs
	doneOuterJobScanC := make(chan bool)
	go scanOuterJobs(doneOuterJobScanC)

	doneStateJobScanC := make(chan bool)
	go scanStateJobs(doneStateJobScanC)

	doneRunScanC := make(chan bool)
	go scanRunJobs(doneRunScanC)

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
		apiDownloadRoutes(router) // web-service /api routes to download files from home/io/download
	}
	if isUpload {
		apiUploadRoutes(router) // web-service /api routes to upload files to home/io/upload
	}
	apiUpdateRoutes(router)   // web-service /api routes to update metadata
	apiRunModelRoutes(router) // web-service /api routes to run the model
	apiUserRoutes(router)     // web-service /api routes for user-specific requests
	apiServiceRoutes(router)  // web-service /api routes for service state
	apiAdminRoutes(router)    // web-service /api routes for administrative tasks

	// serve static content from home/io/download folder
	if isDownload {
		router.Get("/download/*", downloadHandler, logRequest)
	}
	// serve static content from home/io/upload folder
	if isUpload {
		router.Get("/upload/*", downloadHandler, logRequest)
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

	doneRunScanC <- true
	doneStateJobScanC <- true
	doneOuterJobScanC <- true
	doneLogScanC <- true
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
	setContentType(http.FileServer(http.Dir(theCfg.htmlDir))).ServeHTTP(w, r)
}

// downloadHandler is static file download handler from user home/io/download and home/io/upload folders.
// Files served from home/io directory URLs are:
//   https://domain.name/download/file.name
//   https://domain.name/upload/file.name
// Only GET requests expected.
func downloadHandler(w http.ResponseWriter, r *http.Request) {
	setContentType(http.FileServer(http.Dir(theCfg.inOutDir))).ServeHTTP(w, r)
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
	router.Get("/api/model-list/text", modelTextListHandler, logRequest)
	router.Get("/api/model-list/text/lang/:lang", modelTextListHandler, logRequest)

	// GET /api/model/:model
	router.Get("/api/model/:model", modelMetaHandler, logRequest)

	// GET /api/model/:model/text
	// GET /api/model/:model/text/lang/:lang
	router.Get("/api/model/:model/text", modelTextHandler, logRequest)
	router.Get("/api/model/:model/text/lang/:lang", modelTextHandler, logRequest)

	// GET /api/model/:model/text/all
	router.Get("/api/model/:model/text/all", modelAllTextHandler, logRequest)

	//
	// GET model extra: languages, profile(s)
	//

	// GET /api/model/:model/lang-list
	router.Get("/api/model/:model/lang-list", langListHandler, logRequest)

	// GET /api/model/:model/word-list
	// GET /api/model/:model/word-list/lang/:lang
	router.Get("/api/model/:model/word-list", wordListHandler, logRequest)
	router.Get("/api/model/:model/word-list/lang/:lang", wordListHandler, logRequest)

	// GET /api/model/:model/profile/:profile
	router.Get("/api/model/:model/profile/:profile", modelProfileHandler, logRequest)

	// GET /api/model/:model/profile-list
	router.Get("/api/model/:model/profile-list", modelProfileListHandler, logRequest)

	//
	// GET model run results
	//

	// GET /api/model/:model/run-list
	router.Get("/api/model/:model/run-list", runListHandler, logRequest)

	// GET /api/model/:model/run-list/text
	// GET /api/model/:model/run-list/text/lang/:lang
	router.Get("/api/model/:model/run-list/text", runListTextHandler, logRequest)
	router.Get("/api/model/:model/run-list/text/lang/:lang", runListTextHandler, logRequest)

	// GET /api/model/:model/run/:run/status
	router.Get("/api/model/:model/run/:run/status", runStatusHandler, logRequest)

	// GET /api/model/:model/run/:run/status/list
	router.Get("/api/model/:model/run/:run/status/list", runStatusListHandler, logRequest)

	// GET /api/model/:model/run/status/first
	router.Get("/api/model/:model/run/status/first", firstRunStatusHandler, logRequest)

	// GET /api/model/:model/run/status/last
	router.Get("/api/model/:model/run/status/last", lastRunStatusHandler, logRequest)

	// GET /api/model/:model/run/status/last-completed
	router.Get("/api/model/:model/run/status/last-completed", lastCompletedRunStatusHandler, logRequest)

	// GET /api/model/:model/run/:run
	router.Get("/api/model/:model/run/:run", runFullHandler, logRequest)

	router.Get("/api/model/:model/run/:run/", http.NotFound) // reject if request ill-formed

	// GET /api/model/:model/run/:run/text
	// GET /api/model/:model/run/:run/text/lang/:lang
	router.Get("/api/model/:model/run/:run/text", runTextHandler, logRequest)
	router.Get("/api/model/:model/run/:run/text/lang/:lang", runTextHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/text/", http.NotFound)
	router.Get("/api/model/:model/run/:run/text/lang/", http.NotFound)

	// GET /api/model/:model/run/:run/text/all
	router.Get("/api/model/:model/run/:run/text/all", runAllTextHandler, logRequest)

	//
	// GET model set of input parameters (workset)
	//

	// GET /api/model/:model/workset-list
	router.Get("/api/model/:model/workset-list", worksetListHandler, logRequest)

	// GET /api/model/:model/workset-list/text
	// GET /api/model/:model/workset-list/text/lang/:lang
	router.Get("/api/model/:model/workset-list/text", worksetListTextHandler, logRequest)
	router.Get("/api/model/:model/workset-list/text/lang/:lang", worksetListTextHandler, logRequest)

	// GET /api/model/:model/workset/:set/status
	router.Get("/api/model/:model/workset/:set/status", worksetStatusHandler, logRequest)

	// GET /api/model/:model/workset/status/default
	router.Get("/api/model/:model/workset/status/default", worksetDefaultStatusHandler, logRequest)

	// GET /api/model/:model/workset/:set/text
	// GET /api/model/:model/workset/:set/text/lang/:lang
	router.Get("/api/model/:model/workset/:set/text", worksetTextHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/text/lang/:lang", worksetTextHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/workset/:set/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/text/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/text/lang/", http.NotFound)

	// GET /api/model/:model/workset/:set/text/all
	router.Get("/api/model/:model/workset/:set/text/all", worksetAllTextHandler, logRequest)

	//
	// GET modeling tasks and task run history
	//

	// GET /api/model/:model/task-list
	router.Get("/api/model/:model/task-list", taskListHandler, logRequest)

	// GET /api/model/:model/task-list/text
	// GET /api/model/:model/task-list/text/lang/:lang
	router.Get("/api/model/:model/task-list/text", taskListTextHandler, logRequest)
	router.Get("/api/model/:model/task-list/text/lang/:lang", taskListTextHandler, logRequest)

	// GET /api/model/:model/task/:task/sets
	router.Get("/api/model/:model/task/:task/sets", taskSetsHandler, logRequest)

	// GET /api/model/:model/task/:task/runs
	router.Get("/api/model/:model/task/:task/runs", taskRunsHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/run/:run
	router.Get("/api/model/:model/task/:task/run-status/run/:run", taskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/list/:run
	router.Get("/api/model/:model/task/:task/run-status/list/:run", taskRunStatusListHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/first
	router.Get("/api/model/:model/task/:task/run-status/first", firstTaskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/last
	router.Get("/api/model/:model/task/:task/run-status/last", lastTaskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/last-completed
	router.Get("/api/model/:model/task/:task/run-status/last-completed", lastCompletedTaskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/text
	// GET /api/model/:model/task/:task/text/lang/:lang
	router.Get("/api/model/:model/task/:task/text", taskTextHandler, logRequest)
	router.Get("/api/model/:model/task/:task/text/lang/:lang", taskTextHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/task/:task/", http.NotFound)
	router.Get("/api/model/:model/task/:task/text/", http.NotFound)
	router.Get("/api/model/:model/task/:task/text/lang/", http.NotFound)

	// GET /api/model/:model/task/:task/text/all
	router.Get("/api/model/:model/task/:task/text/all", taskAllTextHandler, logRequest)
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
	router.Get("/api/model/:model/workset/:set/parameter/:name/value", worksetParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start", worksetParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start/count/:count", worksetParameterPageGetHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/workset/:set/parameter/:name/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/parameter/:name/value
	// GET /api/model/:model/run/:run/parameter/:name/value/start/:start
	// GET /api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count
	router.Get("/api/model/:model/run/:run/parameter/:name/value", runParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start", runParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count", runParameterPageGetHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/parameter/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/expr
	// GET /api/model/:model/run/:run/table/:name/expr/start/:start
	// GET /api/model/:model/run/:run/table/:name/expr/start/:start/count/:count
	router.Get("/api/model/:model/run/:run/table/:name/expr", runTableExprPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start", runTableExprPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start/count/:count", runTableExprPageGetHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/acc/start/:start
	// GET /api/model/:model/run/:run/table/:name/acc/start/:start/count/:count
	router.Get("/api/model/:model/run/:run/table/:name/acc", runTableAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start", runTableAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start/count/:count", runTableAccPageGetHandler, logRequest)
	// reject if request ill-formed
	// router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/all-acc
	// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start
	// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count
	router.Get("/api/model/:model/run/:run/table/:name/all-acc", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count", runTableAllAccPageGetHandler, logRequest)
	// reject if request ill-formed
	// router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start/count/", http.NotFound)
}

// add http GET web-service /api routes to read parameters or output tables as csv stream
func apiReadCsvRoutes(router *vestigo.Router) {

	// GET /api/model/:model/workset/:set/parameter/:name/csv
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv", worksetParameterCsvGetHandler, logRequest)

	// GET /api/model/:model/workset/:set/parameter/:name/csv-bom
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv-bom", worksetParameterCsvBomGetHandler, logRequest)

	// GET /api/model/:model/workset/:set/parameter/:name/csv-id
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv-id", worksetParameterIdCsvGetHandler, logRequest)

	// GET /api/model/:model/workset/:set/parameter/:name/csv-id-bom
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv-id-bom", worksetParameterIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv
	router.Get("/api/model/:model/run/:run/parameter/:name/csv", runParameterCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv-bom
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-bom", runParameterCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv-id
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-id", runParameterIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv-id-bom
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-id-bom", runParameterIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv", runTableExprCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv-bom
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-bom", runTableExprCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv-id
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-id", runTableExprIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/expr/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-id-bom", runTableExprIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv", runTableAccCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv-bom
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-bom", runTableAccCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv-id
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-id", runTableAccIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-id-bom", runTableAccIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv", runTableAllAccCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-bom
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-bom", runTableAllAccCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-id", runTableAllAccIdCsvGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-id-bom", runTableAllAccIdCsvBomGetHandler, logRequest)
}

// add web-service /api routes to update metadata
func apiUpdateRoutes(router *vestigo.Router) {

	//
	// update profile
	//

	// PATCH /api/model/:model/profile
	router.Patch("/api/model/:model/profile", profileReplaceHandler, logRequest)

	// DELETE /api/model/:model/profile/:profile
	router.Delete("/api/model/:model/profile/:profile", profileDeleteHandler, logRequest)

	// POST /api/model/:model/profile/:profile/key/:key/value/:value
	router.Post("/api/model/:model/profile/:profile/key/:key/value/:value", profileOptionReplaceHandler, logRequest)

	// DELETE /api/model/:model/profile/:profile/key/:key
	router.Delete("/api/model/:model/profile/:profile/key/:key", profileOptionDeleteHandler, logRequest)

	//
	// update model set of input parameters (workset)
	//

	// POST /api/model/:model/workset/:set/readonly/:readonly
	router.Post("/api/model/:model/workset/:set/readonly/:readonly", worksetReadonlyUpdateHandler, logRequest)

	// PUT  /api/workset-create
	router.Put("/api/workset-create", worksetCreateHandler, logRequest)

	// PUT  /api/workset-replace
	router.Put("/api/workset-replace", worksetReplaceHandler, logRequest)

	// PATCH /api/workset-merge
	router.Patch("/api/workset-merge", worksetMergeHandler, logRequest)

	// DELETE /api/model/:model/workset/:set
	router.Delete("/api/model/:model/workset/:set", worksetDeleteHandler, logRequest)

	// PATCH /api/model/:model/workset/:set/parameter/:name/new/value
	router.Patch("/api/model/:model/workset/:set/parameter/:name/new/value", parameterPageUpdateHandler, logRequest)

	// PATCH /api/model/:model/workset/:set/parameter/:name/new/value-id
	router.Patch("/api/model/:model/workset/:set/parameter/:name/new/value-id", parameterIdPageUpdateHandler, logRequest)

	// DELETE /api/model/:model/workset/:set/parameter/:name
	router.Delete("/api/model/:model/workset/:set/parameter/:name", worksetParameterDeleteHandler, logRequest)

	// PUT  /api/model/:model/workset/:set/copy/parameter/:name/from-run/:run
	router.Put("/api/model/:model/workset/:set/copy/parameter/:name/from-run/:run", worksetParameterRunCopyHandler, logRequest)

	// PATCH  /api/model/:model/workset/:set/merge/parameter/:name/from-run/:run
	router.Patch("/api/model/:model/workset/:set/merge/parameter/:name/from-run/:run", worksetParameterRunMergeHandler, logRequest)

	// PUT /api/model/:model/workset/:set/copy/parameter/:name/from-workset/:from-set
	router.Put("/api/model/:model/workset/:set/copy/parameter/:name/from-workset/:from-set", worksetParameterCopyFromWsHandler, logRequest)

	// PATCH /api/model/:model/workset/:set/merge/parameter/:name/from-workset/:from-set
	router.Patch("/api/model/:model/workset/:set/merge/parameter/:name/from-workset/:from-set", worksetParameterMergeFromWsHandler, logRequest)

	// PATCH /api/model/:model/workset/:set/parameter-text
	router.Patch("/api/model/:model/workset/:set/parameter-text", worksetParameterTextMergeHandler, logRequest)

	//
	// update model run
	//

	// PATCH /api/run/text
	router.Patch("/api/run/text", runTextMergeHandler, logRequest)

	// DELETE /api/model/:model/run/:run
	router.Delete("/api/model/:model/run/:run", runDeleteHandler, logRequest)

	// PATCH /api/model/:model/run/:run/parameter-text
	router.Patch("/api/model/:model/run/:run/parameter-text", runParameterTextMergeHandler, logRequest)

	//
	// update modeling task and task run history
	//

	// PUT  /api/task-new
	router.Put("/api/task-new", taskDefReplaceHandler, logRequest)

	// PATCH /api/task
	router.Patch("/api/task", taskDefMergeHandler, logRequest)

	// DELETE /api/model/:model/task/:task
	router.Delete("/api/model/:model/task/:task", taskDeleteHandler, logRequest)
}

// add web-service /api routes to run the model and monitor progress
func apiRunModelRoutes(router *vestigo.Router) {

	// POST /api/run
	router.Post("/api/run", runModelHandler, logRequest)

	// GET /api/run/log/model/:model/stamp/:stamp
	// GET /api/run/log/model/:model/stamp/:stamp/start/:start/count/:count
	router.Get("/api/run/log/model/:model/stamp/:stamp", runLogPageHandler, logRequest)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start", runLogPageHandler, logRequest)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start/count/:count", runLogPageHandler, logRequest)

	// PUT /api/run/stop/model/:model/stamp/:stamp
	router.Put("/api/run/stop/model/:model/stamp/:stamp", stopModelHandler, logRequest)

	// reject run log if request ill-formed
	router.Get("/api/run/log/model/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start/count/", http.NotFound)
	router.Put("/api/run/log/model/:model/stamp/", http.NotFound)
}

// add http web-service /api routes to download and manage files from home/io/download folder
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

	// reject if request ill-formed
	// POST /api/download/model/:model/run/
	// POST /api/download/model/:model/workset/
	router.Post("/api/download/model/", http.NotFound)
	router.Post("/api/download/model/run/", http.NotFound)
	router.Post("/api/download/model/workset/", http.NotFound)

	// DELETE /api/download/delete/:folder
	router.Delete("/api/download/delete/:folder", downloadDeleteHandler, logRequest)

	// DELETE /api/download/start/delete/:folder
	router.Delete("/api/download/start/delete/:folder", downloadAsyncDeleteHandler, logRequest)
}

// add http web-service /api routes to upload and manage files at home/io/upload folder
func apiUploadRoutes(router *vestigo.Router) {

	// GET /api/upload/log/all
	router.Get("/api/upload/log/all", allLogUploadGetHandler, logRequest)

	// GET /api/upload/log/model/:model
	router.Get("/api/upload/log/model/:model", modelLogUploadGetHandler, logRequest)

	// GET /api/upload/log/file/:name
	router.Get("/api/upload/log/file/:name", fileLogUploadGetHandler, logRequest)

	// GET /api/upload/file-tree/:folder
	router.Get("/api/upload/file-tree/:folder", fileTreeUploadGetHandler, logRequest)

	// POST /api/upload/model/:model/workset
	// POST /api/upload/model/:model/workset/:set
	router.Post("/api/upload/model/:model/workset", worksetUploadPostHandler, logRequest)
	router.Post("/api/upload/model/:model/workset/:set", worksetUploadPostHandler, logRequest)

	// POST /api/upload/model/:model/run
	// POST /api/upload/model/:model/run/:run
	router.Post("/api/upload/model/:model/run", runUploadPostHandler, logRequest)
	router.Post("/api/upload/model/:model/run/:run", runUploadPostHandler, logRequest)

	// reject if request ill-formed
	router.Post("/api/upload/model/", http.NotFound)
	router.Post("/api/upload/model/:model/workset/", http.NotFound)
	router.Post("/api/upload/model/:model/run/", http.NotFound)

	// DELETE /api/upload/delete/:folder
	router.Delete("/api/upload/delete/:folder", uploadDeleteHandler, logRequest)

	// DELETE /api/upload/start/delete/:folder
	router.Delete("/api/upload/start/delete/:folder", uploadAsyncDeleteHandler, logRequest)
}

// add http web-service /api routes to upload, download and manage files at home/io/files folder
func apiFilesRoutes(router *vestigo.Router) {

	// GET /api/files/list/:folder
	// router.Get("/api/files/list/:path", fileListGetHandler, logRequest)

	// GET /api/files/file/:path
	// router.Get("/api/files/file/:path", fileDownloadGetHandler, logRequest)

	// POST /api/files/file/:path
	// router.Post("/api/files/file/:path", fileUploadPostHandler, logRequest)

	// DELETE /api/files/file/:path
	// router.Delete("/api/files/file/:path", fileDeleteHandler, logRequest)

	// POST /api/files/folder/:path
	// router.Post("/api/files/folder/:path", folderCreatePostHandler, logRequest)

	// DELETE /api/files/folder/:path
	// router.Delete("/api/files/folder/:path", folderDeleteHandler, logRequest)
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

// add web-service /api routes service state
func apiServiceRoutes(router *vestigo.Router) {

	// GET /api/service/config
	router.Get("/api/service/config", serviceConfigHandler, logRequest)

	// GET /api/service/state
	router.Get("/api/service/state", serviceStateHandler, logRequest)

	// GET /api/service/job/active/:job
	// GET /api/service/job/queue/:job
	// GET /api/service/job/history/:job
	router.Get("/api/service/job/active/:job", jobActiveHandler, logRequest)
	router.Get("/api/service/job/queue/:job", jobQueueHandler, logRequest)
	router.Get("/api/service/job/history/:job", jobHistoryHandler, logRequest)

	// PUT /api/service/job/move/:pos/:job
	router.Put("/api/service/job/move/:pos/:job", jobMoveHandler, logRequest)

	// DELETE /api/service/job/delete/history/:job
	router.Delete("/api/service/job/delete/history/:job", jobHistoryDeleteHandler, logRequest)
}

// add web-service /api routes for administrative tasks
func apiAdminRoutes(router *vestigo.Router) {

	// POST /api/admin/all-models/refresh
	router.Post("/api/admin/all-models/refresh", allModelsRefreshHandler, logRequest)

	// POST /api/admin/all-models/close
	router.Post("/api/admin/all-models/close", allModelsCloseHandler, logRequest)

	// POST /api/admin/jobs-pause/:pause
	router.Post("/api/admin/jobs-pause/:pause", jobsPauseHandler, logRequest)

	// DO NOT USE in production, development only
	//
	// POST /api/admin/run-test/:exe/:arg
	// router.Post("/api/admin/run-test/:exe/:arg", runTestHandler, logRequest)
}
