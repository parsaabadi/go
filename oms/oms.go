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
	There are some arguments which can be specified through ini-file and not on command line,
	please see oms.ini file in our source code or our wiki for more details.

Following arguments supporetd by oms:

-l localhost:4040
-oms.Listen localhost:4040

	address to listen, default: localhost:4040.
	Use -l :4040 if you need to access oms web-service from other computer (make sure firewall configured properly).

-oms.UrlSaveTo someModel.ui.url.txt

	file path to save oms URL which can be used to open web UI in browser.
	Default: empty value, URL is not saved in a file by default, example of URL file content: http://localhost:4040

-oms.RootDir om/root

	oms root directory, default: current directory.
	Recommended to have log/ subdirectory to store oms web-service log files.

-oms.ModelDir models/bin

	models executable and model.sqlite database files directory, default: models/bin,
	If relative then must be relative to oms root directory.

-oms.ModelLogDir models/log

	models log directory, default: models/log, if relative then must be relative to oms root directory.

-oms.ModelDocDir models/doc

	models documentation directory, default: models/doc, if relative then must be relative to oms root directory
	UI expect it ends /doc subdirectory, for example: C:\any\dir\doc

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

-oms.AllowMicrodata

	if true then allow model runs microdata usage else model microdata API disabled.

-oms.ApiOnly false

	if true then API only web-service, it is false by default and oms also act as http server for openM++ UI.

-oms.LogRequest false

	if true then log HTTP requests on console and/or log file.

-oms.Admin

	if true then allow global administrative routes: /admin-all/

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
	listenArgKey       = "oms.Listen"         // address to listen, default: localhost:4040
	listenShortKey     = "l"                  // address to listen (short form)
	omsNameArgKey      = "oms.Name"           // oms instance name, if empty then derived from address to listen
	urlFileArgKey      = "oms.UrlSaveTo"      // file path to save oms URL in form of: http://localhost:4040, if relative then must be relative to oms root directory
	rootDirArgKey      = "oms.RootDir"        // oms root directory, expected to contain log subfolder
	modelDirArgKey     = "oms.ModelDir"       // models executable and model.sqlite directory, if relative then must be relative to oms root directory
	modelLogDirArgKey  = "oms.ModelLogDir"    // models log directory, if relative then must be relative to oms root directory
	modelDocDirArgKey  = "oms.ModelDocDir"    // models documentation directory, if relative then must be relative to oms root directory
	etcDirArgKey       = "oms.EtcDir"         // configuration files directory, if relative then must be relative to oms root directory
	htmlDirArgKey      = "oms.HtmlDir"        // front-end UI directory, if relative then must be relative to oms root directory
	jobDirArgKey       = "oms.JobDir"         // job control directory, if relative then must be relative to oms root directory
	homeDirArgKey      = "oms.HomeDir"        // user personal home directory, if relative then must be relative to oms root directory
	isDownloadArgKey   = "oms.AllowDownload"  // if true then allow download from user home sub-directory: home/io/download
	isUploadArgKey     = "oms.AllowUpload"    // if true then allow upload to user home sub-directory: home/io/upload
	isMicrodataArgKey  = "oms.AllowMicrodata" // if true then allow model run microdata
	logRequestArgKey   = "oms.LogRequest"     // if true then log http request
	apiOnlyArgKey      = "oms.ApiOnly"        // if true then API only web-service, no web UI
	adminAllArgKey     = "oms.Admin"          // if true then allow global administrative routes: /admin-all/
	uiLangsArgKey      = "oms.Languages"      // list of supported languages
	encodingArgKey     = "oms.CodePage"       // code page for converting source files, e.g. windows-1252
	doubleFormatArgKey = "oms.DoubleFormat"   // format to convert float or double value to string, e.g. %.15g
)

// server run configuration
var theCfg = struct {
	rootDir      string            // server root directory
	htmlDir      string            // front-end UI directory with html and javascript
	etcDir       string            // configuration files directory
	isHome       bool              // if true then it is a single user mode
	homeDir      string            // user home directory
	downloadDir  string            // if download allowed then it is home/io/download directory
	uploadDir    string            // if upload allowed then it is home/io/upload directory
	inOutDir     string            // if download or upload allowed then it is home/io directory
	isMicrodata  bool              // if true then allow model run microdata
	isModelDoc   bool              // if true then model documentation enabled and models/doc is exist
	docParentDir string            // parent of models documentation directory, default: models
	isAdminAll   bool              // if true then admin-all routes are enabled
	isJobControl bool              // if true then do job control: model run queue and resource allocation
	isJobPast    bool              // if true then do job history shadow copy
	isDiskUse    bool              // if true then storage usage control enabled
	jobDir       string            // job control directory
	omsName      string            // oms instance name, if empty then derived from address to listen
	dbcopyPath   string            // if download or upload allowed then it is path to dbcopy.exe
	doubleFmt    string            // format to convert float or double value to string
	codePage     string            // "code page" to convert source file into utf-8, for example: windows-1252
	env          map[string]string // server config environmemt variables to control UI
}{
	htmlDir:      "html",
	etcDir:       "etc",
	isHome:       false,
	homeDir:      "",
	downloadDir:  "",
	uploadDir:    "",
	isModelDoc:   false,
	isJobControl: false,
	jobDir:       "",
	omsName:      "",
	doubleFmt:    "%.15g",
	env:          map[string]string{},
}

// if true then log http requests
var isLogRequest bool

// matcher to find UI supported language corresponding to request
var uiLangMatcher language.Matcher

var refreshDiskScanC chan bool

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
	_ = flag.String(listenArgKey, "localhost:4040", "address to listen")
	_ = flag.String(listenShortKey, "localhost:4040", "address to listen (short form of "+listenArgKey+")")
	_ = flag.String(urlFileArgKey, "", "file path to save oms URL, if relative then must be relative to root directory")
	_ = flag.String(rootDirArgKey, "", "root directory, default: current directory")
	_ = flag.String(modelDirArgKey, "models/bin", "models directory, if relative then must be relative to root directory")
	_ = flag.String(modelLogDirArgKey, "models/log", "models log directory, if relative then must be relative to root directory")
	_ = flag.String(modelDocDirArgKey, "models/doc", "models documentation directory, if relative then must be relative to root directory")
	_ = flag.String(etcDirArgKey, theCfg.etcDir, "configuration files directory, if relative then must be relative to oms root directory")
	_ = flag.String(htmlDirArgKey, theCfg.htmlDir, "front-end UI directory, if relative then must be relative to root directory")
	_ = flag.String(homeDirArgKey, "", "user personal home directory, if relative then must be relative to root directory")
	_ = flag.Bool(isDownloadArgKey, false, "if true then allow download from user home/io/download directory")
	_ = flag.Bool(isUploadArgKey, false, "if true then allow upload to user home/io/upload directory")
	_ = flag.Bool(isMicrodataArgKey, false, "if true then allow model run microdata")
	_ = flag.String(jobDirArgKey, "", "job control directory, if relative then must be relative to root directory")
	_ = flag.String(omsNameArgKey, "", "instance name, automatically generated if empty")
	_ = flag.Bool(logRequestArgKey, false, "if true then log HTTP requests")
	_ = flag.Bool(apiOnlyArgKey, false, "if true then API only web-service, no web UI")
	_ = flag.Bool(adminAllArgKey, false, "if true then allow global administrative routes: /admin-all/")
	_ = flag.String(uiLangsArgKey, "en", "comma-separated list of supported languages")
	_ = flag.String(encodingArgKey, "", "code page to convert source file into utf-8, e.g.: windows-1252")
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
	theCfg.isMicrodata = runOpts.Bool(isMicrodataArgKey)
	theCfg.isAdminAll = runOpts.Bool(adminAllArgKey)

	theCfg.doubleFmt = runOpts.String(doubleFormatArgKey)

	theCfg.codePage = runOpts.String(encodingArgKey)

	// get server config environmemt variables and pass it to UI
	env := os.Environ()
	for _, e := range env {
		if strings.HasPrefix(e, "OM_CFG_") {
			if kv := strings.SplitN(e, "=", 2); len(kv) == 2 {
				theCfg.env[kv[0]] = kv[1]
			}
		}
	}

	// set UI languages to find model text in browser language
	ll := helper.ParseCsvLine(runOpts.String(uiLangsArgKey), ',')
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

	// change to root directory
	omsAbsPath, err := filepath.Abs(args[0])
	if err != nil {
		return errors.New("Error: unable to make absolute path to oms: " + err.Error())
	}

	theCfg.rootDir = runOpts.String(rootDirArgKey) // server root directory

	if theCfg.rootDir != "" && theCfg.rootDir != "." {
		if err := os.Chdir(theCfg.rootDir); err != nil {
			return errors.New("Error: unable to change directory: " + err.Error())
		}
	}
	omppLog.New(logOpts) // adjust log options, log path can be relative to root directory

	if theCfg.rootDir != "" && theCfg.rootDir != "." {
		omppLog.Log("Change directory to:  ", theCfg.rootDir)
	}

	// model directory required to build initial list of model sqlite files
	modelLogDir := runOpts.String(modelLogDirArgKey)
	modelDir := filepath.Clean(runOpts.String(modelDirArgKey))
	if modelDir == "" || modelDir == "." {
		return errors.New("Error: model directory argument cannot be empty or . dot")
	}
	omppLog.Log("Models directory:     ", modelDir)

	if err := theCatalog.refreshSqlite(modelDir, modelLogDir); err != nil {
		return err
	}

	// check if model documentation directory exists
	docDir := filepath.Clean(runOpts.String(modelDocDirArgKey))
	docDir = strings.TrimSuffix(docDir, string(filepath.Separator))

	theCfg.docParentDir = filepath.Dir(docDir)
	theCfg.isModelDoc = dirExist(docDir) && dirExist(theCfg.docParentDir)

	if theCfg.isModelDoc {

		omppLog.Log("Models documentation: ", docDir)

		if filepath.Base(docDir) != "doc" {
			omppLog.Log("Warning: UI expect model documentation directory ends with 'doc', for example: /any/dir/doc")
		}
	}

	// check if it is single user run mode and use of home directory enabled
	if theCfg.homeDir = runOpts.String(homeDirArgKey); theCfg.homeDir != "" {
		if !dirExist(theCfg.homeDir) {
			omppLog.Log("Warning: user home directory not found: ", theCfg.homeDir)
			theCfg.homeDir = ""
		}
		theCfg.isHome = theCfg.homeDir != ""
		if theCfg.isHome {
			omppLog.Log("User directory:       ", theCfg.homeDir)
		}
	}

	// check download and upload options:
	// home/io/download or home/io/upload directory must exist and dbcopy.exe must exist
	isInOut := runOpts.Bool(isDownloadArgKey) || runOpts.Bool(isUploadArgKey)
	isDownload := false
	isUpload := false

	theCfg.dbcopyPath = dbcopyPath(omsAbsPath)
	isDbCopy := theCfg.dbcopyPath != ""

	if isInOut {
		if theCfg.homeDir != "" {
			theCfg.inOutDir = filepath.Join(theCfg.homeDir, "io") // download and upload directory for web-server, to serve static content
		}
		isInOut = theCfg.inOutDir != "" && isDbCopy
		if !isInOut {
			theCfg.inOutDir = ""
			theCfg.dbcopyPath = ""
		}
	}
	if runOpts.Bool(isDownloadArgKey) {
		if isInOut && theCfg.inOutDir != "" {

			theCfg.downloadDir = filepath.Join(theCfg.inOutDir, "download") // download directory UI

			if !dirExist(theCfg.downloadDir) {
				theCfg.downloadDir = ""
			}
		}
		isDownload = isInOut && theCfg.downloadDir != ""
		if !isDownload {
			theCfg.downloadDir = ""
			omppLog.Log("Warning: user home download directory not found or dbcopy not found, download disabled")
		} else {
			omppLog.Log("Download directory:   ", theCfg.downloadDir)
		}
	}
	if runOpts.Bool(isUploadArgKey) {
		if isInOut && theCfg.inOutDir != "" {

			theCfg.uploadDir = filepath.Join(theCfg.inOutDir, "upload") // upload directory UI

			if !dirExist(theCfg.uploadDir) {
				theCfg.uploadDir = ""
			}
		}
		isUpload = isInOut && theCfg.uploadDir != ""
		if !isUpload {
			theCfg.uploadDir = ""
			omppLog.Log("Warning: user home upload directory not found or dbcopy not found, upload disabled")
		} else {
			omppLog.Log("Upload directory:     ", theCfg.uploadDir)
		}
	}

	// if UI required then server root directory must have html subdir
	if !isApiOnly {
		theCfg.htmlDir = runOpts.String(htmlDirArgKey)
		if !dirExist(theCfg.htmlDir) {
			isApiOnly = true
			omppLog.Log("Warning: serving API only because UI directory not found: ", theCfg.htmlDir)
		} else {
			omppLog.Log("HTML UI directory:    ", theCfg.htmlDir)
		}
	}

	// check if job control is required:
	theCfg.jobDir = runOpts.String(jobDirArgKey)
	theCfg.isJobControl, theCfg.isJobPast, theCfg.isDiskUse, err = jobDirValid(theCfg.jobDir)
	if err != nil {
		return errors.New("Error: invalid job control directory" + ": " + err.Error())
	}
	if !theCfg.isJobControl && theCfg.jobDir != "" {
		return errors.New("Error: invalid job control directory" + ": " + theCfg.jobDir)
	}
	if theCfg.isJobControl {
		omppLog.Log("Jobs directory:       ", theCfg.jobDir)
	}

	// etc subdirectory required to run MPI models
	theCfg.etcDir = runOpts.String(etcDirArgKey)
	if !dirExist(theCfg.etcDir) {
		omppLog.Log("Warning: configuration files directory not found, it is required to run models on MPI cluster: ", filepath.Join(theCfg.etcDir))
	} else {
		omppLog.Log("Etc directory:        ", theCfg.etcDir)
	}

	// make instance name, use address to listen if name not specified
	theCfg.omsName = runOpts.String(omsNameArgKey)
	if theCfg.omsName == "" {
		theCfg.omsName = runOpts.String(listenArgKey)
	}
	theCfg.omsName = helper.CleanPath(theCfg.omsName)
	omppLog.Log("Oms instance name:    ", theCfg.omsName)

	// refresh run state catalog and start scanning model log files
	jsc, _ := jobStateRead()
	if err := theRunCatalog.refreshCatalog(theCfg.etcDir, jsc); err != nil {
		return err
	}

	doneModelRunScanC := make(chan bool)
	go scanModelRuns(doneModelRunScanC)

	// start scanning for model run jobs
	doneOuterJobScanC := make(chan bool)
	go scanOuterJobs(doneOuterJobScanC)

	doneStateJobScanC := make(chan bool)
	go scanStateJobs(doneStateJobScanC)

	doneRunJobScanC := make(chan bool)
	go scanRunJobs(doneRunJobScanC)

	doneDiskScanC := make(chan bool)
	refreshDiskScanC = make(chan bool)
	go scanDisk(doneDiskScanC, refreshDiskScanC)

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
		apiDownloadRoutes(router) // web-service /api routes to download and manage files at home/io/download folder
	}
	if isUpload {
		apiUploadRoutes(router) // web-service /api routes to upload and manage files at home/io/upload folder
	}
	apiUpdateRoutes(router)   // web-service /api routes to update metadata
	apiRunModelRoutes(router) // web-service /api routes to run the model
	apiUserRoutes(router)     // web-service /api routes for user-specific requests
	apiServiceRoutes(router)  // web-service /api routes for service state
	apiAdminRoutes(router)    // web-service /api routes for oms instance administrative tasks
	adminAllRoutes(router)    // web-service /admin-all routes for global administrative tasks

	// serve static content from home/io/download folder
	if isDownload {
		router.Get("/download/*", downloadHandler, logRequest)
	}
	// serve static content from home/io/upload folder
	if isUpload {
		router.Get("/upload/*", downloadHandler, logRequest)
	}
	// serve static content from models/doc folder
	if theCfg.isModelDoc {
		router.Get("/doc/*", modelDocHandler, logRequest)
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
	// PUT /shutdown
	ctx, cancel := context.WithCancel((context.Background()))
	defer cancel()

	shutdownHandler := func(w http.ResponseWriter, r *http.Request) {

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
	router.Put("/shutdown", shutdownHandler, logRequest)

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

	doneDiskScanC <- true
	doneRunJobScanC <- true
	doneStateJobScanC <- true
	doneOuterJobScanC <- true
	doneModelRunScanC <- true
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
