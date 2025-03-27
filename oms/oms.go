// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
oms is openM++ JSON web-service which is also used as a simple web-server for openM++ UI html pages.

Web-service allows viewing and updating model database(s) and running openM++ models from models/bin subdirectory.
Web-server allows serving static html (css, images, javascript) content from the html subdirectory.

Arguments for oms can be specified on the command line or through a .ini file:

	oms -ini my-oms.ini
	oms -OpenM.IniFile my-oms.ini

Command-line arguments take precedence over ini-file options.
Some arguments can be specified only through an ini-file.
Please see the `oms.ini` file in the source code or the wiki for more details.

Following arguments are supported by oms:

	-l localhost:4040
	-oms.Listen localhost:4040
	Address to listen on, default: localhost:4040.
	Use -l :4040 if you need to access oms web-service from another computer
	(make sure the firewall is configured properly).

	-oms.UrlSaveTo some/dir/oms.url.txt
	File path to save the OMS URL (e.g., http://localhost:4040).
	If the path is relative, it is relative to the OMS root directory.
	By default, this is empty, so no file is created.

	-oms.PidSaveTo some/dir/oms.pid.txt
	File path to save the OMS process ID (PID).
	If the path is relative, it is relative to the OMS root directory.
	By default, this is empty, so no PID file is created.

	-oms.RootDir ompp/root
	The OMS root directory, default is the current directory.
	Recommended to have a log/ subdirectory to store OMS web-service log files.

	-oms.ModelDir models/bin
	The models executable and model.sqlite database files directory,
	default: models/bin (relative to the OMS root directory).

	-oms.ModelLogDir models/log
	Models log directory, default: models/log (relative to the OMS root directory).

	-oms.ModelDocDir models/doc
	Models documentation directory, default: models/doc (relative to the OMS root directory).

	-oms.HtmlDir html
	The front-end UI directory, default: html (relative to the OMS root directory).
	Not used if -oms.ApiOnly is specified.

	-oms.EtcDir etc
	Configuration files directory, default: etc (relative to the OMS root directory).
	Optional directory; may contain config files, e.g., templates for running models on an MPI cluster.

	-oms.JobDir job
	Jobs control directory.
	If this is specified, it is relative to the OMS root directory.
	It allows managing computational resources (CPUs) and organizes a model run queue.
	The default value is an empty string, which disables job control.

	-oms.Name someName
	An instance name used for job control. If empty, it is derived from the address to listen on.

	-oms.HomeDir models/home
	A user “home” directory to store files and settings (relative to the OMS root directory).
	Default is empty, which disables the use of a home directory.

	-oms.AllowDownload false
	If true, allows downloading from the user’s home/io/download directory.

	-oms.AllowUpload false
	If true, allows uploading to the user’s home/io/upload directory.

	-oms.FilesDir
	The user files directory, where a user can store, upload, and download ini/CSV files.
	If relative, it is relative to the OMS root directory.
	If a user home directory is specified, then this defaults to home/io.

	-oms.AllowMicrodata
	If true, model-run microdata usage is permitted. Otherwise, the microdata API is disabled.

	-oms.ApiOnly false
	If true, runs as an API-only web-service (no web UI).

	-oms.LogRequest false
	If true, logs every HTTP request to the console and/or log file.

	-oms.NoAdmin
	If true, disables local administrative routes: /admin/.

	-oms.NoShutdown
	If true, disables the shutdown route: /shutdown/.

	-oms.AdminAll
	If true, allows global administrative routes: /admin-all/.

	-oms.Languages en
	A comma-separated list of supported languages, default: en.
	Used to match request languages to model languages.

	-oms.DoubleFormat %.15g
	The format for converting float or double values to strings, default: %.15g.

	-oms.CodePage
	A “code page” for converting source files into UTF-8 (e.g., windows-1252).
	Used primarily for compatibility with older Windows files.

OpenM++ standard log settings (see openM++ wiki):

	-OpenM.LogToConsole If true, logs to standard output (default: true)
	-v Shorthand for -OpenM.LogToConsole
	-OpenM.LogToFile If true, logs to a file
	-OpenM.LogFilePath Path to the log file (default: current/dir/exeName.log)
	-OpenM.LogUseDailyStamp If true, uses a daily stamp in the log file name (rotates logs daily)
	-OpenM.LogUseTs If true, uses a timestamp in the log file name
	-OpenM.LogUsePid If true, uses a pid-stamp in the log file name
	-OpenM.LogSql If true, logs SQL statements to the log file
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
	urlFileArgKey      = "oms.UrlSaveTo"      // file path to save oms URL
	pidFileArgKey      = "oms.PidSaveTo"      // file path to save oms process ID
	rootDirArgKey      = "oms.RootDir"        // oms root directory
	modelDirArgKey     = "oms.ModelDir"       // models directory
	modelLogDirArgKey  = "oms.ModelLogDir"    // models log directory
	modelDocDirArgKey  = "oms.ModelDocDir"    // models doc directory
	etcDirArgKey       = "oms.EtcDir"         // config files directory
	htmlDirArgKey      = "oms.HtmlDir"        // front-end UI directory
	jobDirArgKey       = "oms.JobDir"         // job control directory
	homeDirArgKey      = "oms.HomeDir"        // user home directory
	isDownloadArgKey   = "oms.AllowDownload"  // if true then allow download
	isUploadArgKey     = "oms.AllowUpload"    // if true then allow upload
	filesDirArgKey     = "oms.FilesDir"       // user files directory
	isMicrodataArgKey  = "oms.AllowMicrodata" // if true then allow model run microdata
	logRequestArgKey   = "oms.LogRequest"     // if true then log http request
	apiOnlyArgKey      = "oms.ApiOnly"        // if true then API only
	adminAllArgKey     = "oms.AdminAll"       // if true then allow global administrative routes
	noAdminArgKey      = "oms.NoAdmin"        // if true then disable local admin routes
	noShutdownArgKey   = "oms.NoShutdown"     // if true then disable shutdown route
	uiLangsArgKey      = "oms.Languages"      // list of supported languages
	encodingArgKey     = "oms.CodePage"       // code page for converting
	doubleFormatArgKey = "oms.DoubleFormat"   // format to convert float/double
)

// server run configuration
var theCfg = struct {
	rootDir      string            // server root directory
	htmlDir      string            // front-end UI directory
	etcDir       string            // configuration files directory
	isHome       bool              // if true then single user mode
	homeDir      string            // user home directory
	downloadDir  string            // home/io/download directory
	uploadDir    string            // home/io/upload directory
	inOutDir     string            // home/io directory (if available)
	filesDir     string            // user files directory
	isMicrodata  bool              // if true then allow model-run microdata
	docDir       string            // models documentation directory
	isJobControl bool              // if true then do job control
	isJobPast    bool              // if true then job history shadow copy
	isDiskUse    bool              // if true then disk usage control
	jobDir       string            // job control directory
	omsName      string            // instance name
	dbcopyPath   string            // path to dbcopy.exe (if any)
	doubleFmt    string            // float/double format
	codePage     string            // code page for reading model text
	env          map[string]string // server config environment
	uiExtra      string            // UI extra config from etc/ui.extra.json
}{
	htmlDir:      "html",
	etcDir:       "etc",
	isHome:       false,
	homeDir:      "",
	downloadDir:  "",
	uploadDir:    "",
	filesDir:     "",
	docDir:       "",
	isJobControl: false,
	jobDir:       "",
	omsName:      "",
	doubleFmt:    "%.15g",
	env:          map[string]string{},
}

// if true then log http requests
var isLogRequest bool

// matcher to find UI supported language
var uiLangMatcher language.Matcher

var refreshDiskScanC chan bool

func main() {
	defer exitOnPanic() // fatal error handler: log and exit

	err := mainBody(os.Args)
	if err != nil {
		omppLog.Log(err.Error())
		os.Exit(1)
	}
	omppLog.Log("Done.") // completed OK
}

// actual main body
func mainBody(args []string) error {

	// set command line argument keys and ini-file keys
	_ = flag.String(listenArgKey, "localhost:4040", "address to listen")
	_ = flag.String(listenShortKey, "localhost:4040", "address to listen (short form of "+listenArgKey+")")
	_ = flag.String(urlFileArgKey, "", "file path to save oms URL")
	_ = flag.String(rootDirArgKey, "", "root directory")
	_ = flag.String(modelDirArgKey, "models/bin", "models directory")
	_ = flag.String(modelLogDirArgKey, "models/log", "models log directory")
	_ = flag.String(modelDocDirArgKey, "models/doc", "models documentation directory")
	_ = flag.String(etcDirArgKey, theCfg.etcDir, "configuration files directory")
	_ = flag.String(htmlDirArgKey, theCfg.htmlDir, "front-end UI directory")
	_ = flag.String(homeDirArgKey, "", "user personal home directory")
	_ = flag.Bool(isDownloadArgKey, false, "if true then allow download from user home/io/download")
	_ = flag.Bool(isUploadArgKey, false, "if true then allow upload to user home/io/upload")
	_ = flag.String(filesDirArgKey, "", "user files directory")
	_ = flag.Bool(isMicrodataArgKey, false, "if true then allow model run microdata")
	_ = flag.String(jobDirArgKey, "", "job control directory")
	_ = flag.String(omsNameArgKey, "", "instance name")
	_ = flag.Bool(logRequestArgKey, false, "if true then log HTTP requests")
	_ = flag.Bool(apiOnlyArgKey, false, "if true then API only web-service")
	_ = flag.Bool(adminAllArgKey, false, "if true then allow global administrative routes: /admin-all/")
	_ = flag.Bool(noAdminArgKey, false, "if true then disable local administrative routes: /admin/")
	_ = flag.Bool(noShutdownArgKey, false, "if true then disable shutdown route: /shutdown/")
	_ = flag.String(uiLangsArgKey, "en", "comma-separated list of supported languages")
	_ = flag.String(encodingArgKey, "", "code page to convert source files into utf-8")
	_ = flag.String(doubleFormatArgKey, theCfg.doubleFmt, "format to convert float or double value")
	_ = flag.String(pidFileArgKey, "", "file path to save OMS process ID")

	// pairs of full and short argument names
	optFs := []config.FullShort{
		{Full: listenArgKey, Short: listenShortKey},
	}

	// parse command line arguments and ini-file
	runOpts, logOpts, err := config.New(encodingArgKey, false, optFs)
	if err != nil {
		return errors.New("Invalid arguments: " + err.Error())
	}
	isLogRequest = runOpts.Bool(logRequestArgKey)
	isApiOnly := runOpts.Bool(apiOnlyArgKey)
	theCfg.isMicrodata = runOpts.Bool(isMicrodataArgKey)
	isAdminAll := runOpts.Bool(adminAllArgKey)
	isAdmin := !runOpts.Bool(noAdminArgKey)
	isShutdown := !runOpts.Bool(noShutdownArgKey)
	theCfg.doubleFmt = runOpts.String(doubleFormatArgKey)
	theCfg.codePage = runOpts.String(encodingArgKey)

	// gather OM_CFG_* environment variables
	envVars := os.Environ()
	for _, e := range envVars {
		if strings.HasPrefix(e, "OM_CFG_") {
			if kv := strings.SplitN(e, "=", 2); len(kv) == 2 {
				theCfg.env[kv[0]] = kv[1]
			}
		}
	}

	// set UI languages
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

	theCfg.rootDir = filepath.Clean(runOpts.String(rootDirArgKey)) // server root directory

	if theCfg.rootDir != "" && theCfg.rootDir != "." {
		if err := os.Chdir(theCfg.rootDir); err != nil {
			return errors.New("Error: unable to change directory: " + err.Error())
		}
	}
	omppLog.New(logOpts) // set up logging

	if theCfg.rootDir != "" && theCfg.rootDir != "." {
		omppLog.Log("Change directory to: ", theCfg.rootDir)
	}

	// model directory
	modelDir := filepath.Clean(runOpts.String(modelDirArgKey))
	if modelDir == "" || modelDir == "." {
		return errors.New("Error: model directory argument cannot be empty or .")
	}
	omppLog.Log("Models directory: ", modelDir)

	modelLogDir := filepath.Clean(runOpts.String(modelLogDirArgKey))
	modelLogDir = strings.TrimSuffix(modelLogDir, string(filepath.Separator))
	if modelLogDir != "" && modelLogDir != "." && dirExist(modelLogDir) {
		omppLog.Log("Models log directory: ", modelLogDir)
	} else {
		modelLogDir = ""
	}

	if err := theCatalog.refreshSqlite(modelDir, modelLogDir); err != nil {
		return err
	}

	// model docs
	theCfg.docDir = filepath.Clean(runOpts.String(modelDocDirArgKey))
	theCfg.docDir = strings.TrimSuffix(theCfg.docDir, string(filepath.Separator))
	if theCfg.docDir == "." || !dirExist(theCfg.docDir) {
		omppLog.Log("Warning: model documentation directory not found or invalid: ", theCfg.docDir)
		theCfg.docDir = ""
	} else {
		omppLog.Log("Models documentation: ", theCfg.docDir)
	}

	// home directory if single user mode
	if runOpts.IsExist(homeDirArgKey) {
		theCfg.homeDir = filepath.Clean(runOpts.String(homeDirArgKey))
		theCfg.homeDir = strings.TrimSuffix(theCfg.homeDir, string(filepath.Separator))

		if theCfg.homeDir == "." || !dirExist(theCfg.homeDir) {
			omppLog.Log("Warning: user home directory not found or invalid: ", theCfg.homeDir)
			theCfg.homeDir = ""
		}
		theCfg.isHome = theCfg.homeDir != ""
		if theCfg.isHome {
			omppLog.Log("User Home directory: ", theCfg.homeDir)

			theCfg.inOutDir = filepath.Join(theCfg.homeDir, "io")
			if theCfg.inOutDir == "." || !dirExist(theCfg.inOutDir) {
				theCfg.inOutDir = ""
			}
		}
	}

	// allow download/upload if possible
	isDownload := false
	isUpload := false

	theCfg.dbcopyPath = dbcopyPath(omsAbsPath)
	if runOpts.Bool(isDownloadArgKey) {
		if theCfg.inOutDir != "" && theCfg.dbcopyPath != "" {
			theCfg.downloadDir = filepath.Join(theCfg.inOutDir, "download")
			if !dirExist(theCfg.downloadDir) {
				theCfg.downloadDir = ""
			}
		}
		isDownload = theCfg.downloadDir != ""
		if !isDownload {
			theCfg.downloadDir = ""
			omppLog.Log("Warning: download disabled (missing directory or dbcopy)")
		} else {
			omppLog.Log("Download directory: ", theCfg.downloadDir)
		}
	}
	if runOpts.Bool(isUploadArgKey) {
		if theCfg.inOutDir != "" && theCfg.dbcopyPath != "" {
			theCfg.uploadDir = filepath.Join(theCfg.inOutDir, "upload")
			if !dirExist(theCfg.uploadDir) {
				theCfg.uploadDir = ""
			}
		}
		isUpload = theCfg.uploadDir != ""
		if !isUpload {
			theCfg.uploadDir = ""
			omppLog.Log("Warning: upload disabled (missing directory or dbcopy)")
		} else {
			omppLog.Log("Upload directory: ", theCfg.uploadDir)
		}
	}

	// user files directory
	if runOpts.IsExist(filesDirArgKey) {
		theCfg.filesDir = filepath.Clean(runOpts.String(filesDirArgKey))
		theCfg.filesDir = strings.TrimSuffix(theCfg.filesDir, string(filepath.Separator))
		if theCfg.filesDir == "." || !dirExist(theCfg.filesDir) {
			omppLog.Log("Warning: user files directory not found or invalid: ", theCfg.filesDir)
			theCfg.filesDir = ""
		}
	} else {
		if theCfg.inOutDir != "" && (isDownload || isUpload) {
			theCfg.filesDir = theCfg.inOutDir
		}
	}
	if theCfg.filesDir != "" {
		omppLog.Log("User Files directory: ", theCfg.filesDir)
	}

	// if UI required then check html subdir
	if !isApiOnly {
		theCfg.htmlDir = runOpts.String(htmlDirArgKey)
		if !dirExist(theCfg.htmlDir) {
			isApiOnly = true
			omppLog.Log("Warning: serving API only because UI directory not found: ", theCfg.htmlDir)
		} else {
			omppLog.Log("HTML UI directory: ", theCfg.htmlDir)
		}
	}

	// etc directory
	theCfg.etcDir = runOpts.String(etcDirArgKey)
	if !dirExist(theCfg.etcDir) {
		omppLog.Log("Warning: configuration files directory not found: ", theCfg.etcDir)
	} else {
		omppLog.Log("Etc directory: ", theCfg.etcDir)
	}

	// read UI extra config (etc/ui.extra.json)
	if bt, err := os.ReadFile(filepath.Join(theCfg.etcDir, "ui.extra.json")); err == nil {
		theCfg.uiExtra = string(bt)
	}

	// check disk usage control
	dini := filepath.Join(theCfg.etcDir, "disk.ini")
	theCfg.isDiskUse = fileExist(dini)
	if theCfg.isDiskUse {
		omppLog.Log("Storage control: ", dini)
	}

	// job control
	theCfg.jobDir = runOpts.String(jobDirArgKey)
	theCfg.isJobControl, theCfg.isJobPast, err = jobDirValid(theCfg.jobDir)
	if err != nil {
		return errors.New("Error: invalid job control directory: " + err.Error())
	}
	if !theCfg.isJobControl && theCfg.jobDir != "" {
		return errors.New("Error: invalid job control directory: " + theCfg.jobDir)
	}
	if theCfg.isJobControl {
		omppLog.Log("Jobs directory: ", theCfg.jobDir)
	}

	// instance name
	theCfg.omsName = runOpts.String(omsNameArgKey)
	if theCfg.omsName == "" {
		theCfg.omsName = runOpts.String(listenArgKey)
	}
	theCfg.omsName = helper.CleanFileName(theCfg.omsName)
	omppLog.Log("Oms instance name: ", theCfg.omsName)

	// refresh model run state
	jsc, _ := jobStateRead()
	if err := theRunCatalog.refreshCatalog(theCfg.etcDir, jsc); err != nil {
		return err
	}

	// background watchers
	doneModelRunScanC := make(chan bool)
	go scanModelRuns(doneModelRunScanC)

	doneOuterJobScanC := make(chan bool)
	go scanOuterJobs(doneOuterJobScanC)

	doneStateJobScanC := make(chan bool)
	go scanStateJobs(doneStateJobScanC)

	doneRunJobScanC := make(chan bool)
	go scanRunJobs(doneRunJobScanC)

	doneDiskScanC := make(chan bool)
	refreshDiskScanC = make(chan bool)
	go scanDisk(doneDiskScanC, refreshDiskScanC)

	// set up router
	router := vestigo.NewRouter()
	router.SetGlobalCors(&vestigo.CorsAccessControl{
		AllowOrigin:      []string{"*"},
		AllowCredentials: true,
		AllowHeaders:     []string{"Content-Type"},
		ExposeHeaders:    []string{"Content-Type", "Content-Location"},
	})

	apiGetRoutes(router)
	apiReadRoutes(router)
	apiReadCsvRoutes(router)

	if isDownload {
		apiDownloadRoutes(router)
	}
	if isUpload {
		apiUploadRoutes(router)
	}
	if theCfg.filesDir != "" && (isDownload || theCfg.filesDir != theCfg.inOutDir) {
		router.Get("/files/*", filesHandler, logRequest)
		apiFilesRoutes(router)
	}
	apiUpdateRoutes(router)
	apiRunModelRoutes(router)
	apiUserRoutes(router)
	apiServiceRoutes(router)

	if isAdmin {
		apiAdminRoutes(isAdminAll, router)
	}

	// serve static content
	if isDownload {
		router.Get("/download/*", downloadHandler, logRequest)
	}
	if isUpload {
		router.Get("/upload/*", downloadHandler, logRequest)
	}
	if theCfg.docDir != "" {
		router.Get("/doc/*", modelDocHandler, logRequest)
	}

	// set web root handler or 404 if API only
	if !isApiOnly {
		router.Get("/*", homeHandler, logRequest)
	} else {
		router.Get("/*", http.NotFound)
	}

	// initialize server
	addr := runOpts.String(listenArgKey)
	srv := http.Server{Addr: addr, Handler: router}

	// PUT /shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdownHandler := func(w http.ResponseWriter, r *http.Request) {
		// close catalog
		omppLog.Log("Shutdown server...")
		if err := theCatalog.closeAll(); err != nil {
			omppLog.Log(err)
		}

		srv.SetKeepAlivesEnabled(false)
		w.Write([]byte("Shutdown completed"))

		cancel() // send shutdown completed to the main
	}
	if isShutdown {
		router.Put("/shutdown", shutdownHandler, logRequest)
	}

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
			omppLog.Log("Error writing URL file: ", urlFile, " error: ", err)
			return err
		}
	}

	if pidFile := runOpts.String(pidFileArgKey); pidFile != "" {
		pid := os.Getpid()
		if err = os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0644); err != nil {
			omppLog.Log("Error writing PID to file: ", pidFile, " error: ", err)
			return err
		}
		omppLog.Log("PID written to file: ", pidFile, " Value: ", pid)
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

// exitOnPanic logs error message and exits with return = 2
func exitOnPanic() {
	r := recover()
	if r == nil {
		return
	}
	switch e := r.(type) {
	case error:
		omppLog.Log(e.Error())
	case string:
		omppLog.Log(e)
	default:
		omppLog.Log("FAILED")
	}
	os.Exit(2)
}
