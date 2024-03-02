// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"bufio"
	"io"
	"net/http"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openmpp/go/ompp/omppLog"
)

// reload models catalog: rescan models directory tree and reload model.sqlite.
//
//	POST /api/admin/all-models/refresh
func allModelsRefreshHandler(w http.ResponseWriter, r *http.Request) {

	// model directory required to build list of model sqlite files
	modelLogDir, _ := theCatalog.getModelLogDir()
	modelDir, _ := theCatalog.getModelDir()
	if modelDir == "" {
		omppLog.Log("Failed to refersh models catalog: path to model directory cannot be empty")
		http.Error(w, "Failed to refersh models catalog: path to model directory cannot be empty", http.StatusBadRequest)
		return
	}
	omppLog.Log("Model directory: ", modelDir)

	// refresh models catalog
	if err := theCatalog.refreshSqlite(modelDir, modelLogDir); err != nil {
		omppLog.Log(err)
		http.Error(w, "Failed to refersh models catalog: "+modelDir, http.StatusBadRequest)
		return
	}

	// refresh run state catalog
	if err := theRunCatalog.refreshCatalog(theCfg.etcDir, nil); err != nil {
		omppLog.Log(err)
		http.Error(w, "Failed to refersh model runs catalog", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Location", "/api/admin/all-models/refresh/"+modelDir)
	w.Header().Set("Content-Type", "text/plain")
}

// clean models catalog: close all model.sqlite connections and clean models catalog
//
//	POST /api/admin/all-models/close
func allModelsCloseHandler(w http.ResponseWriter, r *http.Request) {

	// close models catalog
	modelDir, _ := theCatalog.getModelDir()

	if err := theCatalog.closeAll(); err != nil {
		omppLog.Log(err)
		http.Error(w, "Failed to close models catalog: "+modelDir, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Location", "/api/admin/all-models/close/"+modelDir)
	w.Header().Set("Content-Type", "text/plain")
}

// close model.sqlite connection and clean model from catalog
//
//	POST /api/admin/model/:model/close
//
// Model identified by digest-or-name.
// If multiple models with same name exist then result is undefined.
func modelCloseHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")

	if dn == "" {
		omppLog.Log("Error: invalid (empty) model digest and name")
		http.Error(w, "Invalid (empty) model digest and name", http.StatusBadRequest)
		return
	}

	// close model and remove from catalog
	if err := theCatalog.closeModel(dn); err != nil {
		omppLog.Log(err)
		http.Error(w, "Failed to close model"+": "+dn, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Location", "/api/admin/model/"+dn+"/close")
	w.Header().Set("Content-Type", "text/plain")
}

// pause or resume jobs queue processing by this oms instance
//
//	POST /api/admin/jobs-pause/:pause
func jobsPauseHandler(w http.ResponseWriter, r *http.Request) {
	doJobsPause(jobQueuePausedPath(theCfg.omsName), "/api/admin/jobs-pause/", w, r)
}

// pause or resume jobs queue processing by all oms instances
//
//	POST /admin-all/jobs-pause/:pause
func jobsAllPauseHandler(w http.ResponseWriter, r *http.Request) {
	doJobsPause(jobAllQueuePausedPath(), "/admin-all/jobs-pause/", w, r)
}

// Pause or resume jobs queue processing by this oms instance all by all oms instances
//
//	POST /admin/jobs-pause/:pause
//	POST /admin-all/jobs-pause/:pause
func doJobsPause(filePath, urlPath string, w http.ResponseWriter, r *http.Request) {

	// url or query parameters: pause or resume boolean flag
	sp := getRequestParam(r, "pause")
	isPause, err := strconv.ParseBool(sp)
	if sp == "" || err != nil {
		http.Error(w, "Invalid (or empty) jobs pause flag, expected true or false", http.StatusBadRequest)
		return
	}

	// create jobs paused state file or remove it to resume queue processing
	isOk := false
	if isPause {
		isOk = fileCreateEmpty(false, filePath)
	} else {
		isOk = fileDeleteAndLog(false, filePath)
	}
	if !isOk {
		isPause = !isPause // operation failed
	}

	// Content-Location: /api/admin/jobs-pause/true
	w.Header().Set("Content-Location", urlPath+strconv.FormatBool(isPause))
	w.Header().Set("Content-Type", "text/plain")
}

// async start of model database cleanup and retrun LogFileName on success
//
//	POST /api/admin/db-cleanup/:path
//	POST /api/admin/db-cleanup/:path/name/:name
//	POST /api/admin/db-cleanup/:path/name/:name/digest/:digest
//
// Relative path to model database file is required, slash / in the path must be replaced with * star.
// Model name and digest are optional parameters.
// Cleanup is done on separate thread by db cleanup script, defined in disk.ini [Common] DbCleanup.
// Model database must be closed, for example by: POST /api/admin/model/:model/close.
func modelDbCleanupHandler(w http.ResponseWriter, r *http.Request) {

	// if disk space use control disabled then do nothing
	if !theCfg.isDiskUse {
		w.Header().Set("Content-Location", "/api/admin/db-cleanup/none")
		w.Header().Set("Content-Type", "text/plain")
	}

	// validate parameters: path to database file is required
	dbPath := getRequestParam(r, "path")
	name := getRequestParam(r, "name")
	digest := getRequestParam(r, "digest")

	if dbPath == "" {
		omppLog.Log("Error: invalid (empty) path to model database file")
		http.Error(w, "Invalid (empty) path to model database file", http.StatusBadRequest)
		return
	}
	dbPath = strings.ReplaceAll(dbPath, "*", "/") // restore slashed / path

	// check if database cleanup script defined
	// check if database file is exists and belong to current oms instance: it must be in the list of instance database files
	diskUse, dbUse := theRunCatalog.getDiskUse()

	if diskUse.dbCleanupCmd == "" {
		omppLog.Log("Error: db cleanup script is not defined in disk.ini")
		http.Error(w, "Error: db cleanup script is not defined in disk.ini", http.StatusInternalServerError)
		return
	}
	if i := slices.IndexFunc(
		dbUse, func(du dbDiskUse) bool { return du.DbPath == dbPath }); i < 0 || i >= len(dbUse) {
		http.Error(w, "Error: model database not found"+" "+name+" "+digest, http.StatusBadRequest)
		return
	}

	// check if model database is closed: it should not be in the list of model db files
	mbs := theCatalog.allModels()

	if i := slices.IndexFunc(mbs, func(mb modelBasic) bool { return mb.relPath == dbPath }); i >= 0 && i < len(mbs) {
		http.Error(w, "Error: model database must be closed"+" "+name+" "+digest, http.StatusBadRequest)
		return
	}

	// make log file path
	ts, _ := theCatalog.getNewTimeStamp()

	ln := filepath.Base(dbPath)
	if ln == "." || ln == "/" || ln == "\\" {
		ln = "no-name"
	}
	ln = "db-cleanup." + ts + "." + ln + ".console.txt"

	lp := ln
	if d, isOk := theCatalog.getModelLogDir(); isOk {
		lp = path.Join(d, lp)
	}

	// start database cleanup
	go func(cmdPath, mDbPath, mName, mDigest, logPath string) {

		// make db cleanup command
		cArgs := []string{
			mDbPath,
			mName,
			mDigest,
		}
		cmd := exec.Command(cmdPath, cArgs...)

		// connect console output to output log file
		outPipe, err := cmd.StdoutPipe()
		if err != nil {
			omppLog.Log("Error at join to stdout log", ": ", logPath, ": ", err)
			return
		}
		errPipe, err := cmd.StderrPipe()
		if err != nil {
			omppLog.Log("Error at join to stderr log", ": ", logPath, ": ", err)
			return
		}
		outDoneC := make(chan bool, 1)
		errDoneC := make(chan bool, 1)
		logTck := time.NewTicker(logTickTimeout * time.Millisecond)

		// start console output listners
		isLogOk := fileCreateEmpty(false, logPath)
		if !isLogOk {
			omppLog.Log("Error at creating log file", ": ", logPath)
		}

		doLog := func(path string, r io.Reader, done chan<- bool) {
			sc := bufio.NewScanner(r)
			for sc.Scan() {
				if isLogOk {
					isLogOk = writeToCmdLog(path, false, sc.Text())
				}
			}
			done <- true
			close(done)
		}
		go doLog(logPath, outPipe, outDoneC)
		go doLog(logPath, errPipe, errDoneC)

		// start db cleanup
		omppLog.Log(cmdPath, " ", strings.Join(cmd.Args, " "))
		isLogOk = writeToCmdLog(logPath, true, strings.Join(cmd.Args, " "))

		err = cmd.Start()
		if err != nil {
			omppLog.Log("Error at", ": ", logPath, ": ", err)
			writeToCmdLog(logPath, true, err.Error())
			return
		}
		// else db cleanup started: wait until completed

		// wait until stdout and stderr closed
		for outDoneC != nil || errDoneC != nil {
			select {
			case _, ok := <-outDoneC:
				if !ok {
					outDoneC = nil
				}
			case _, ok := <-errDoneC:
				if !ok {
					errDoneC = nil
				}
			case <-logTck.C:
			}
		}

		// wait for db cleanup to be completed
		e := cmd.Wait()
		if e != nil {
			omppLog.Log("Error at: ", cmd.Args)
			writeToCmdLog(logPath, true, e.Error())
			return
		}
		// else: completed OK
		if !isLogOk {
			omppLog.Log("Warning: db cleanup log output may be incomplete")
		}
	}(diskUse.dbCleanupCmd, dbPath, name, digest, lp)

	// db cleanup is starting now: return path to log file
	jsonResponse(w, r, struct{ LogFileName string }{LogFileName: ln})
}

/*
// DO NOT USE in production, development only
//
// runTestHandler run command: exe arg
// POST /api/admin/run-test/:exe/:arg
func runTestHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters: executable name and argument
	// executable must be a name only, cannot be a path: / or \ not allowed
	exe := getRequestParam(r, "exe")
	arg := getRequestParam(r, "arg")
	if exe == "" || strings.ContainsAny(exe, "/\\") {
		http.Error(w, "Invalid (or empty) executable name", http.StatusBadRequest)
		return
	}

	// make a command, run it and return combined output
	cmd := exec.Command(exe, arg)

	out, err := cmd.CombinedOutput()
	if err != nil {
		omppLog.Log("Run error: ", err)
		if len(out) > 0 {
			omppLog.Log(string(out))
		}
		http.Error(w, "Error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// return combined output
	w.Header().Set("Content-Location", "/api/admin/run-test/"+exe+"/"+arg)
	w.Header().Set("Content-Type", "text/plain")
	w.Write(out)
}
*/
