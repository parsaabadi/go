// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"
	"strconv"

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
