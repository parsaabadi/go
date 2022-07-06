// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"github.com/openmpp/go/ompp/omppLog"
)

// serviceConfigHandler return service configuration, including configuration of model catalog and run catalog.
// GET /api/service/config
func serviceConfigHandler(w http.ResponseWriter, r *http.Request) {

	st := struct {
		OmsName           string             // server instance name
		RowPageMaxSize    int64              // default "page" size: row count to read parameters or output tables
		RunHistoryMaxSize int                // max number of completed model run states to keep in run list history
		DoubleFmt         string             // format to convert float or double value to string
		LoginUrl          string             // user login URL for UI
		LogoutUrl         string             // user logout URL for UI
		AllowUserHome     bool               // if true then store user settings in home directory
		AllowDownload     bool               // if true then allow download from home/io/download directory
		AllowUpload       bool               // if true then allow upload from home/io/upload directory
		IsJobControl      bool               // if true then job control enabled
		Env               map[string]string  // server config environmemt variables
		ModelCatalog      ModelCatalogConfig // "public" state of model catalog
		RunCatalog        RunCatalogConfig   // "public" state of model run catalog
	}{
		OmsName:           theCfg.omsName,
		RowPageMaxSize:    theCfg.pageMaxSize,
		RunHistoryMaxSize: theCfg.runHistoryMaxSize,
		DoubleFmt:         theCfg.doubleFmt,
		AllowUserHome:     theCfg.isHome,
		AllowDownload:     theCfg.downloadDir != "",
		AllowUpload:       theCfg.uploadDir != "",
		IsJobControl:      theCfg.isJobControl,
		Env:               theCfg.env,
		ModelCatalog:      *theCatalog.toPublicConfig(),
		RunCatalog:        *theRunCatalog.toPublicConfig(),
	}
	jsonResponse(w, r, st)
}

// serviceStateHandler return service and model runs state: queue, active runs and run history
// GET /api/service/state
func serviceStateHandler(w http.ResponseWriter, r *http.Request) {

	// run job item
	type rj struct {
		JobKey string // job key to find the job
		RunJob        // model run job control info
	}
	// history job item
	type hj struct {
		JobKey         string // job key to find the job
		historyJobFile        // job control file info for history job
	}
	// service state: model run jobs queue, active jobs and history
	st := struct {
		IsJobControl   bool   // if true then job control enabled
		UpdateDateTime string // last date-time jobs list updated
		Queue          []rj   // list of model run jobs in the queue
		Active         []rj   // list of active (currently running) model run jobs
		History        []hj   // history of model runs
	}{
		IsJobControl: theCfg.isJobControl,
		Queue:        []rj{},
		Active:       []rj{},
		History:      []hj{},
	}

	if theCfg.isJobControl {
		updateDt, qKeys, qJobs, aKeys, aJobs, hKeys, hJobs := theRunCatalog.getRunJobs()

		st.Queue = make([]rj, len(qKeys))
		for k := range qKeys {
			st.Queue[k] = rj{JobKey: qKeys[k], RunJob: qJobs[k]}
		}

		st.Active = make([]rj, len(aKeys))
		for k := range aKeys {
			st.Active[k] = rj{JobKey: aKeys[k], RunJob: aJobs[k]}
			st.Active[k].Pid = 0
			st.Active[k].CmdPath = ""
		}

		st.History = make([]hj, len(hKeys))
		for k := range hKeys {
			st.History[k] = hj{JobKey: hKeys[k], historyJobFile: hJobs[k]}
		}
		st.UpdateDateTime = updateDt
	}
	jsonResponse(w, r, st)
}

// allModelsRefreshHandler reload models catalog: rescan models directory tree and reload model.sqlite.
// POST /api/admin/all-models/refresh
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
	if err := theRunCatalog.refreshCatalog(theCfg.etcDir); err != nil {
		omppLog.Log(err)
		http.Error(w, "Failed to refersh model runs catalog", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Location", "/api/admin/all-models/refresh/"+modelDir)
	w.Header().Set("Content-Type", "text/plain")
}

// allModelsCloseHandler clean models catalog: close all model.sqlite connections and clean models catalog
// POST /api/admin/all-models/close
func allModelsCloseHandler(w http.ResponseWriter, r *http.Request) {

	// close models catalog
	modelDir, _ := theCatalog.getModelDir()

	if err := theCatalog.close(); err != nil {
		omppLog.Log(err)
		http.Error(w, "Failed to close models catalog: "+modelDir, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Location", "/api/admin/all-models/close/"+modelDir)
	w.Header().Set("Content-Type", "text/plain")
}
