// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"github.com/openmpp/go/ompp/omppLog"
)

// CatalogState is a server "public" state, including model catalog state and model runs state
type CatalogState struct {
	RootDir           string          // server root directory
	RowPageMaxSize    int64           // default "page" size: row count to read parameters or output tables
	RunHistoryMaxSize int             // max number of model run states to keep in run list history
	DoubleFmt         string          // format to convert float or double value to string
	LoginUrl          string          // user login URL for UI
	LogoutUrl         string          // user logout URL for UI
	ModelCatalog      ModelCatalogPub // "public" state of model catalog
	RunCatalog        RunCatalogPub   // "public" state of model run catalog
}

// serviceStateHandler return service state and configuration.
// GET /api/service/state
func serviceStateHandler(w http.ResponseWriter, r *http.Request) {

	st := CatalogState{
		RootDir:           theCfg.rootDir,
		RowPageMaxSize:    theCfg.pageMaxSize,
		RunHistoryMaxSize: theCfg.runHistoryMaxSize,
		DoubleFmt:         theCfg.doubleFmt,
		LoginUrl:          theCfg.loginUrl,
		LogoutUrl:         theCfg.logoutUrl,
		ModelCatalog:      *theCatalog.toPublic(),
		RunCatalog:        *theRunStateCatalog.toPublic(),
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
	if err := theCatalog.RefreshSqlite(modelDir, modelLogDir); err != nil {
		omppLog.Log(err)
		http.Error(w, "Failed to refersh models catalog: "+modelDir, http.StatusBadRequest)
		return
	}

	// refresh run state catalog
	if err := theRunStateCatalog.RefreshCatalog(etcDir); err != nil {
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

	if err := theCatalog.Close(); err != nil {
		omppLog.Log(err)
		http.Error(w, "Failed to close models catalog: "+modelDir, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Location", "/api/admin/all-models/close/"+modelDir)
	w.Header().Set("Content-Type", "text/plain")
}
