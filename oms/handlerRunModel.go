// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"go.openmpp.org/ompp/omppLog"
)

// modelNewRunHandler run the model identified by model digest-or-name with specified number of sub-values.
// Json is posted to specify run name, input workset name and other run parameters,
// see RunState structre for details.
// POST /api/model/:model/new-run?model=modelNameOrDigest&sub-count=16
// POST /api/model/:model/new-run
// POST /api/model/:model/new-run/sub-values/:sub-count
// If multiple models with same name exist then result is undefined.
// If workset not specified then default workset is used.
// Model run log redirected to log file with unique timestamped file name.
func modelNewRunHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")

	// convert sub-value count, if specified
	subCount, ok := getIntRequestParam(r, "sub-count", 0)
	if !ok {
		http.Error(w, "Invalid sub-value count ", http.StatusBadRequest)
		return
	}

	// decode json request body
	var src RunState
	if !jsonRequestDecode(w, r, &src) {
		return // error at json decode, response done with http error
	}

	// if sub-count not specified by url then use json body value
	if subCount <= 0 {
		subCount = src.SubCount
	}

	// find model metadata by digest or name
	m, ok := theCatalog.ModelDicByDigestOrName(dn)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}
	modelDigest := m.Digest
	modelName := m.Name

	// start model run
	prs, e := theRunStateCatalog.startModelExec(modelDigest, modelName, subCount, &src)
	if e != nil {
		omppLog.Log(e)
		http.Error(w, "Model start failed: "+dn, http.StatusBadRequest)
		return
	}

	// write new model run key and json response
	w.Header().Set("Location", "/api/model/"+modelDigest+"/new-run/"+prs.RunKey)
	jsonResponse(w, r, prs)
}

// modelNewRunLogPageHandler return model current (most recent) run status and log by model digest-or-name.
// GET /api/model/new-run-state?model=modelNameOrDigest&start=0&count=0
// GET /api/model/:model/new-run-state
// GET /api/model/:model/new-run-state/start/:start
// GET /api/model/:model/new-run-state/start/:start/count/:count
// If multiple models with same name exist then result is undefined.
// Model run log is same as console output and include stdout and stderr.
// Run log can be returned by page defined by zero-based "start" line and line count.
// If count <= 0 then all log lines until eof returned, complete current log: start=0, count=0
func modelNewRunLogPageHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters:  model digest-or-name, page offset and page size
	dn := getRequestParam(r, "model")

	start, ok := getIntRequestParam(r, "start", 0)
	if !ok {
		http.Error(w, "Invalid value of start log start line "+dn, http.StatusBadRequest)
		return
	}
	count, ok := getIntRequestParam(r, "count", int(pageMaxSize))
	if !ok {
		http.Error(w, "Invalid value of log line count "+dn, http.StatusBadRequest)
		return
	}

	// find model metadata by digest or name
	m, ok := theCatalog.ModelDicByDigestOrName(dn)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}
	modelDigest := m.Digest
	modelName := m.Name

	// get current run status and page of log lines
	lrp, e := theRunStateCatalog.readModelLastRunLog(modelDigest, start, count)
	if e != nil {
		omppLog.Log(e)
		http.Error(w, "Model run status red failed: "+modelName+": "+dn, http.StatusBadRequest)
		return
	}

	// write new model run key and json response
	jsonResponse(w, r, lrp)
}
