// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"go.openmpp.org/ompp/omppLog"
)

// runModelHandler run the model identified by model digest-or-name with specified run options.
// POST /api/run
// Json RunRequest structre is posted to specify model digest-or-name, run stamp and othe run options.
// If multiple models with same name exist then result is undefined.
// Model run console output redirected to log file: models/log/modelName.runStamp.console.log
func runModelHandler(w http.ResponseWriter, r *http.Request) {

	// decode json request body
	var src RunRequest
	if !jsonRequestDecode(w, r, &src) {
		return // error at json decode, response done with http error
	}

	// find model metadata by digest or name
	dn := src.ModelDigest
	if dn == "" {
		dn = src.ModelName
	}
	m, ok := theCatalog.ModelDicByDigestOrName(dn)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}
	src.ModelDigest = m.Digest
	src.ModelName = m.Name

	// start model run
	prs, e := theRunStateCatalog.runModel(&src)
	if e != nil {
		omppLog.Log(e)
		http.Error(w, "Model start failed: "+dn, http.StatusBadRequest)
		return
	}

	// write new model run key and json response
	w.Header().Set("Content-Location", "/api/model/"+src.ModelDigest+"/run/"+prs.RunStamp)
	jsonResponse(w, r, prs)
}

// runModelLogPageHandler return model run status and log by model digest-or-name and run stamp.
// GET /api/run/log/model/:model/stamp/:stamp
// GET /api/run/log/model/:model/stamp/:stamp/start/:start/count/:count
// GET /api/run-log?model=modelNameOrDigest&stamp=runStamp&start=0&count=0
// If multiple models with same name exist then result is undefined.
// Model run log is same as console output and include stdout and stderr.
// Run log can be returned by page defined by zero-based "start" line and line count.
// If count <= 0 then all log lines until eof returned, complete current log: start=0, count=0
func runModelLogPageHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters:  model digest-or-name, page offset and page size
	dn := getRequestParam(r, "model")
	runStamp := getRequestParam(r, "stamp")

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
	lrp, e := theRunStateCatalog.readModelLastRunLog(modelDigest, runStamp, start, count)
	if e != nil {
		omppLog.Log(e)
		http.Error(w, "Model run status read failed: "+modelName+": "+dn, http.StatusBadRequest)
		return
	}

	// write new model run key and json response
	jsonResponse(w, r, lrp)
}
