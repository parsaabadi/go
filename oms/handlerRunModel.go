// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"
	"strconv"

	"golang.org/x/text/language"

	"github.com/openmpp/go/ompp/omppLog"
)

// runModelHandler run the model identified by model digest-or-name with specified run options.
// POST /api/run
// Json RunRequest structre is posted to specify model digest-or-name, run stamp and othe run options.
// If multiple models with same name exist then result is undefined.
// Model run console output redirected to log file: models/log/modelName.runStamp.console.log
func runModelHandler(w http.ResponseWriter, r *http.Request) {

	// decode json request body
	var req RunRequest
	if !jsonRequestDecode(w, r, true, &req) {
		return // error at json decode, response done with http error
	}

	// if log messages language not specified then use browser preferred language
	if _, ok := req.Opts["OpenM.MessageLanguage"]; !ok {
		if rqLangTags, _, e := language.ParseAcceptLanguage(r.Header.Get("Accept-Language")); e == nil {
			if len(rqLangTags) > 0 && rqLangTags[0] != language.Und {
				req.Opts["OpenM.MessageLanguage"] = rqLangTags[0].String()
			}
		}
	}

	// find model metadata by digest or name
	dn := req.ModelDigest
	if dn == "" {
		dn = req.ModelName
	}
	m, ok := theCatalog.ModelDicByDigestOrName(dn)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}
	req.ModelDigest = m.Digest
	req.ModelName = m.Name

	// if job control enabled then add model run to queue
	submitStamp, _ := theCatalog.getNewTimeStamp()

	e := addJobToQueue(submitStamp, &req)
	if e != nil {
		http.Error(w, "Model run submission failed: "+dn, http.StatusBadRequest)
		return
	}

	// start model run
	prs, e := theRunCatalog.runModel(submitStamp, &req)
	if e != nil {
		omppLog.Log(e)
		http.Error(w, "Model start failed: "+dn, http.StatusBadRequest)
		return
	}

	// write new model run key and json response
	w.Header().Set("Content-Location", "/api/model/"+req.ModelDigest+"/run/"+prs.RunStamp)
	jsonResponse(w, r, prs)
}

// runModelStopHandler kill model run by model digest-or-name and run stamp or remove model run request from queue by submit stamp.
// PUT /api/run/stop/model/:model/stamp/:stamp
// If multiple models with same name exist then result is undefined.
func runModelStopHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters:  model digest-or-name, page offset and page size
	dn := getRequestParam(r, "model")
	stamp := getRequestParam(r, "stamp")

	// find model metadata by digest or name
	m, ok := theCatalog.ModelDicByDigestOrName(dn)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}
	modelDigest := m.Digest

	// kill model run by run stamp or
	// remove run request from the queue by submit stamp or by run stamp
	isDone := theRunCatalog.stopModelRun(modelDigest, stamp)

	// write new model run key and json response
	w.Header().Set("Content-Location", "/api/model/"+modelDigest+"/run/"+stamp+"/"+strconv.FormatBool(isDone))
}

// runModelLogPageHandler return model run status and log by model digest-or-name and run stamp.
// GET /api/run/log/model/:model/stamp/:stamp
// GET /api/run/log/model/:model/stamp/:stamp/start/:start/count/:count
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
	count, ok := getIntRequestParam(r, "count", int(theCfg.pageMaxSize))
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
	lrp, e := theRunCatalog.readModelRunLog(modelDigest, runStamp, start, count)
	if e != nil {
		omppLog.Log(e)
		http.Error(w, "Model run status read failed: "+modelName+": "+dn, http.StatusBadRequest)
		return
	}

	// write new model run key and json response
	jsonResponse(w, r, lrp)
}
