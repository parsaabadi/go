// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"
	"strconv"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
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
			st.Queue[k].Env = map[string]string{}
		}

		st.Active = make([]rj, len(aKeys))
		for k := range aKeys {
			st.Active[k] = rj{JobKey: aKeys[k], RunJob: aJobs[k]}
			st.Active[k].Pid = 0
			st.Active[k].CmdPath = ""
			st.Active[k].LogPath = ""
			st.Active[k].Env = map[string]string{}
		}

		st.History = make([]hj, len(hKeys))
		for k := range hKeys {
			st.History[k] = hj{JobKey: hKeys[k], historyJobFile: hJobs[k]}
		}
		st.UpdateDateTime = updateDt
	}
	jsonResponse(w, r, st)
}

// job control state, log file content and run progress
type runJobState struct {
	JobKey    string      // if empty then job not found
	JobStatus string      // if not empty then job run status name: success, error, exit
	RunJob                // job control state: job control file content
	RunStatus []db.RunPub // if not empty then run_lst and run_progerss from db
	Lines     []string    // log file content
}

// return empty value of job control state
func emptyRunJobState(jKey string) runJobState {
	return runJobState{
		JobKey: jKey,
		RunJob: RunJob{
			RunRequest: RunRequest{
				Opts:   map[string]string{},
				Env:    map[string]string{},
				Tables: []string{},
				RunNotes: []struct {
					LangCode string
					Note     string
				}{}},
		},
		RunStatus: []db.RunPub{},
		Lines:     []string{},
	}
}

// jobActiveHandler return active job state, run log file content and, if model run exists in database then also run progress
// GET /api/service/job/active/:job
func jobActiveHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters: job key
	jKey := getRequestParam(r, "job")
	if jKey == "" {
		http.Error(w, "Invalid (empty) job key", http.StatusBadRequest)
		return
	}

	// find job state in run catalog
	jKey = jobKeyFromStamp(jKey)
	aj, isOk := theRunCatalog.getActiveJobItem(jKey)

	if !isOk || aj.isError {
		jsonResponse(w, r, emptyRunJobState(jKey)) // job not found or job control file error
		return
	}

	// get job control state, read log file and run progress, if it is available
	isOk, st := getJobState(aj.filePath)
	if !isOk {
		jsonResponse(w, r, emptyRunJobState(jKey)) // unable to read job control file
		return
	}
	st.JobKey = jKey

	jsonResponse(w, r, st) // retrun final result
}

// jobQueueHandler return queue job state
// GET /api/service/job/queue/:job
func jobQueueHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters: job key
	jKey := getRequestParam(r, "job")
	if jKey == "" {
		http.Error(w, "Invalid (empty) job key", http.StatusBadRequest)
		return
	}

	// find job state in run catalog
	jKey = jobKeyFromStamp(jKey)
	qj, isOk := theRunCatalog.getQueueJobItem(jKey)

	if !isOk || qj.isError {
		jsonResponse(w, r, emptyRunJobState(jKey)) // job not found or job control file error
		return
	}

	// get job control state, log file and run progress are always empty
	isOk, st := getJobState(qj.filePath)
	if !isOk {
		jsonResponse(w, r, emptyRunJobState(jKey)) // unable to read job control file
		return
	}
	st.JobKey = jKey

	jsonResponse(w, r, st) // retrun final result
}

// jobHistoryHandler return history job state, run log file content and, if model run exists in database then also return run progress
// GET /api/service/job/history/:job
func jobHistoryHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters: job key
	jKey := getRequestParam(r, "job")
	if jKey == "" {
		http.Error(w, "Invalid (empty) job key", http.StatusBadRequest)
		return
	}

	// find job state in run catalog
	jKey = jobKeyFromStamp(jKey)
	hj, isOk := theRunCatalog.getHistoryJobItem(jKey)

	if !isOk || hj.isError {
		jsonResponse(w, r, emptyRunJobState(jKey)) // job not found or job control file error
		return
	}

	// get job control state, read log file and run progress, if it is available
	isOk, st := getJobState(hj.filePath)
	if !isOk {
		jsonResponse(w, r, emptyRunJobState(jKey)) // unable to read job control file
		return
	}
	st.JobKey = jKey
	st.JobStatus = hj.JobStatus

	jsonResponse(w, r, st) // retrun final result
}

// getJobState returns job control file content, run log file content and, if model run exists in database then also return run progress
// it is clear (set to empty) server-only part of job state: pid, exe path, log path and environment
func getJobState(filePath string) (bool, *runJobState) {

	st := emptyRunJobState("")

	// read job control file
	var jc RunJob
	isOk, err := helper.FromJsonFile(filePath, &jc)
	if err != nil {
		omppLog.Log(err)
	}
	if !isOk || err != nil {
		return false, &st
	}

	// set job state and clear server-only part of the job state: pid, exe path, log path and environment
	st.RunJob = jc
	st.Pid = 0
	st.CmdPath = ""
	st.LogPath = ""
	st.Env = map[string]string{}
	if len(st.Opts) == 0 {
		st.Opts = map[string]string{}
	}

	// read log file content
	if jc.LogPath != "" {
		st.Lines, _ = readLogFile(jc.LogPath)
	}

	// get run progress if model run exist in database
	if jc.ModelDigest != "" && jc.RunStamp != "" {
		if _, isOk = theCatalog.ModelDicByDigest(jc.ModelDigest); isOk { // if model exist
			if rst, isOk := theCatalog.RunStatusList(jc.ModelDigest, jc.RunStamp); isOk { // get run status
				st.RunStatus = rst
			}
		}
	}

	return true, &st // retrun final result
}

// jobMoveHandler move job into the specified queue position.
// Top of the queue position is zero, negative position treated as zero.
// If position number exceeds queue length then job moved to the bottom of the queue.
// PUT /api/service/job/move/:pos/:job
func jobMoveHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters: position and job key
	sp := getRequestParam(r, "pos")
	nPos, err := strconv.Atoi(sp)
	if sp == "" || err != nil {
		http.Error(w, "Invalid (or empty) job queue position", http.StatusBadRequest)
		return
	}

	jKey := getRequestParam(r, "job")
	if jKey == "" {
		http.Error(w, "Invalid (empty) job key", http.StatusBadRequest)
		return
	}

	// move job in the queue
	jKey = jobKeyFromStamp(jKey)
	isOk := theRunCatalog.moveJobInQueue(jKey, nPos)

	if !isOk {
		w.Header().Set("Content-Location", "service/job/move/false/"+sp+"/"+jKey)
		return
	}
	// else: job moved into the spoecified queue position
	w.Header().Set("Content-Location", "service/job/move/true/"+sp+"/"+jKey)
}
