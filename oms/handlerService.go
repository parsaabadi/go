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

	// service state: model run jobs queue, active jobs and history
	st := struct {
		IsJobControl   bool             // if true then job control enabled
		UpdateDateTime string           // last date-time jobs list updated
		IsPaused       bool             // if true then jobs queue is paused, jobs are not selected from queue
		ActiveTotalRes RunRes           // active model run resources (CPUs and memory) used by all oms instances
		ActiveOwnRes   RunRes           // active model run resources (CPUs and memory) used by this oms instance
		QueueTotalRes  RunRes           // queue model run resources (CPUs and memory) requested by all oms instances
		QueueOwnRes    RunRes           // queue model run resources (CPUs and memory) requested by this oms instance
		Queue          []RunJob         // list of model run jobs in the queue
		Active         []RunJob         // list of active (currently running) model run jobs
		History        []historyJobFile // history of model runs
	}{
		IsJobControl: theCfg.isJobControl,
		Queue:        []RunJob{},
		Active:       []RunJob{},
		History:      []historyJobFile{},
	}

	if theCfg.isJobControl {
		updateDt, isPause, qKeys, qJobs, qTotal, qOwn, aKeys, aJobs, aTotal, aOwn, hKeys, hJobs := theRunCatalog.getRunJobs()

		st.UpdateDateTime = updateDt
		st.IsPaused = isPause
		st.ActiveTotalRes = aTotal
		st.ActiveOwnRes = aOwn
		st.QueueTotalRes = qTotal
		st.QueueOwnRes = qOwn

		st.Queue = make([]RunJob, len(qKeys))
		for k := range qKeys {
			st.Queue[k] = qJobs[k]
			st.Queue[k].Env = map[string]string{}
		}

		st.Active = make([]RunJob, len(aKeys))
		for k := range aKeys {
			st.Active[k] = aJobs[k]
			st.Active[k].Pid = 0
			st.Active[k].CmdPath = ""
			st.Active[k].LogPath = ""
			st.Active[k].Env = map[string]string{}
		}

		st.History = make([]historyJobFile, len(hKeys))
		for k := range hKeys {
			st.History[k] = hJobs[k]
		}
	}
	jsonResponse(w, r, st)
}

// job control state, log file content and run progress
type runJobState struct {
	JobStatus string      // if not empty then job run status name: success, error, exit
	RunJob                // job control state: job control file content
	RunStatus []db.RunPub // if not empty then run_lst and run_progerss from db
	Lines     []string    // log file content
}

// return empty value of job control state
func emptyRunJobState(submitStamp string) runJobState {
	return runJobState{
		RunJob: RunJob{
			SubmitStamp: submitStamp,
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

	// url or query parameters: submission stamp
	submitStamp := getRequestParam(r, "job")
	if submitStamp == "" {
		http.Error(w, "Invalid (empty) submission stamp", http.StatusBadRequest)
		return
	}

	// find job state in run catalog
	aj, isOk := theRunCatalog.getActiveJobItem(submitStamp)

	if !isOk || aj.isError {
		jsonResponse(w, r, emptyRunJobState(submitStamp)) // job not found or job control file error
		return
	}

	// get job control state, read log file and run progress, if it is available
	isOk, st := getJobState(aj.filePath)
	if !isOk {
		jsonResponse(w, r, emptyRunJobState(submitStamp)) // unable to read job control file
		return
	}

	jsonResponse(w, r, st) // return final result
}

// jobQueueHandler return queue job state
// GET /api/service/job/queue/:job
func jobQueueHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters: submission stamp
	submitStamp := getRequestParam(r, "job")
	if submitStamp == "" {
		http.Error(w, "Invalid (empty) submission stamp", http.StatusBadRequest)
		return
	}

	// find job state in run catalog
	qj, isOk := theRunCatalog.getQueueJobItem(submitStamp)

	if !isOk || qj.isError {
		jsonResponse(w, r, emptyRunJobState(submitStamp)) // job not found or job control file error
		return
	}

	// get job control state, log file and run progress are always empty
	isOk, st := getJobState(qj.filePath)
	if !isOk {
		jsonResponse(w, r, emptyRunJobState(submitStamp)) // unable to read job control file
		return
	}

	jsonResponse(w, r, st) // return final result
}

// jobHistoryHandler return history job state, run log file content and, if model run exists in database then also return run progress
// GET /api/service/job/history/:job
func jobHistoryHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters: submission stamp
	submitStamp := getRequestParam(r, "job")
	if submitStamp == "" {
		http.Error(w, "Invalid (empty) submission stamp", http.StatusBadRequest)
		return
	}

	// find job state in run catalog
	hj, isOk := theRunCatalog.getHistoryJobItem(submitStamp)

	if !isOk || hj.isError {
		jsonResponse(w, r, emptyRunJobState(submitStamp)) // job not found or job control file error
		return
	}

	// get job control state, read log file and run progress, if it is available
	isOk, st := getJobState(hj.filePath)
	if !isOk {
		jsonResponse(w, r, emptyRunJobState(submitStamp)) // unable to read job control file
		return
	}
	st.JobStatus = hj.JobStatus

	jsonResponse(w, r, st) // return final result
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

	return true, &st // return final result
}

// jobMoveHandler move job into the specified queue position.
// Top of the queue position is zero, negative position treated as zero.
// If position number exceeds queue length then job moved to the bottom of the queue.
// PUT /api/service/job/move/:pos/:job
func jobMoveHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters: position and submission stamp
	sp := getRequestParam(r, "pos")
	nPos, err := strconv.Atoi(sp)
	if sp == "" || err != nil {
		http.Error(w, "Invalid (or empty) job queue position", http.StatusBadRequest)
		return
	}

	submitStamp := getRequestParam(r, "job")
	if submitStamp == "" {
		http.Error(w, "Invalid (empty) submission stamp", http.StatusBadRequest)
		return
	}

	// move job in the queue
	isOk := theRunCatalog.moveJobInQueue(submitStamp, nPos)

	w.Header().Set("Content-Type", "text/plain")
	if !isOk {
		w.Header().Set("Content-Location", "service/job/move/false/"+sp+"/"+submitStamp)
		return
	}
	// else: job moved into the spoecified queue position
	w.Header().Set("Content-Location", "service/job/move/true/"+sp+"/"+submitStamp)
}
