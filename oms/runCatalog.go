// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/openmpp/go/ompp/helper"
)

// RunCatalog is a most recent state of model run for each model.
type RunCatalog struct {
	rscLock      sync.Mutex                     // mutex to lock for model list operations
	models       map[string]modelRunBasic       // map model digest to basic info to run the model and manage log files
	etcDir       string                         // model run templates directory, if relative then must be relative to oms root directory
	runTemplates []string                       // list of model run templates
	mpiTemplates []string                       // list of model MPI run templates
	presets      []RunOptionsPreset             // list of preset run options
	runLst       *list.List                     // list of model runs state (runStateLog)
	modelLogs    map[string]map[string]RunState // map each model digest to run stamps to run state and run log path
	jobsUpdateDt string                         // last date-time jobs list updated
	isPaused     bool                           // if true then job queue is paused, jobs are not selected from queue
	queueKeys    []string                       // run job keys of model runs waiting in the queue
	activeKeys   []string                       // job keys of active (currently running) model runs
	historyKeys  []string                       // job keys of models run history
	queueJobs    map[string]runJobFile          // model run jobs waiting in the queue
	activeJobs   map[string]runJobFile          // active (currently running) model run jobs
	historyJobs  map[string]historyJobFile      // models run jobs history
}

var theRunCatalog RunCatalog // list of most recent state of model run for each model.

// modelRunBasic is basic info to run model and obtain model logs
type modelRunBasic struct {
	name     string // model name
	binDir   string // database and .exe directory: directory part of models/bin/model.sqlite
	logDir   string // model log directory
	isLogDir bool   // if true then use model log directory for model run logs
}

// RunCatalogConfig is "public" state of model run catalog for json import-export
type RunCatalogConfig struct {
	RunTemplates       []string           // list of model run templates
	DefaultMpiTemplate string             // default template to run MPI model
	MpiTemplates       []string           // list of model MPI run templates
	Presets            []RunOptionsPreset // list of preset run options
}

// RunOptionsPreset is "public" view of model run options preset
type RunOptionsPreset struct {
	Name    string // name of preset, based on file name
	Options string // run options as json stringify
}

// RunRequest is request to run the model with specified model options.
// Log to console always enabled.
// Model run console output redirected to log file: modelName.YYYY_MM_DD_hh_mm_ss_SSS.console.log
type RunRequest struct {
	ModelName   string            // model name to run
	ModelDigest string            // model digest to run
	RunStamp    string            // run stamp, if empty then auto-generated as timestamp
	Dir         string            // working directory to run the model, if relative then must be relative to oms root directory
	Opts        map[string]string // model run options
	Env         map[string]string // environment variables to set
	Threads     int               // number of modelling threads
	Mpi         struct {
		Np int // if non-zero then number of MPI processes
	}
	Template string   // template file name to make run model command line
	Tables   []string // if not empty then output tables or table groups to retain, by default retain all tables
	RunNotes []struct {
		LangCode string // model language code
		Note     string // run notes
	}
}

// RunJob is model run request and run job control: submission stamp and model process id
type RunJob struct {
	SubmitStamp string // submission timestamp
	Pid         int    // process id
	CmdPath     string // executable path
	RunRequest         // model run request: model name, digest and run options
	LogFileName string // log file name
	LogPath     string // log file path: log/dir/modelName.RunStamp.console.log
}

// run job control file info
type runJobFile struct {
	omsName  string // oms instance name
	filePath string // job control file path
	isError  bool   // if true then ignore that file due to error
	RunJob          // job control file content
}

// job control file info for history job: parts of file name
type historyJobFile struct {
	omsName     string // oms instance name
	filePath    string // job control file path
	isError     bool   // if true then ignore that file due to error
	SubmitStamp string // submission timestamp
	ModelName   string // model name
	ModelDigest string // model digest
	RunStamp    string // run stamp, if empty then auto-generated as timestamp
	JobStatus   string // run status
}

// job control state
type jobControlState struct {
	Paused bool     // if true then job queue paused
	Queue  []string // jobs queue
}

// RunState is model run state.
// Model run console output redirected to log file: modelName.YYYY_MM_DD_hh_mm_ss_SSS.console.log
type RunState struct {
	ModelName      string    // model name
	ModelDigest    string    // model digest
	RunStamp       string    // model run stamp, may be auto-generated as timestamp
	SubmitStamp    string    // submission timestamp
	IsFinal        bool      // final state, model completed
	UpdateDateTime string    // last update date-time
	RunName        string    // if not empty then run name
	TaskRunName    string    // if not empty then task run name
	IsLog          bool      // if true then use run log file
	LogFileName    string    // log file name
	logPath        string    // log file path: log/dir/modelName.RunStamp.console.log
	isHistory      bool      // if true then it is model run history or run done outside of oms service
	pid            int       // process id
	cmdPath        string    // executable path
	killC          chan bool // channel to kill model process
}

// runStateLog is model run state and log file lines.
type runStateLog struct {
	RunState            // model run state
	logLineLst []string // model run log lines
}

// RunStateLogPage is run model status and page of the log lines.
type RunStateLogPage struct {
	RunState           // model run state
	Offset    int      // log page start line
	Size      int      // log page size
	TotalSize int      // log total run line count
	Lines     []string // page of log lines
}

// timeout in msec, wait on stdout and stderr polling.
const logTickTimeout = 7

// file name of MPI model run template by default
const defaultMpiTemplate = "mpi.ModelRun.template.txt"

// RefreshCatalog reset state of most recent model run for each model.
func (rsc *RunCatalog) refreshCatalog(etcDir string, jsc *jobControlState) error {

	// get list of template files
	rsc.runTemplates = []string{}
	rsc.mpiTemplates = []string{}
	if dirExist(etcDir) == nil {
		if fl, err := filepath.Glob(etcDir + "/" + "run.*.template.txt"); err == nil {
			for k := range fl {
				f := filepath.Base(fl[k])
				if f != "." && f != ".." && f != "/" && f != "\\" {
					rsc.runTemplates = append(rsc.runTemplates, f)
				}
			}
		}
		if fl, err := filepath.Glob(etcDir + "/" + "mpi.*.template.txt"); err == nil {
			for k := range fl {
				f := filepath.Base(fl[k])
				if f != "." && f != ".." && f != "/" && f != "\\" {
					rsc.mpiTemplates = append(rsc.mpiTemplates, f)
				}
			}
		}
	}

	// read all run options preset files
	// keep steam of preset file name: run-options.RiskPaths.1-small.json => RiskPaths.1-small
	// and file content as string
	rsc.presets = []RunOptionsPreset{}
	if dirExist(etcDir) == nil {
		if fl, err := filepath.Glob(etcDir + "/" + "run-options.*.json"); err == nil {
			for k := range fl {

				f := filepath.Base(fl[k])
				if len(f) < len("run-options.*.json") { // file name must be at least that size
					continue
				}
				bt, err := os.ReadFile(fl[k]) // read entire file
				if err != nil {
					continue // skip on errors
				}

				rsc.presets = append(rsc.presets,
					RunOptionsPreset{
						Name:    f[len("run-options.") : len(f)-(len(".json"))], // stem of the file: skip prefix and suffix
						Options: string(bt),
					})
			}
		}
	}

	// make all models basic info: name, digest and files location
	mbs := theCatalog.allModels()
	rbs := make(map[string]modelRunBasic, len(mbs))

	for idx := range mbs {
		rbs[mbs[idx].digest] = modelRunBasic{
			name:     mbs[idx].name,
			binDir:   mbs[idx].binDir,
			logDir:   mbs[idx].logDir,
			isLogDir: mbs[idx].isLogDir,
		}
	}

	// lock and update run state catalog
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// update etc directory and list of templates
	rsc.etcDir = etcDir

	// copy existing models run history
	rLst := list.New()

	if rsc.runLst != nil {
		for re := rsc.runLst.Front(); re != nil; re = re.Next() {

			rs, ok := re.Value.(*runStateLog) // model run state expected
			if !ok || rs == nil {
				continue
			}
			if _, ok = rbs[rs.ModelDigest]; ok { // copy existing run history
				rLst.PushBack(rs)
			}
		}
	}
	rsc.runLst = rLst

	// model log history: add new models and delete existing models
	if rsc.modelLogs == nil {
		rsc.modelLogs = map[string]map[string]RunState{}
	}
	// if model deleted then delete model logs history
	for d := range rsc.modelLogs {
		if _, ok := rbs[d]; !ok {
			delete(rsc.modelLogs, d)
		}
	}
	// if new model added then add new empty logs history
	for d := range rbs {
		if _, ok := rsc.modelLogs[d]; !ok {
			rsc.modelLogs[d] = map[string]RunState{}
		}
	}
	rsc.models = rbs

	// cleanup jobs control files info
	rsc.jobsUpdateDt = helper.MakeDateTime(time.Now())
	rsc.activeKeys = make([]string, 0, theCfg.runHistoryMaxSize)
	rsc.historyKeys = make([]string, 0, theCfg.runHistoryMaxSize)
	rsc.queueJobs = make(map[string]runJobFile, theCfg.runHistoryMaxSize)
	rsc.activeJobs = make(map[string]runJobFile, theCfg.runHistoryMaxSize)
	rsc.historyJobs = make(map[string]historyJobFile, theCfg.runHistoryMaxSize)

	if jsc == nil {
		rsc.queueKeys = make([]string, 0, theCfg.runHistoryMaxSize)
	} else {
		rsc.isPaused = jsc.Paused
		if len(jsc.Queue) > 0 {
			rsc.queueKeys = append(rsc.queueKeys, jsc.Queue...)
		}
	}

	return nil
}

// get "public" configuration of model run catalog
func (rsc *RunCatalog) toPublicConfig() *RunCatalogConfig {

	// lock run catalog and return results
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rcp := RunCatalogConfig{
		RunTemplates:       make([]string, len(rsc.runTemplates)),
		DefaultMpiTemplate: defaultMpiTemplate,
		MpiTemplates:       make([]string, len(rsc.mpiTemplates)),
		Presets:            make([]RunOptionsPreset, len(rsc.presets)),
	}
	copy(rcp.RunTemplates, rsc.runTemplates)
	copy(rcp.MpiTemplates, rsc.mpiTemplates)
	copy(rcp.Presets, rsc.presets)

	return &rcp
}

// allModels return basic info from catalog about all models.
func (rsc *RunCatalog) allModels() map[string]modelRunBasic {
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rbs := make(map[string]modelRunBasic, len(rsc.models))
	for key, val := range rsc.models {
		rbs[key] = val
	}
	return rbs
}

// update run catalog with current job control files
func (rsc *RunCatalog) updateRunJobs(queueJobs map[string]runJobFile, activeJobs map[string]runJobFile, historyJobs map[string]historyJobFile) *jobControlState {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// update queue with current list of job control files
	rsc.jobsUpdateDt = helper.MakeDateTime(time.Now())

	n := len(queueJobs)
	if n < theCfg.runHistoryMaxSize {
		n = theCfg.runHistoryMaxSize
	}
	rsc.queueJobs = make(map[string]runJobFile, n)

	if n < cap(rsc.queueKeys) {
		n = cap(rsc.queueKeys)
	}
	qKeys := make([]string, 0, n)

	// copy existing queue job keys which still in the queue
	if len(queueJobs) > 0 {
		for _, jobKey := range rsc.queueKeys {

			jf, ok := queueJobs[jobKey]
			if !ok {
				continue // skip: job is no longer in the queue
			}
			if jf.isError || jf.omsName != theCfg.omsName {
				continue // skip: model job error or it is a different oms instance
			}
			if _, ok = rsc.models[jf.ModelDigest]; !ok {
				continue // skip: model digest is not the models list
			}

			// check if job already exists in job list
			isFound := false
			for k := 0; !isFound && k < len(qKeys); k++ {
				isFound = qKeys[k] == jobKey
			}
			if !isFound {
				qKeys = append(qKeys, jobKey)
			}
		}
	}
	rsc.queueKeys = qKeys

	// update queue jobs and collect all new job keys
	qKeys = make([]string, 0, n)

	for jobKey, jf := range queueJobs {

		if _, ok := rsc.models[jf.ModelDigest]; !ok {
			continue // skip: model digest is not the models list
		}
		if jf.isError || jf.omsName != theCfg.omsName {
			continue // skip: model job error or it is a different oms instance
		}
		rsc.queueJobs[jobKey] = jf

		// check if job already exists in job list
		isFound := false
		for k := 0; !isFound && k < len(rsc.queueKeys); k++ {
			isFound = rsc.queueKeys[k] == jobKey
		}
		if !isFound {
			qKeys = append(qKeys, jobKey)
		}
	}

	// append new job keys at the end of existing queue
	sort.Strings(qKeys)
	rsc.queueKeys = append(rsc.queueKeys, qKeys...)

	// update active model run jobs
	n = len(activeJobs)
	if n < theCfg.runHistoryMaxSize {
		n = theCfg.runHistoryMaxSize
	}
	rsc.activeJobs = make(map[string]runJobFile, n)

	if n < cap(rsc.activeKeys) {
		n = cap(rsc.activeKeys)
	}
	rsc.activeKeys = make([]string, 0, n)

	for jobKey, jf := range activeJobs {
		if _, ok := rsc.models[jf.ModelDigest]; !ok {
			continue // skip: model digest is not the models list
		}
		if jf.isError || jf.omsName != theCfg.omsName {
			continue // skip: model job error or it is a different oms instance
		}
		rsc.activeJobs[jobKey] = jf
		rsc.activeKeys = append(rsc.activeKeys, jobKey)
	}
	sort.Strings(rsc.activeKeys)

	// update model run job history
	n = len(historyJobs)
	if n < theCfg.runHistoryMaxSize {
		n = theCfg.runHistoryMaxSize
	}
	rsc.historyJobs = make(map[string]historyJobFile, n)

	if n < cap(rsc.historyKeys) {
		n = cap(rsc.historyKeys)
	}
	rsc.historyKeys = make([]string, 0, n)

	for jobKey, jh := range historyJobs {
		rsc.historyJobs[jobKey] = jh
		if !jh.isError && jh.omsName == theCfg.omsName {
			rsc.historyKeys = append(rsc.historyKeys, jobKey)
		}
	}
	sort.Strings(rsc.historyKeys)

	// retrun job control state
	jsc := jobControlState{
		Paused: rsc.isPaused,
		Queue:  make([]string, len(rsc.queueKeys)),
	}
	copy(jsc.Queue, rsc.queueKeys)

	return &jsc
}

// return copy of job keys and job control items for queue, active and history model run jobs
func (rsc *RunCatalog) getRunJobs() (string, []string, []RunJob, []string, []RunJob, []string, []historyJobFile) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	qKeys := make([]string, len(rsc.queueKeys))
	qJobs := make([]RunJob, len(rsc.queueKeys))
	for k, jobKey := range rsc.queueKeys {
		qKeys[k] = jobKey
		qJobs[k] = rsc.queueJobs[jobKey].RunJob
	}

	aKeys := make([]string, len(rsc.activeKeys))
	aJobs := make([]RunJob, len(rsc.activeKeys))
	for k, jobKey := range rsc.activeKeys {
		aKeys[k] = jobKey
		aJobs[k] = rsc.activeJobs[jobKey].RunJob
	}

	hKeys := make([]string, len(rsc.historyKeys))
	hJobs := make([]historyJobFile, len(rsc.historyKeys))
	for k, jobKey := range rsc.historyKeys {
		hKeys[k] = jobKey
		hJobs[k] = rsc.historyJobs[jobKey]
	}

	return rsc.jobsUpdateDt, qKeys, qJobs, aKeys, aJobs, hKeys, hJobs
}

// return active job control item and is found boolean flag
func (rsc *RunCatalog) getActiveJobItem(jobKey string) (runJobFile, bool) {

	if jobKey == "" {
		return runJobFile{}, false // empty job key: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if aj, ok := rsc.activeJobs[jobKey]; ok {
		return aj, true
	}
	return runJobFile{}, false // not found
}

// return queue job control item and is found boolean flag
func (rsc *RunCatalog) getQueueJobItem(jobKey string) (runJobFile, bool) {

	if jobKey == "" {
		return runJobFile{}, false // empty job key: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if qj, ok := rsc.queueJobs[jobKey]; ok {
		return qj, true
	}
	return runJobFile{}, false // not found
}

// return history job control item and is found boolean flag
func (rsc *RunCatalog) getHistoryJobItem(jobKey string) (historyJobFile, bool) {

	if jobKey == "" {
		return historyJobFile{}, false // empty job key: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if hj, ok := rsc.historyJobs[jobKey]; ok {
		return hj, true
	}
	return historyJobFile{}, false // not found
}

// move job into the specified queue position.
// Top of the queue position is zero, negative position treated as zero.
// If position number exceeds queue length then job moved to the bottom of the queue.
// Return false if job not found in the queue
func (rsc *RunCatalog) moveJobInQueue(jobKey string, position int) bool {

	if jobKey == "" {
		return false // empty job key: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find current job position in the queue
	isFound := false
	n := 0
	for n = range rsc.queueKeys {
		isFound = rsc.queueKeys[n] == jobKey
		if isFound {
			break
		}
	}
	if !isFound {
		return false // job not found in the queue
	}

	// position must be between zero at the last postion in the queue
	nPos := position
	if nPos <= 0 {
		nPos = 0
	}
	if nPos >= len(rsc.queueKeys)-1 {
		nPos = len(rsc.queueKeys) - 1
	}
	if nPos == n {
		return true // job is already at this position
	}

	// move down to the queue: shift items up
	if nPos > n {

		for k := n; k < nPos && k < len(rsc.queueKeys)-1; k++ {
			rsc.queueKeys[k] = rsc.queueKeys[k+1]
		}
	} else { // move up in the queue: shift items down

		for k := n - 1; k >= nPos && k >= 0; k-- {
			rsc.queueKeys[k+1] = rsc.queueKeys[k]
		}
	}
	rsc.queueKeys[nPos] = jobKey

	return true
}
