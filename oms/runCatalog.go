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

	"github.com/openmpp/go/ompp/db"

	"github.com/openmpp/go/ompp/omppLog"
)

// RunCatalog is a most recent state of model run for each model.
type RunCatalog struct {
	rscLock      sync.Mutex                     // mutex to lock for model list operations
	models       map[string]modelRunBasic       // map model digest to basic info to run the model and manege log files
	etcDir       string                         // model run templates directory, if relative then must be relative to oms root directory
	runTemplates []string                       // list of model run templates
	mpiTemplates []string                       // list of model MPI run templates
	presets      []RunOptionsPreset             // list of preset run options
	runLst       *list.List                     // list of model runs state (runStateLog) submitted through the service
	modelLogs    map[string]map[string]RunState // model runs state: map model digest to run stamp to run state
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
	SubmitStamp string            // submission timestamp
	Dir         string            // working directory to run the model, if relative then must be relative to oms root directory
	Opts        map[string]string // model run options
	Env         map[string]string // environment variables to set
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

// RunState is model run state.
// Model run console output redirected to log file: modelName.YYYY_MM_DD_hh_mm_ss_SSS.console.log
type RunState struct {
	ModelName      string // model name
	ModelDigest    string // model digest
	RunStamp       string // model run stamp, may be auto-generated as timestamp
	SubmitStamp    string // submission timestamp
	IsFinal        bool   // final state, model completed
	UpdateDateTime string // last update date-time
	RunName        string // if not empty then run name
	TaskRunName    string // if not empty then task run name
	IsLog          bool   // if true then use run log file
	LogFileName    string // log file name
	logPath        string // log file path: log/dir/modelName.RunStamp.console.log
	isHistory      bool   // if true then it is model run history or run done outside of oms service
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

// timeout in msec, sleep interval between scanning log directory
const scanSleepTimeout = 4021

// RefreshCatalog reset state of most recent model run for each model.
func (rsc *RunCatalog) refreshCatalog(etcDir string) error {

	// get list of template files
	rsc.runTemplates = []string{}
	rsc.mpiTemplates = []string{}
	if isDirExist(etcDir) == nil {
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
	if isDirExist(etcDir) == nil {
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
		n := 0
		for re := rsc.runLst.Front(); n < theCfg.runHistoryMaxSize && re != nil; re = re.Next() {

			rs, ok := re.Value.(*runStateLog) // model run state expected
			if !ok || rs == nil {
				continue
			}
			if _, ok = rbs[rs.ModelDigest]; ok { // copy existing run history
				rLst.PushBack(rs)
				n++
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

// add new log file names to model log list
// if run stamp already exist in model run history then replace run state with more recent data
func (rsc *RunCatalog) addModelLogs(digest string, runStateLst []RunState) {
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// add new runs to model run history
	for _, r := range runStateLst {

		if ml, ok := rsc.modelLogs[digest]; ok { // skip model if not exist
			ml[r.RunStamp] = r
		}
	}
}

// scan model log directories to collect list log files for each model run history
func scanModelLogDirs(doneC <-chan bool) {

	// path to model run log file found by log directory scan
	type logItem struct {
		runId       int    // model run id
		runStamp    string // model run stamp
		runName     string // model run name
		isCompleted bool   // if true then run completed
		updateDt    string // last update date-time
	}

	for {
		// get current models from run catalog and main catalog
		rbs := theRunCatalog.allModels()
		dLst := theCatalog.allModelDigests()
		sort.Strings(dLst)

		// find new model runs since last scan
		for d, it := range rbs {

			// skip model if digest does not exist in main catalog, ie: catalog closed
			if i := sort.SearchStrings(dLst, d); i < 0 || i >= len(dLst) || dLst[i] != d {
				continue
			}

			// model log directory path must be not empty to search for log files
			if !it.isLogDir {
				continue
			}
			if it.logDir == "" {
				it.logDir = "." // assume current directory if log directory not specified but eanbled by isLogDir
			}
			it.logDir = filepath.ToSlash(it.logDir) // use / path separator

			// get list of model runs
			rl, ok := theCatalog.RunRowListByModelDigest(d)
			if !ok || len(rl) <= 0 {
				continue // no model runs (or ignore get runs error)
			}

			// append new runs to model run list
			logLst := make([]logItem, len(rl))

			for k := range rl {
				logLst[k] = logItem{
					runId:       rl[k].RunId,
					runStamp:    rl[k].RunStamp,
					runName:     rl[k].Name,
					isCompleted: db.IsRunCompleted(rl[k].Status),
					updateDt:    rl[k].UpdateDateTime,
				}
			}

			// get list of model run log files
			ptrn := it.logDir + "/" + it.name + "." + "*.log"

			fLst, err := filepath.Glob(ptrn)
			if err != nil {
				omppLog.Log("Error at log files search: ", ptrn)
				continue
			}

			// replace path separators by / and sort file paths list
			for k := range fLst {
				fLst[k] = filepath.ToSlash(fLst[k])
			}
			sort.Strings(fLst) // sort by file path

			// search new model runs to find log file path
			// append run state to run state list ordered by run id
			rsLst := []RunState{}
			for k := 0; k < len(logLst); k++ {

				// search for modelName.runStamp.console.log
				fn := it.logDir + "/" + it.name + "." + logLst[k].runStamp + ".console.log"
				n := sort.SearchStrings(fLst, fn)
				isFound := n < len(fLst) && fLst[n] == fn

				// search for modelName.runStamp.log
				if !isFound {
					fn = it.logDir + "/" + it.name + "." + logLst[k].runStamp + ".log"
					n = sort.SearchStrings(fLst, fn)
					isFound = n < len(fLst) && fLst[n] == fn
				}

				if isFound {
					rsLst = append(rsLst,
						RunState{
							ModelName:      it.name,
							ModelDigest:    d,
							RunStamp:       logLst[k].runStamp,
							IsFinal:        logLst[k].isCompleted,
							UpdateDateTime: logLst[k].updateDt,
							RunName:        logLst[k].runName,
							IsLog:          true,
							LogFileName:    filepath.Base(fn),
							logPath:        fn,
							isHistory:      true,
						})
				}
			}

			// add new log file names to model log list and update most recent run id for that model
			theRunCatalog.addModelLogs(d, rsLst)
		}

		// wait for doneC or or sleep
		select {
		case <-doneC:
			return
		case <-time.After(scanSleepTimeout * time.Millisecond):
		}
	}
}
