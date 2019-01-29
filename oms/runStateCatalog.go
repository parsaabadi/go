// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"sync"
	"time"

	"go.openmpp.org/ompp/helper"
)

// RunStateCatalog is a most recent state of model run for each model.
type RunStateCatalog struct {
	rscLock         sync.Mutex // mutex to lock for model list operations
	isLogDirEnabled bool       // if true then use model log directory for model run logs
	modelLogDir     string     // model log directory, if relative then must be relative to oms root directory
	etcDir          string     // model run templates directory, if relative then must be relative to oms root directory
	lastTimeStamp   string     // most recent last timestamp
	runLst          *list.List // list of model runs state
}

// list of most recent state of model run for each model.
var theRunStateCatalog RunStateCatalog

// RunRequest is request to run the model with specified model options.
// Any log options (LogToConsole, LogToFile, LogFilePath, etc.) are ignored.
type RunRequest struct {
	ModelName   string            // model name to run
	ModelDigest string            // model digest to run
	RunStamp    string            // run stamp, if empty then by default timestamp
	Dir         string            // working directory to run the model, if relative then must be relative to oms root directory
	Opts        map[string]string // model run options
	Env         map[string]string // environment variables to set
	Mpi         struct {
		Np int // if non-zero then number of MPI processes
	}
	Template string // template file name to make run model command line
}

// RunState is model run state.
// Last run log file name is time-stamped: modelName.YYYY_MM_DD_hh_mm_ss_SSS.log
type RunState struct {
	ModelName      string // model name to run
	ModelDigest    string // model digest to run
	RunStamp       string // run stamp, if empty then by default timestamp
	IsFinal        bool   // final state, model completed
	UpdateDateTime string // last update date-time
	RunName        string // run name
	TaskRunName    string // if not empty then task run name
	isLog          bool   // if true then redirect console output to log file
	logPath        string // last run log file name: log/dir/modelName.timestamp.log
}

// procRunState is model run state.
type procRunState struct {
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

// max number of model run states to keep in run list history
const runListMaxSize = 200

// file name of model run template by default
const defaultRunTemplate = "mpiModelRun.template.txt"

// RefreshCatalog reset state of most recent model run for each model.
func (rsc *RunStateCatalog) RefreshCatalog(digestLst []string, modelLogDir, etcDir string) error {

	// model log directory is optional, if empty or not exists then model log disabled
	isLog := modelLogDir != "" && modelLogDir != "."
	if isLog {
		isLog = isDirExist(modelLogDir) == nil
	}

	// lock and update run state catalog
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// update model log directory and etc template directory
	rsc.isLogDirEnabled = isLog
	rsc.modelLogDir = modelLogDir
	rsc.etcDir = etcDir

	// copy most recent half of existing models run history
	rLst := list.New()

	if rsc.runLst != nil {
		n := 0
		for re := rsc.runLst.Front(); re != nil; re = re.Next() {

			rs, ok := re.Value.(*procRunState) // model run state expected
			if !ok || rs == nil {
				continue
			}

			// check if model digest exist in new model list
			for k := range digestLst {
				if rs.ModelDigest == digestLst[k] {
					rLst.PushBack(rs)
					n++
					break
				}
			}

			// copy only half of most recent model run history
			if n > runListMaxSize/2 {
				break
			}
		}
	}
	rsc.runLst = rLst

	return nil
}

// getModelLogDir return model log directory
func (rsc *RunStateCatalog) getModelLogDir() (string, bool) {
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()
	return rsc.modelLogDir, rsc.isLogDirEnabled
}

// getNewTimeStamp return new unique timestamp and source time of it.
func (rsc *RunStateCatalog) getNewTimeStamp() (string, time.Time) {
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	dtNow := time.Now()
	ts := helper.MakeTimeStamp(dtNow)
	if ts == rsc.lastTimeStamp {
		time.Sleep(2 * time.Millisecond)
		dtNow = time.Now()
		ts = helper.MakeTimeStamp(dtNow)
	}
	rsc.lastTimeStamp = ts
	return ts, dtNow
}
