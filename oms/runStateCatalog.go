// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"bufio"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// RunStateCatalog is a most recent state of model run for each model.
type RunStateCatalog struct {
	rscLock         sync.Mutex               // mutex to lock for model list operations
	isLogDirEnabled bool                     // if true then use model log directory for model run logs
	modelLogDir     string                   // model log directory
	lastTimeStamp   string                   // most recent last timestamp
	runMap          map[string]*procRunState // map[model digest] to state of model run
}

// list of most recent state of model run for each model.
var theRunStateCatalog RunStateCatalog

// timeout in msec, wait on stdout and stderr polling.
const logTickTimeout = 7

// procRunState is model run state.
// Last run log file name is time-stamped: modelName.YYYY_MM_DD_hh_mm_ss_0SSS.log
type procRunState struct {
	RunState            // model run state
	logLineLst []string // model run log lines
}

// RunState is model run state.
// Last run log file name is time-stamped: modelName.YYYY_MM_DD_hh_mm_ss_0SSS.log
type RunState struct {
	RunKey         string // run key: modelName.timestamp
	IsFinal        bool   // final state, model completed
	RunName        string // run name
	SetName        string // if not empty then workset name
	SubCount       int    // subvalue count
	StartDateTime  string // model start date-time
	UpdateDateTime string // last update date-time
	logPath        string // last run log file name: log/dir/modelName.timestamp.log
}

// RunStateLogPage is run model status and page of the log lines.
type RunStateLogPage struct {
	RunState           // model run state
	Offset    int      // log page start line
	Size      int      // log page size
	TotalSize int      // log total run line count
	Lines     []string // page of log lines
}

// RefreshCatalog reset state of most recent model run for each model.
func (rsc *RunStateCatalog) RefreshCatalog(ds []string, modelDir string, modelLogDir string) error {

	// model log directory is optional, if empty or not exists then model log disabled
	isDir := modelLogDir != "" && modelLogDir != "."
	if isDir {
		isDir = isDirExist(filepath.Join(modelDir, modelLogDir)) == nil
	}

	// lock and update run state catalog
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// update model log directory
	rsc.modelLogDir = modelLogDir
	rsc.isLogDirEnabled = isDir

	// for each model create empty run state
	rMap := make(map[string]*procRunState, len(ds))
	for k := range ds {
		rMap[ds[k]] = &procRunState{logLineLst: []string{}}
	}

	rsc.runMap = rMap
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
	ts := helper.MakeUnderscoreTimeStamp(dtNow)
	if ts == rsc.lastTimeStamp {
		time.Sleep(2 * time.Millisecond)
		dtNow = time.Now()
		ts = helper.MakeUnderscoreTimeStamp(dtNow)
	}
	rsc.lastTimeStamp = ts
	return ts, dtNow
}

// get current run status and page of log lines
func (rsc *RunStateCatalog) readModelLastRunLog(digest string, start int, count int) (*RunStateLogPage, error) {
	lrp := &RunStateLogPage{}

	if digest == "" {
		return lrp, nil // empty model digest: exit
	}
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// get model run state by digest
	r, ok := rsc.runMap[digest]
	if !ok {
		return lrp, nil // model digest not found
	}

	// read status and log page lines
	lrp.RunState = r.RunState
	lrp.Offset, lrp.Size, lrp.TotalSize, lrp.Lines = rsc.getFromCurrentLog(digest, start, count)
	return lrp, nil
}

// getFromLog copy [strat, count] log lines from model log.
// If count is zero or less then copy from start to the end of log.
// Return current log line count and slice of log lines.
func (rsc *RunStateCatalog) getFromLog(digest string, runKey string, start int, count int) (int, int, int, []string) {
	if digest == "" {
		return 0, 0, 0, []string{} // empty model digest: exit
	}
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// get model run state by digest
	r, ok := rsc.runMap[digest]
	if !ok {
		return 0, 0, 0, []string{} // model digest not found
	}
	if r.RunKey != runKey {
		return 0, 0, 0, []string{} // model run overriden, only last run stored
	}

	// retrun log page from current log
	return rsc.getFromCurrentLog(digest, start, count)
}

// startModelExec starts new model run and return unique timestamped run key
func (rsc *RunStateCatalog) startModelExec(modelDigest string, modelName string, subCount int, src *RunState) (*RunState, error) {

	// make model run command line arguments
	// assume model exe name same as model name
	mExe := "./" + helper.CleanSpecialChars(modelName)
	exeDir, _ := theCatalog.getModelDir()
	logDir, isDir := rsc.getModelLogDir()
	ts, dtNow := rsc.getNewTimeStamp()
	rKey := helper.ToAlphaNumeric(modelName) + "." + ts

	rs := &RunState{
		RunKey:         rKey,
		StartDateTime:  helper.MakeDateTime(dtNow),
		UpdateDateTime: helper.MakeDateTime(dtNow),
	}
	mArgs := []string{}

	rs.SetName = helper.CleanSpecialChars(src.SetName)
	if rs.SetName != "" {
		mArgs = append(mArgs, "-OpenM.SetName", rs.SetName)
	}

	rs.RunName = helper.CleanSpecialChars(src.RunName)
	if rs.RunName == "" {
		rs.RunName = rs.RunKey
	}
	mArgs = append(mArgs, "-OpenM.RunName", rs.RunName)

	if subCount > 0 {
		rs.SubCount = subCount
		mArgs = append(mArgs, "-OpenM.SubValues", strconv.Itoa(rs.SubCount))
	}

	mArgs = append(mArgs, "-"+config.LogToConsole, "true")
	if isDir && logDir != "" {
		rs.logPath = filepath.Join(logDir, rs.RunKey+".log")
		mArgs = append(mArgs, "-"+config.LogToFile, "true")
		mArgs = append(mArgs, "-"+config.LogFilePath, rs.logPath)
	}

	// build model run command
	cmd := exec.Command(mExe, mArgs...)
	cmd.Dir = exeDir

	// connect console output to log line array
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		return rs, err
	}
	errPipe, e := cmd.StderrPipe()
	if e != nil {
		return rs, err
	}
	outDoneC := make(chan bool, 1)
	errDoneC := make(chan bool, 1)
	logTck := time.NewTicker(logTickTimeout * time.Millisecond)

	// append console output to log lines array
	doLog := func(digest string, runKey string, r io.Reader, done chan<- bool) {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			rsc.updateRunState(digest, runKey, false, sc.Text())
		}
		done <- true
		close(done)
	}

	// wait until model run completed
	doRunWait := func(digest string, runKey string, cmd *exec.Cmd) {

		// wait until stdout and stderr closed
		for outDoneC != nil || errDoneC != nil {
			select {
			case _, ok := <-outDoneC:
				if !ok {
					outDoneC = nil
				}
			case _, ok := <-errDoneC:
				if !ok {
					errDoneC = nil
				}
			case <-logTck.C:
			}
		}

		// wait for model run to be completed
		e = cmd.Wait()
		if e != nil {
			omppLog.Log(e)
			rsc.updateRunState(digest, runKey, true, e.Error())
			return
		}
		// else: completed OK
		rsc.updateRunState(digest, runKey, true, "")
	}

	// run state initialized: save in run state list
	rsc.storeProcRunState(modelDigest, rs)

	// start console output listners
	go doLog(modelDigest, rs.RunKey, outPipe, outDoneC)
	go doLog(modelDigest, rs.RunKey, errPipe, errDoneC)

	// start the model
	err = cmd.Start()
	if err != nil {
		omppLog.Log("Model run error: ", err)
		rsc.updateRunState(modelDigest, rs.RunKey, true, err.Error())
		rs.IsFinal = true
		return rs, err // exit with error: model failed to start
	}
	// else start model listener
	go doRunWait(modelDigest, rs.RunKey, cmd)
	return rs, nil
}

// update model run state
func (rsc *RunStateCatalog) storeProcRunState(digest string, rs *RunState) {
	if digest == "" || rs == nil {
		return
	}
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	prs := &procRunState{}
	prs.RunState = *rs
	if prs.logLineLst == nil {
		prs.logLineLst = make([]string, 0, 32)
	}
	rsc.runMap[digest] = prs
}

// updateRunState does model run state update and append to model log lines array
func (rsc *RunStateCatalog) updateRunState(digest string, runKey string, isFinal bool, msg string) {
	if digest == "" || runKey == "" {
		return // model run undefined
	}
	dtNow := time.Now()

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// get model run state by digest
	r, ok := rsc.runMap[digest]
	if !ok {
		return // model digest not found
	}
	if r.RunKey != runKey {
		return // model run overriden, only last run stored
	}

	// make new log lines array if it is empty
	if r.logLineLst == nil {
		r.logLineLst = []string{}
	}

	// update run state and append new log line if not empty
	r.IsFinal = isFinal
	r.UpdateDateTime = helper.MakeDateTime(dtNow)
	if msg != "" {
		r.logLineLst = append(r.logLineLst, msg)
	}
}

// For internal use only. Must be guarded with locks.
//
// getFromCurrentLog copy [strat, count] log lines from model log.
// If count is zero or less then copy from start to the end of log.
// Return current log line count and slice of log lines.
func (rsc *RunStateCatalog) getFromCurrentLog(digest string, start int, count int) (int, int, int, []string) {

	// get model run state by digest
	r, ok := rsc.runMap[digest]
	if !ok {
		return 0, 0, 0, []string{} // model digest not found
	}

	if len(r.logLineLst) <= 0 { // log is empty
		return 0, 0, 0, []string{}
	}

	// copy log lines
	nLen := len(r.logLineLst)
	nStart := start
	if nStart < 0 {
		nStart = 0
	}
	if nStart >= nLen {
		return nStart, 0, nLen, []string{}
	}
	nCount := count
	if nCount <= 0 || nStart+nCount > nLen {
		nCount = nLen - nStart
	}

	ret := make([]string, nCount)
	copy(ret, r.logLineLst[nStart:nStart+nCount])
	return nStart, nCount, nLen, ret
}
