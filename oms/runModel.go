// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// runModel starts new model run and return unique timestamped run key
func (rsc *RunStateCatalog) runModel(modelDigest, modelName string, req *RunRequest) (*RunState, error) {

	// make model process run stamp, if not specified then use timestamp by default
	ts, dtNow := rsc.getNewTimeStamp()
	rStamp := helper.CleanSpecialChars(req.RunStamp)
	if rStamp == "" {
		rStamp = ts
	}

	// new run state
	rs := &RunState{
		ModelName:      modelName,
		ModelDigest:    modelDigest,
		RunStamp:       rStamp,
		UpdateDateTime: helper.MakeDateTime(dtNow),
	}

	// make model run command line arguments, starting from process run stamp and log options
	mArgs := []string{}
	mArgs = append(mArgs, "-OpenM.RunStamp", rStamp)
	mArgs = append(mArgs, "-OpenM.LogToConsole", "true")

	logDir, isDir := rsc.getModelLogDir()
	if isDir && logDir != "" {
		rs.logPath = filepath.Join(logDir, rs.ModelName+"."+rStamp+".log")
		mArgs = append(mArgs, "-OpenM.LogToFile", "true")
		mArgs = append(mArgs, "-OpenM.LogFilePath", rs.logPath)
	}

	// append model run options from run request, ignore any model log options
	for krq, val := range req.Opts {

		if len(krq) < 1 || len(val) < 1 { // skip empty run options
			continue
		}

		// command line argument key starts with "-" ex: "-OpenM.Threads"
		key := krq
		if krq[1] != '-' {
			key = "-" + krq
		}

		// skip any model log options
		if strings.EqualFold(key, "-OpenM.LogToConsole") ||
			strings.EqualFold(key, "-OpenM.LogToFile") ||
			strings.EqualFold(key, "-OpenM.LogFilePath") ||
			strings.EqualFold(key, "-OpenM.LogToStampedFile") ||
			strings.EqualFold(key, "-OpenM.LogUseTimeStamp") ||
			strings.EqualFold(key, "-OpenM.LogToConsole") ||
			strings.EqualFold(key, "-OpenM.LogUsePidStamp") ||
			strings.EqualFold(key, "-OpenM.LogNoMsgTime") {
			omppLog.Log("Warning: log options are not accepted: ", key)
		} else {
			mArgs = append(mArgs, key, val) // append command line argument key and value
		}
	}

	// build model run command, assume model exe name same as model name
	exeDir, _ := theCatalog.getModelDir()
	mExe := "./" + helper.CleanSpecialChars(modelName)

	cmd := exec.Command(mExe, mArgs...)

	// work directory for model exe
	if req.Dir != "" {
		cmd.Dir = req.Dir
	} else {
		cmd.Dir = exeDir
	}

	// append environment variables, if additional environment specified to run the model
	if len(req.Env) > 0 {
		env := os.Environ()
		for key, val := range req.Env {
			if key != "" && val != "" {
				env = append(env, key+"="+val)
			}
		}
		cmd.Env = env
	}

	// connect console output to log line array
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		return rs, err
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		return rs, err
	}
	outDoneC := make(chan bool, 1)
	errDoneC := make(chan bool, 1)
	logTck := time.NewTicker(logTickTimeout * time.Millisecond)

	// append console output to log lines array
	doLog := func(digest, runStamp string, r io.Reader, done chan<- bool) {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			rsc.updateRunState(digest, runStamp, false, sc.Text())
		}
		done <- true
		close(done)
	}

	// wait until model run completed
	doRunWait := func(digest, runStamp string, cmd *exec.Cmd) {

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
		e := cmd.Wait()
		if e != nil {
			omppLog.Log(e)
			rsc.updateRunState(digest, runStamp, true, e.Error())
			return
		}
		// else: completed OK
		rsc.updateRunState(digest, runStamp, true, "")
	}

	// run state initialized: save in run state list
	rsc.storeProcRunState(rs)

	// start console output listners
	go doLog(modelDigest, rs.RunStamp, outPipe, outDoneC)
	go doLog(modelDigest, rs.RunStamp, errPipe, errDoneC)

	// start the model
	err = cmd.Start()
	if err != nil {
		omppLog.Log("Model run error: ", err)
		rsc.updateRunState(modelDigest, rs.RunStamp, true, err.Error())
		rs.IsFinal = true
		return rs, err // exit with error: model failed to start
	}
	// else start model listener
	go doRunWait(modelDigest, rs.RunStamp, cmd)
	return rs, nil
}

// add new model run state into run history
func (rsc *RunStateCatalog) storeProcRunState(rs *RunState) {
	if rs == nil {
		return
	}
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rsc.runLst.PushFront(
		&procRunState{
			RunState:   *rs,
			logLineLst: make([]string, 0, 32),
		})
	if rsc.runLst.Len() > runListMaxSize {
		rsc.runLst.Remove(rsc.runLst.Back())
	}
}

// updateRunState does model run state update and append to model log lines array
func (rsc *RunStateCatalog) updateRunState(digest, runStamp string, isFinal bool, msg string) {
	if digest == "" || runStamp == "" {
		return // model run undefined
	}
	dtNow := time.Now()

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	// update model run state and append log message
	for re := rsc.runLst.Front(); re != nil; re = re.Next() {

		rs, ok := re.Value.(*procRunState)
		if !ok || rs == nil {
			continue
		}
		if rs.ModelDigest != digest || rs.RunStamp != runStamp {
			continue
		}
		// run state found

		// update run state and append new log line if not empty
		rs.IsFinal = isFinal
		rs.UpdateDateTime = helper.MakeDateTime(dtNow)
		if msg != "" {
			rs.logLineLst = append(rs.logLineLst, msg)
		}
		break
	}
}

// get current run status and page of log lines
func (rsc *RunStateCatalog) readModelLastRunLog(digest, runStamp string, start int, count int) (*RunStateLogPage, error) {

	lrp := &RunStateLogPage{
		Lines: []string{},
	}
	if digest == "" || runStamp == "" {
		return lrp, nil // empty model digest: exit
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	// return model run state and page from model log
	for re := rsc.runLst.Front(); re != nil; re = re.Next() {

		rs, ok := re.Value.(*procRunState)
		if !ok || rs == nil {
			continue
		}
		if rs.ModelDigest != digest || rs.RunStamp != runStamp {
			continue
		}
		// run state found

		// return model run state and page from model log
		lrp.RunState = rs.RunState
		if len(rs.logLineLst) <= 0 { // log is empty
			break
		}

		// copy log lines
		lrp.TotalSize = len(rs.logLineLst)
		lrp.Offset = start
		if lrp.Offset < 0 {
			lrp.Offset = 0
		}
		if lrp.Offset >= lrp.TotalSize {
			break
		}
		lrp.Size = count
		if lrp.Size <= 0 || lrp.Offset+lrp.Size > lrp.TotalSize {
			lrp.Size = lrp.TotalSize - lrp.Offset
		}

		lrp.Lines = make([]string, lrp.Size)
		copy(lrp.Lines, rs.logLineLst[lrp.Offset:lrp.Offset+lrp.Size])
		break
	}
	return lrp, nil
}
