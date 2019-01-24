// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
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

	// set directories: work directory, bin model.exe directory, log directory
	// if any of those is relative then it must be reletive to oms root directory
	// re-base it to model work directory
	binRoot, _ := theCatalog.getModelDir()

	wDir := req.Dir
	if wDir == "" || wDir == "." || wDir == "./" {
		wDir = binRoot
	}

	binDir, ok := theCatalog.binDirectoryByDigestOrName(modelDigest)
	if !ok || binDir == "" || binDir == "." || binDir == "./" {
		binDir = binRoot
	}
	binDir, err := filepath.Rel(wDir, binDir)
	if err != nil {
		binDir = binRoot
	}

	logDir, isLog := rsc.getModelLogDir()
	if isLog && logDir != "" && !filepath.IsAbs(logDir) {
		if logDir, err = filepath.Rel(wDir, logDir); err != nil {
			isLog = false
		}
	}

	// make model run command line arguments, starting from process run stamp and log options
	mArgs := []string{}
	mArgs = append(mArgs, "-OpenM.RunStamp", rStamp)
	mArgs = append(mArgs, "-OpenM.LogToConsole", "true")

	if isLog && logDir != "" {
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
		if krq[0] != '-' {
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

	// make model exe or use mpi run exe, assume model exe name same as model name
	mExe := helper.CleanSpecialChars(modelName)
	if req.Use64Suffix {
		mExe += "64"
	}
	if binDir == "" || binDir == "." || binDir == "./" {
		mExe = "./" + mExe
	} else {
		mExe = filepath.Join(binDir, mExe)
	}

	var cmd *exec.Cmd

	if !req.IsMpi { // run command line model.exe on local host

		// build model run command and set working directory
		cmd = exec.Command(mExe, mArgs...)
		cmd.Dir = wDir

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

	} else { // use MPI to run multiple model.exe on cluster

		// use mpiexec to run model exe, similar to:
		// mpiexec -wdir models/bin -n 16 -host omm,om1,om2  -x   key1=val1 model_mpi.exe ...arguments
		// mpiexec -wdir models/bin -n 16 -host omm,om1,om2  -env key1 val1 model_mpi.exe ...arguments
		// mpiexec -wdir models/bin -n 16 -machinefile afile -env key1 val1 model_mpi.exe ...arguments

		cArgs := []string{}
		if wDir != "" && wDir != "." && wDir != "./" {
			cArgs = append(cArgs, "-wdir", wDir)
		}
		if req.Mpi.Np > 0 {
			cArgs = append(cArgs, "-n", strconv.Itoa(req.Mpi.Np))
		}
		if req.Mpi.Host != "" {
			cArgs = append(cArgs, "-host", req.Mpi.Host)
		}
		if req.Mpi.MachineFile != "" {
			cArgs = append(cArgs, "-machinefile", req.Mpi.MachineFile)
		}

		// append environment variables, if additional environment specified to run the model
		if len(req.Env) > 0 {
			if runtime.GOOS == "windows" { // MS MPI on Windows: mpiexec -env k1 v1 -env k2 v2 model.exe
				for key, val := range req.Env {
					if key != "" && val != "" {
						cArgs = append(cArgs, "-env", key, val)
					}
				}
			} else { // Open MPI on Linux: mpiexec -x k1=v1 -x k2=v2 model.exe
				for key, val := range req.Env {
					if key != "" && val != "" {
						cArgs = append(cArgs, "-x", key+"="+val)
					}
				}
			}
		}

		// append model name and model run options to mpiexec command line
		mExe += "_mpi"
		cArgs = append(cArgs, mExe)
		cArgs = append(cArgs, mArgs...)

		// build model run command
		cmd = exec.Command("mpiexec", cArgs...)
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
	omppLog.Log("Starting ", mExe, " in ", wDir)

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
