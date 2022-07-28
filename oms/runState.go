// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"os"
	"runtime"
	"time"

	"github.com/openmpp/go/ompp/helper"
)

// find model run state by model digest and submission stamp, if not found then return false and empty RunState
func (rsc *RunCatalog) getRunStateBySubmitStamp(digest, stamp string) (bool, RunState) {
	if digest == "" || stamp == "" {
		return false, RunState{}
	}
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state
	var rsl *runStateLog
	var ok bool
	for re := rsc.runLst.Front(); re != nil; re = re.Next() {

		rsl, ok = re.Value.(*runStateLog)
		if !ok || rsl == nil {
			continue
		}
		ok = rsl.ModelDigest == digest && rsl.SubmitStamp == stamp
		if ok {
			return true, rsl.RunState
		}
	}
	// model run state not found
	return false, RunState{}
}

// add new model run state into run state list and create model run log file
func (rsc *RunCatalog) createRunStateLog(rState *RunState) {
	if rState == nil {
		return
	}

	// create log file or truncate existing
	if rState.IsLog {
		f, err := os.Create(rState.logPath)
		if err != nil {
			rState.IsLog = false
			return
		}
		defer f.Close()
	}

	// add to run status list
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rsc.runLst.PushFront(
		&runStateLog{
			RunState:   *rState,
			logLineLst: make([]string, 0, 128),
		})
}

// updateRunStateProcess set process info if isFinal is false or clear it if isFinal is true
func (rsc *RunCatalog) updateRunStateProcess(rState *RunState, isFinal bool) {
	if rState == nil {
		return
	}
	dtNow := time.Now()

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	// update model run state and append log message
	var rs *runStateLog
	var ok bool
	for re := rsc.runLst.Front(); re != nil; re = re.Next() {

		rs, ok = re.Value.(*runStateLog)
		if !ok || rs == nil {
			continue
		}
		ok = rs.ModelDigest == rState.ModelDigest && rs.RunStamp == rState.RunStamp
		if ok {
			break
		}
	}
	// if model run state not found: add new run state
	if !ok || rs == nil {
		rs = &runStateLog{
			RunState:   *rState,
			logLineLst: make([]string, 0, 128),
		}
		rsc.runLst.PushFront(rs)
	}

	// update run state and set or clear process info
	rs.UpdateDateTime = helper.MakeDateTime(dtNow)
	rs.IsFinal = isFinal

	if isFinal {
		rs.killC = nil
	} else {
		rs.pid = rState.pid
	}
	if rState.cmdPath != "" {
		rs.cmdPath = rState.cmdPath
	}
}

// updateRunStateLog does model run state update and append to model log lines array
func (rsc *RunCatalog) updateRunStateLog(rState *RunState, isFinal bool, msg string) {
	if rState == nil {
		return
	}
	dtNow := time.Now()

	// write into model console log file
	if rState.IsLog && msg != "" {

		f, err := os.OpenFile(rState.logPath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			rState.IsLog = false
		} else {
			defer f.Close()

			_, err = f.WriteString(msg)
			if err == nil {
				if runtime.GOOS == "windows" { // adjust newline for windows
					_, err = f.WriteString("\r\n")
				} else {
					_, err = f.WriteString("\n")
				}
			}
			if err != nil {
				rState.IsLog = false
			}
		}
	}

	// update run state list
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	// update model run state and append log message
	var rs *runStateLog
	var ok bool
	for re := rsc.runLst.Front(); re != nil; re = re.Next() {

		rs, ok = re.Value.(*runStateLog)
		if !ok || rs == nil {
			continue
		}
		ok = rs.ModelDigest == rState.ModelDigest && rs.RunStamp == rState.RunStamp
		if ok {
			break
		}
	}
	// if model run state not found: add new run state
	if !ok || rs == nil {
		rs = &runStateLog{
			RunState:   *rState,
			logLineLst: make([]string, 0, 128),
		}
		rsc.runLst.PushFront(rs)
	}

	// update run state and append new log line if not empty
	rs.UpdateDateTime = helper.MakeDateTime(dtNow)
	rs.IsFinal = isFinal
	if isFinal {
		rs.killC = nil
	}

	rs.IsLog = rState.IsLog // it can be updated by write log error
	if msg != "" {
		rs.logLineLst = append(rs.logLineLst, msg)
	}
}
