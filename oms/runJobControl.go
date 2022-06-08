// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"

	ps "github.com/keybase/go-ps"
)

// RunJob is model run request and run job control: submission stamp and model process id
type RunJob struct {
	SubmitStamp string // submission timestamp
	Pid         int    // process id
	CmdPath     string // executable path
	RunRequest         // model run request: model name, digest and run options
}

// timeout in msec, sleep interval between scanning job directory
const jobScanInterval = 5021

// isJobDirValid checking job control configuration.
// if job control directory is empty then job control disabled.
// if job control directory not empty then it must have active, queue and history subdirectories.
// if state.json exists then it must be a valid configuration file.
func isJobDirValid(jobDir string) error {

	if jobDir == "" {
		return nil // job control disabled
	}

	if err := dirExist(jobDir); err != nil {
		return err
	}
	if err := dirExist(filepath.Join(jobDir, "active")); err != nil {
		return err
	}
	if err := dirExist(filepath.Join(jobDir, "queue")); err != nil {
		return err
	}
	if err := dirExist(filepath.Join(jobDir, "history")); err != nil {
		return err
	}
	return nil
}

// retrun path job control file path if model run standing is queue
func jobQueuePath(submitStamp, modelName, modelDigest string) string {
	return filepath.Join(theCfg.jobDir, "queue", submitStamp+"."+theCfg.omsName+"."+modelName+"."+modelDigest+".json")
}

// retrun job control file path if model is running now
func jobActivePath(submitStamp, modelName, modelDigest string, pid int) string {
	return filepath.Join(theCfg.jobDir, "active", submitStamp+"."+theCfg.omsName+"."+modelName+"."+modelDigest+"."+strconv.Itoa(pid)+".json")
}

// retrun job control file path to completed model with run status suffix, e.g.: .success. or .error.
func jobHistoryPath(status, submitStamp, modelName, modelDigest, runStamp string) string {
	return filepath.Join(
		theCfg.jobDir,
		"history",
		submitStamp+"."+theCfg.omsName+"."+modelName+"."+modelDigest+"."+runStamp+"."+db.NameOfRunStatus(status)+".json")
}

// parse active file path or active file name and return submission stamp, model name, digest and process id
func parseActivePath(srcPath string) (string, string, string, int) {

	// source file extension must be .json
	if !strings.HasSuffix(srcPath, ".json") {
		return "", "", "", 0
	}
	p := srcPath[:len(srcPath)-len(".json")]

	// trim job/dir/active/ prefix if path starts with that prefix
	n1 := len(theCfg.jobDir)
	n2 := len(theCfg.jobDir) + 1 + len("active")
	if len(p) > n2 &&
		strings.HasPrefix(p, theCfg.jobDir) && (p[n1] == '/' || p[n1] == '\\') &&
		p[n1+1:n2] == "active" && (p[n2] == '/' || p[n2] == '\\') {
		p = p[n2+1:]
	}

	// check active job file name length and prefix: submission stamp and oms server name
	sn := "." + theCfg.omsName + "."
	nts := helper.TimeStampLength + len(sn)

	if len(p) < nts+len("m.d.1") {
		return "", "", "", 0 // source file path is not active job file: name is too short
	}
	if p[helper.TimeStampLength:nts] != sn {
		return "", "", "", 0 // source file path is not active job file: incorrect file name
	}

	// get submission stamp and process id
	stamp := p[:helper.TimeStampLength]

	// file name ends with .pid, convert process id
	np := strings.LastIndex(p, ".")
	if np < nts || np >= len(p)-1 {
		return "", "", "", 0 // pid not found
	}
	pid, err := strconv.Atoi(p[np+1:])
	if err != nil || pid <= 0 {
		return "", "", "", 0 // pid must be positive integer
	}

	// get model name and model digest parts of file name
	p = p[nts:np]
	nd := strings.LastIndex(p, ".")
	if nd < 1 || nd >= len(p)-1 {
		return "", "", "", 0 // model digest not found
	}
	mn := p[:nd]
	dgst := p[nd+1:]

	return stamp, mn, dgst, pid
}

// add new run request to job queue
func addJobToQueue(stamp string, req *RunRequest) error {
	if !theCfg.isJobControl {
		return nil // job control disabled
	}

	fn := jobQueuePath(stamp, req.ModelName, req.ModelDigest)

	err := helper.ToJsonFile(fn,
		&RunJob{
			SubmitStamp: stamp,
			RunRequest:  *req,
		})
	if err != nil {
		omppLog.Log(err)
		fileDleteAndLog(true, fn) // on error remove file, if any file created
		return err
	}
	return nil
}

// move run job to active state from queue
func moveJobToActive(submitStamp, modelName, modelDigest, runStamp string, pid int, cmdPath string) bool {
	if !theCfg.isJobControl {
		return true // job control disabled
	}

	// read run request from job queue
	src := jobQueuePath(submitStamp, modelName, modelDigest)

	var jc RunJob
	isOk, err := helper.FromJsonFile(src, &jc)
	if err != nil {
		omppLog.Log(err)
	}
	if !isOk || err != nil {
		fileDleteAndLog(true, src) // invalid file content: remove job control file from queue
		return false
	}

	// add run stamp, process info and move job control file into active
	jc.RunStamp = runStamp
	jc.Pid = pid
	jc.CmdPath = cmdPath

	dst := jobActivePath(submitStamp, modelName, modelDigest, pid)

	fileDleteAndLog(false, src) // remove job control file from queue

	err = helper.ToJsonFile(dst, &jc)
	if err != nil {
		omppLog.Log(err)
		fileDleteAndLog(true, dst) // on error remove file, if any file created
		return false
	}
	return true
}

// move active model run job control file to history
func moveJobToHistory(status, submitStamp, modelName, modelDigest, runStamp string, pid int) bool {
	if !theCfg.isJobControl {
		return true // job control disabled
	}

	src := jobActivePath(submitStamp, modelName, modelDigest, pid)
	dst := jobHistoryPath(status, submitStamp, modelName, modelDigest, runStamp)

	if !fileMoveAndLog(false, src, dst) {
		fileDleteAndLog(true, src) // if move failed then delete job control file from active list
		return false
	}
	return true
}

// move outer model run job control file to history
func moveOuterJobToHistory(srcPath string, status, submitStamp, modelName, modelDigest, runStamp string) bool {
	if !theCfg.isJobControl {
		return true // job control disabled
	}

	dst := jobHistoryPath(status, submitStamp, modelName, modelDigest, runStamp)

	if !fileMoveAndLog(true, srcPath, dst) {
		fileDleteAndLog(true, srcPath) // if move failed then delete job control file from active list
		return false
	}
	return true
}

// move model run request from queue to error if model run fail to start
func moveJobQueueToFailed(submitStamp, modelName, modelDigest string) bool {
	if !theCfg.isJobControl {
		return true // job control disabled
	}

	src := jobQueuePath(submitStamp, modelName, modelDigest)
	dst := jobHistoryPath(db.ErrorRunStatus, submitStamp, modelName, modelDigest, "no-run-time-stamp")

	if !fileMoveAndLog(false, src, dst) {
		fileDleteAndLog(true, src) // if move failed then delete job control file from queue
		return false
	}
	return true
}

// find model run state by model digest and submission stamp
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
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rsc.runLst.PushFront(
		&runStateLog{
			RunState:   *rState,
			logLineLst: make([]string, 0, 128),
		})
	for rsc.runLst.Len() > theCfg.runHistoryMaxSize { // remove old run state from history
		rsc.runLst.Remove(rsc.runLst.Back())
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
}

// updateRunStateProcess set process info if isFinal is false or clear it if isFinal is true
func (rsc *RunCatalog) updateRunStateProcess(rState *RunState, isFinal bool, cmdPath string, pid int, killC chan bool) {
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
	if !ok || rs == nil {
		return // model run state not found
	}

	// update run state and set or clear process info
	rs.UpdateDateTime = helper.MakeDateTime(dtNow)
	rs.IsFinal = isFinal

	if cmdPath != "" {
		rs.cmdPath = cmdPath
		rState.cmdPath = cmdPath
	}
	if isFinal {
		rs.killC = nil
	} else {
		rs.killC = killC
		rs.pid = pid
		rState.pid = pid
	}
}

// updateRunStateLog does model run state update and append to model log lines array
func (rsc *RunCatalog) updateRunStateLog(rState *RunState, isFinal bool, msg string) {
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

	// update run state and append new log line if not empty
	rs.UpdateDateTime = helper.MakeDateTime(dtNow)
	rs.IsFinal = isFinal
	if isFinal {
		rs.killC = nil
	}
	if msg != "" {
		rs.logLineLst = append(rs.logLineLst, msg)
	}

	// write into model console log file
	if rs.IsLog && msg != "" {

		f, err := os.OpenFile(rs.logPath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			rs.IsLog = false
			return
		}
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
			rs.IsLog = false
		}
	}
}

// stopModelRun kill model run by run stamp or
// remove run request from the queue by submit stamp or by run stamp
func (rsc *RunCatalog) stopModelRun(modelDigest string, stamp string) bool {

	dtNow := time.Now()

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	// update model run state and append log message
	var rs *runStateLog
	var rsSubmit *runStateLog
	var ok bool
	for re := rsc.runLst.Front(); re != nil; re = re.Next() {

		rs, ok = re.Value.(*runStateLog)
		if !ok || rs == nil {
			continue
		}
		ok = rs.ModelDigest == modelDigest && rs.RunStamp == stamp
		if ok {
			break
		}
		ok = rs.ModelDigest == modelDigest && rs.SubmitStamp == stamp
		if ok {
			rsSubmit = rs
		}
	}
	// if model run stamp not found then check if submit stamp found
	if !ok || rs == nil {
		if rsSubmit == nil {
			return false // no model run stamp and no submit stamp found
		}
		rs = rsSubmit // submit stamp found
	}
	rs.UpdateDateTime = helper.MakeDateTime(dtNow)

	// kill model run if model is running
	if rs.killC != nil {
		rs.killC <- true
		return true
	}
	// else remove request from the queue
	rs.IsFinal = true
	moveJobQueueToFailed(rs.SubmitStamp, rs.ModelName, rs.ModelDigest)

	return true
}

/*
scan active job directory to find active model run files without run state.
It can be a result of oms restart or server reboot.

if active job file found and no run state then
  create run job from active file
  add it to the list of "outer" jobs (active jobs without run state)

for each job in the outer list
  find model process by pid and executable name
  if process exist then wait until it done
  check if file still exist
  read run_lst row
  if no run_lst row then move job file to history as error
  else
    if run state is not completed then update run state as error
    and move file to history according to status
*/
func scanActiveJobs(doneC <-chan bool) {
	if !theCfg.isJobControl {
		return // job control disabled
	}

	// map active job file path to file content (run job), it is only job where no run state in RunCatalog
	outerJobs := map[string]RunJob{}

	activeDir := filepath.Join(theCfg.jobDir, "active")
	nActive := len(activeDir)
	ptrn := activeDir + string(filepath.Separator) + "*." + theCfg.omsName + ".*.json"

	// wait for doneC or sleep
	doExitSleep := func() bool {
		select {
		case <-doneC:
			return true
		case <-time.After(jobScanInterval * time.Millisecond):
		}
		return false
	}

	for {
		// find active job files
		fLst, err := filepath.Glob(ptrn)
		if err != nil {
			omppLog.Log("Error at active job files search: ", ptrn)
			if doExitSleep() {
				return
			}
			continue
		}
		if len(fLst) <= 0 {
			if doExitSleep() {
				return
			}
			continue // no active jobs for that model
		}

		// find new active jobs since last scan which do not exist in run state list of RunCatalog
		for k := range fLst {
			fn := fLst[k][nActive+1:] // remove directory prefix and / separator

			if _, ok := outerJobs[fn]; ok {
				continue // this file already in the outer jobs list
			}

			// get submission stamp, model digest and process id from active job file name
			stamp, mName, mDgst, pid := parseActivePath(fn)
			if stamp == "" || mName == "" || mDgst == "" || pid <= 0 {
				continue // file name is not an active job file name
			}

			// find run state by model digest and submission stamp
			isFound, _ := theRunCatalog.getRunStateBySubmitStamp(mDgst, stamp)
			if isFound {
				continue // this is an active job under oms control
			}

			// run state not found: create run state from active job file
			var jc RunJob
			isOk, err := helper.FromJsonFile(fLst[k], &jc)
			if err != nil {
				omppLog.Log(err)
			}
			if !isOk || err != nil {
				moveOuterJobToHistory(fLst[k], "", stamp, mName, mDgst, "no-model-run-time-stamp") // invalid file content: move to history with unknown status
				continue
			}

			// add job into outer jobs list
			outerJobs[fLst[k]] = jc
		}

		// for outer jobs find process by pid and executable name
		// if process completed then move job file into the history
		for fp, jc := range outerJobs {

			proc, err := ps.FindProcess(jc.Pid)

			if err == nil && proc != nil &&
				strings.HasSuffix(strings.ToLower(jc.CmdPath), strings.ToLower(proc.Executable())) {
				continue // model still running
			}

			// check if job file not exist then remove it from the outer job list
			if fileExist(fp) != nil {
				delete(outerJobs, fp)
				continue
			}

			// get run_lst row and move to jib history according to status
			// model process does not exist, run status must completed: s=success, x=exit, e=error
			// if model status is not completed then it is an error
			var rStat string
			rp, ok := theCatalog.RunStatus(jc.ModelDigest, jc.RunStamp)
			if ok && rp != nil {
				rStat = rp.Status
				if !db.IsRunCompleted(rStat) {
					rStat = db.ErrorRunStatus
					_, e := theCatalog.UpdateRunStatus(jc.ModelDigest, jc.RunStamp, db.ErrorRunStatus)
					if e != nil {
						omppLog.Log(e)
					}
				}
			}
			moveOuterJobToHistory(fp, rStat, jc.SubmitStamp, jc.ModelName, jc.ModelDigest, jc.RunStamp)
			delete(outerJobs, fp)
		}

		// wait for doneC or sleep
		if doExitSleep() {
			return
		}
	}
}
