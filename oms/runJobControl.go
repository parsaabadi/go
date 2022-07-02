// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"

	ps "github.com/keybase/go-ps"
)

const jobScanInterval = 1123       // timeout in msec, sleep interval between scanning all job directories
const jobActiveScanInterval = 5021 // timeout in msec, sleep interval between scanning active job directory

// jobDirValid checking job control configuration.
// if job control directory is empty then job control disabled.
// if job control directory not empty then it must have active, queue and history subdirectories.
// if state.json exists then it must be a valid configuration file.
func jobDirValid(jobDir string) error {

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
	return filepath.Join(theCfg.jobDir, "queue", submitStamp+"{"+theCfg.omsName+"}"+modelName+"."+modelDigest+".json")
}

// retrun job control file path if model is running now
func jobActivePath(submitStamp, modelName, modelDigest string, pid int) string {
	return filepath.Join(theCfg.jobDir, "active", submitStamp+"{"+theCfg.omsName+"}"+modelName+"."+modelDigest+"."+strconv.Itoa(pid)+".json")
}

// retrun job control file path to completed model with run status suffix, e.g.: .success. or .error.
func jobHistoryPath(status, submitStamp, modelName, modelDigest, runStamp string) string {
	return filepath.Join(
		theCfg.jobDir,
		"history",
		submitStamp+"{"+theCfg.omsName+"}"+modelName+"."+modelDigest+"{"+runStamp+"}"+db.NameOfRunStatus(status)+".json")
}

// parse job file file path or job file name:
// remove .json extension and directory prefix
// return submission stamp, oms instance name and the rest of the file name
func parseJobPath(srcPath string) (string, string, string) {

	// remove job directory and extension, file extension must be .json
	if filepath.Ext(srcPath) != ".json" {
		return "", "", ""
	}
	p := filepath.Base(srcPath)
	p = p[:len(p)-len(".json")]

	// check job file name length and prefix: submission stamp and oms server name
	n1 := strings.Index(p, "{")
	n2 := strings.Index(p, "}")
	if len(p) < helper.TimeStampLength+len("{s}") || n1 < helper.TimeStampLength || n2 < n1+2 || n2 >= len(p)-1 {
		return "", "", "" // source file path is not job file: name is too short
	}

	// retrun submission stamp, oms instance name and the rest of file name
	return p[:helper.TimeStampLength], p[n1+1 : n2], p[n2+1:]
}

// parse history file path or history file name and
// return submission stamp, oms instance name, model name, digest, run stamp and run status
func parseHistoryPath(srcPath string) (string, string, string, string, string, string) {

	// parse common job file part and get get sumbmission stamp and oms instance name
	subStamp, oms, p := parseJobPath(srcPath)

	if subStamp == "" || oms == "" || p == "" || len(p) < len("m.d{r}s") {
		return subStamp, oms, "", "", "", "" // source file path is not history job file
	}

	// get status and run stamp
	n1 := strings.LastIndex(p, "{")
	n2 := strings.LastIndex(p, "}")
	if n1 < len("m.d") || n2 < n1+len("{r") || n2 >= len(p)-1 {
		return subStamp, oms, "", "", "", "" // source file path is not history job file
	}
	rStamp := p[n1+1 : n2]
	status := p[n2+1:]
	p = p[:n1]

	// split the rest by model name and digest
	nd := strings.LastIndex(p, ".")
	if nd < 1 || nd >= len(p)-1 {
		return subStamp, oms, "", "", "", "" // source file path is not history job file
	}

	return subStamp, oms, p[:nd], p[nd+1:], rStamp, status
}

// parse queue file path or queue file name and return submission stamp, oms instance name, model name and digest
func parseQueuePath(srcPath string) (string, string, string, string) {

	// parse common job file part and get get sumbmission stamp and oms instance name
	stamp, oms, p := parseJobPath(srcPath)

	if stamp == "" || oms == "" || p == "" || len(p) < len("m.d") {
		return stamp, oms, "", "" // source file path is not queue job file
	}

	// split the rest by model name and digest
	nd := strings.LastIndex(p, ".")
	if nd < 1 || nd >= len(p)-1 {
		return stamp, oms, "", "" // source file path is not queue job file
	}

	return stamp, oms, p[:nd], p[nd+1:]
}

// parse active file path or active file name and return submission stamp, oms instance name, model name, digest and process id
func parseActivePath(srcPath string) (string, string, string, string, int) {

	// parse common job file part and get get sumbmission stamp and oms instance name
	stamp, oms, p := parseJobPath(srcPath)

	if stamp == "" || oms == "" || p == "" || len(p) < len("m.d.1") {
		return stamp, oms, "", "", 0 // source file path is not active job file
	}

	// file name ends with .pid, convert process id
	np := strings.LastIndex(p, ".")
	if np < len("m.d.") || np >= len(p)-1 {
		return stamp, oms, "", "", 0 // pid not found
	}
	pid, err := strconv.Atoi(p[np+1:])
	if err != nil || pid <= 0 {
		return stamp, oms, "", "", 0 // pid must be positive integer
	}
	p = p[:np]

	// split the rest by model name and digest
	nd := strings.LastIndex(p, ".")
	if nd < 1 || nd >= len(p)-1 {
		return stamp, oms, "", "", 0 // source file path is not active job file
	}

	return stamp, oms, p[:nd], p[nd+1:], pid
}

// add new run request to job queue
func addJobToQueue(stamp string, req *RunRequest) error {
	if !theCfg.isJobControl {
		return nil // job control disabled
	}

	fn := jobQueuePath(stamp, req.ModelName, req.ModelDigest)

	err := helper.ToJsonIndentFile(fn,
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
func moveJobToActive(rState *RunState, runStamp string) bool {
	if !theCfg.isJobControl || rState == nil {
		return true // job control disabled
	}

	// read run request from job queue
	src := jobQueuePath(rState.SubmitStamp, rState.ModelName, rState.ModelDigest)

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
	jc.Pid = rState.pid
	jc.CmdPath = rState.cmdPath
	jc.LogFileName = rState.LogFileName

	dst := jobActivePath(rState.SubmitStamp, rState.ModelName, rState.ModelDigest, rState.pid)

	fileDleteAndLog(false, src) // remove job control file from queue

	err = helper.ToJsonIndentFile(dst, &jc)
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
	ptrn := activeDir + string(filepath.Separator) + "*{" + theCfg.omsName + "}*.json"

	for {
		// find active job files
		fLst := filesByPattern(ptrn, "Error at active job files search")
		if len(fLst) <= 0 {
			if doExitSleep(jobActiveScanInterval, doneC) {
				return
			}
			continue // no active jobs
		}

		// find new active jobs since last scan which do not exist in run state list of RunCatalog
		for k := range fLst {
			fn := fLst[k][nActive+1:] // remove directory prefix and / separator

			if _, ok := outerJobs[fn]; ok {
				continue // this file already in the outer jobs list
			}

			// get submission stamp, model name, digest and process id from active job file name
			stamp, _, mName, mDgst, pid := parseActivePath(fn)
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
		if doExitSleep(jobActiveScanInterval, doneC) {
			return
		}
	}
}

// scan job control directories to read and update job lists: queue, active and history
func scanJobs(doneC <-chan bool) {
	if !theCfg.isJobControl {
		return // job control disabled
	}

	queuePtrn := filepath.Join(theCfg.jobDir, "queue") + string(filepath.Separator) + "*{" + theCfg.omsName + "}*.json"
	activePtrn := filepath.Join(theCfg.jobDir, "active") + string(filepath.Separator) + "*{" + theCfg.omsName + "}*.json"
	historyPtrn := filepath.Join(theCfg.jobDir, "history") + string(filepath.Separator) + "*{" + theCfg.omsName + "}*.json"

	// map job file key (submission stamp and oms instance name) to file content (run job)
	toJobMap := func(fLst []string, jobMap map[string]runJobFile) []string {

		jKeys := make([]string, 0, len(fLst)) // list of jobs key

		for _, f := range fLst {

			// get submission stamp and oms instance
			stamp, oms, _ := parseJobPath(f)
			if stamp == "" || oms == "" {
				continue // file name is not a job file name
			}
			jobKey := stamp + "{" + oms + "}"
			jKeys = append(jKeys, jobKey)

			if _, ok := jobMap[jobKey]; ok {
				continue // this file already in the jobs list
			}

			// create run state from job file
			var jc RunJob
			isOk, err := helper.FromJsonFile(f, &jc)
			if err != nil {
				omppLog.Log(err)
				jobMap[jobKey] = runJobFile{omsName: oms, filePath: f, isError: true}
			}
			if !isOk || err != nil {
				continue // file not exist or invalid
			}

			jobMap[jobKey] = runJobFile{RunJob: jc, omsName: oms, filePath: f} // add job into jobs list
		}
		return jKeys
	}

	queueJobs := map[string]runJobFile{}
	activeJobs := map[string]runJobFile{}
	historyJobs := map[string]historyJobFile{}

	for {
		queueFiles := filesByPattern(queuePtrn, "Error at queue job files search")
		activeFiles := filesByPattern(activePtrn, "Error at active job files search")
		historyFiles := filesByPattern(historyPtrn, "Error at history job files search")

		qKeys := toJobMap(queueFiles, queueJobs)
		aKeys := toJobMap(activeFiles, activeJobs)

		// parse history files list
		hKeys := make([]string, 0, len(historyFiles))

		for _, f := range historyFiles {

			// get submission stamp and oms instance
			subStamp, oms, mn, dgst, rStamp, status := parseHistoryPath(f)
			if subStamp == "" || oms == "" {
				continue // file name is not a job file name
			}
			jobKey := subStamp + "{" + oms + "}"
			hKeys = append(hKeys, jobKey)

			if _, ok := historyJobs[jobKey]; ok {
				continue // this file already in the history jobs list
			}

			// add job into history jobs list
			historyJobs[jobKey] = historyJobFile{
				omsName:     oms,
				filePath:    f,
				isError:     (mn == "" || dgst == "" || rStamp == "" || status == ""),
				SubmitStamp: subStamp,
				ModelName:   mn,
				ModelDigest: dgst,
				RunStamp:    rStamp,
				Status:      status,
			}
		}

		// remove from map queue files or active files which are in history
		// remove from map queue files which are in active
		for jobKey := range historyJobs {
			delete(queueJobs, jobKey)
			delete(activeJobs, jobKey)
		}
		for jobKey := range activeJobs {
			delete(queueJobs, jobKey)
		}

		// remove entries from job maps where files no longer exist
		sort.Strings(qKeys)
		for jobKey := range queueJobs {
			k := sort.SearchStrings(qKeys, jobKey)
			if k < 0 || k >= len(qKeys) || qKeys[k] != jobKey {
				delete(queueJobs, jobKey)
			}
		}
		sort.Strings(aKeys)
		for jobKey := range activeJobs {
			k := sort.SearchStrings(aKeys, jobKey)
			if k < 0 || k >= len(aKeys) || aKeys[k] != jobKey {
				delete(activeJobs, jobKey)
			}
		}
		sort.Strings(hKeys)
		for jobKey := range historyJobs {
			k := sort.SearchStrings(hKeys, jobKey)
			if k < 0 || k >= len(hKeys) || hKeys[k] != jobKey {
				delete(historyJobs, jobKey)
			}
		}

		// update run catalog with current job control files
		theRunCatalog.updateRunJobs(queueJobs, activeJobs, historyJobs)

		// wait for doneC or sleep
		if doExitSleep(jobScanInterval, doneC) {
			return
		}
	}
}
