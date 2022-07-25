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

const jobScanInterval = 1123      // timeout in msec, sleep interval between scanning all job directories
const jobQueueScanInterval = 107  // timeout in msec, sleep interval between getting next job from the queue
const jobOuterScanInterval = 5021 // timeout in msec, sleep interval between scanning active job directory

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
	if err := dirExist(filepath.Join(jobDir, "state")); err != nil {
		return err
	}
	return nil
}

// return job control file path if model is running now, e.g.: 2022_07_08_23_03_27_555-#-_4040-#-RiskPaths-#-d90e1e9a-#-8888.json
func jobActivePath(submitStamp, modelName, modelDigest string, pid int) string {
	return filepath.Join(theCfg.jobDir, "active", submitStamp+"-#-"+theCfg.omsName+"-#-"+modelName+"-#-"+modelDigest+"-#-"+strconv.Itoa(pid)+".json")
}

// return path job control file path if model run standing is queue, e.g.: 2022_07_05_19_55_38_111-#-_4040-#-RiskPaths-#-d90e1e9a.json
func jobQueuePath(submitStamp, modelName, modelDigest string) string {
	return filepath.Join(theCfg.jobDir, "queue", submitStamp+"-#-"+theCfg.omsName+"-#-"+modelName+"-#-"+modelDigest+".json")
}

// return job control file path to completed model with run status suffix.
// For example: 2022_07_04_20_06_10_817-#-_4040-#-RiskPaths-#-d90e1e9a-#-2022_07_04_20_06_10_818-#-success.json
func jobHistoryPath(status, submitStamp, modelName, modelDigest, runStamp string) string {
	return filepath.Join(
		theCfg.jobDir,
		"history",
		submitStamp+"-#-"+theCfg.omsName+"-#-"+modelName+"-#-"+modelDigest+"-#-"+runStamp+"-#-"+db.NameOfRunStatus(status)+".json")
}

// return job state file path e.g.: /job/state/_4040.json
func jobStatePath() string {
	return filepath.Join(theCfg.jobDir, "state", theCfg.omsName+".json")
}

// return job queue paused file path e.g.: /job/state/jobs.queue.paused
func jobPausedPath() string {
	return filepath.Join(theCfg.jobDir, "state", "jobs.queue.paused")
}

// parse job file file path or job file name:
// remove .json extension and directory prefix
// return submission stamp, oms instance name, model name, model digest and the rest of the file name
func parseJobPath(srcPath string) (string, string, string, string, string) {

	// remove job directory and extension, file extension must be .json
	if filepath.Ext(srcPath) != ".json" {
		return "", "", "", "", ""
	}
	p := filepath.Base(srcPath)
	p = p[:len(p)-len(".json")]

	// check result: it must be at least 4 non-empty parts and first must be a time stamp
	sp := strings.SplitN(p, "-#-", 5)
	if len(sp) < 4 || !helper.IsUnderscoreTimeStamp(sp[0]) || sp[1] == "" || sp[2] == "" || sp[3] == "" {
		return "", "", "", "", "" // source file path is not job file
	}

	if len(sp) == 4 {
		return sp[0], sp[1], sp[2], sp[3], "" // only 4 parts, the rest of source file name is empty
	}
	return sp[0], sp[1], sp[2], sp[3], sp[4]
}

// parse active file path or active file name and return submission stamp, oms instance name, model name, digest and process id.
// For example: 2022_07_08_23_03_27_555-#-_4040-#-RiskPaths-#-d90e1e9a-#-8888.json
func parseActivePath(srcPath string) (string, string, string, string, int) {

	// parse common job file part
	subStamp, oms, mn, dgst, p := parseJobPath(srcPath)

	if subStamp == "" || oms == "" || mn == "" || dgst == "" || p == "" {
		return subStamp, oms, "", "", 0 // source file path is not active job file
	}

	// file name ends with pid, convert process id
	pid, err := strconv.Atoi(p)
	if err != nil || pid <= 0 {
		return subStamp, oms, "", "", 0 // pid must be positive integer
	}
	return subStamp, oms, mn, dgst, pid
}

// parse queue file path or queue file name and return submission stamp, oms instance name, model name and digest.
// For example: 2022_07_05_19_55_38_111-#-_4040-#-RiskPaths-#-d90e1e9a.json
func parseQueuePath(srcPath string) (string, string, string, string) {

	// parse common job file part
	subStamp, oms, mn, dgst, p := parseJobPath(srcPath)

	if subStamp == "" || oms == "" || mn == "" || dgst == "" || p != "" {
		return subStamp, oms, "", "" // source file path is not queue job file
	}

	return subStamp, oms, mn, dgst
}

// parse history file path or history file name and
// return submission stamp, oms instance name, model name, digest, run stamp and run status.
// For example: 2022_07_04_20_06_10_817-#-_4040-#-RiskPaths-#-d90e1e9a-#-2022_07_04_20_06_10_818-#-success.json
func parseHistoryPath(srcPath string) (string, string, string, string, string, string) {

	// parse common job file part
	subStamp, oms, mn, dgst, p := parseJobPath(srcPath)

	if subStamp == "" || oms == "" || mn == "" || dgst == "" || len(p) < len("r-#-s") {
		return subStamp, oms, "", "", "", "" // source file path is not history job file
	}

	// get run stamp and status
	sp := strings.Split(p, "-#-")
	if len(sp) != 2 || sp[0] == "" || sp[1] == "" {
		return subStamp, oms, "", "", "", "" // source file path is not history job file
	}

	return subStamp, oms, mn, dgst, sp[0], sp[1]
}

// write new run request into job queue file
func addJobToQueue(job *RunJob) (*runJobFile, error) {

	rjf := runJobFile{RunJob: *job}

	// write into job queue file if job control is enabled
	if !theCfg.isJobControl {
		return &rjf, nil // job control disabled
	}

	rjf.filePath = jobQueuePath(job.SubmitStamp, job.ModelName, job.ModelDigest)

	err := helper.ToJsonIndentFile(rjf.filePath, job)
	if err != nil {
		omppLog.Log(err)
		fileDeleteAndLog(true, rjf.filePath) // on error remove file, if any file created
		return nil, err
	}

	return &rjf, nil
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
		fileDeleteAndLog(true, src) // invalid file content: remove job control file from queue
		return false
	}

	// add run stamp, process info and move job control file into active
	jc.RunStamp = runStamp
	jc.Pid = rState.pid
	jc.CmdPath = rState.cmdPath
	jc.LogFileName = rState.LogFileName
	jc.LogPath = rState.logPath

	dst := jobActivePath(rState.SubmitStamp, rState.ModelName, rState.ModelDigest, rState.pid)

	fileDeleteAndLog(false, src) // remove job control file from queue

	err = helper.ToJsonIndentFile(dst, &jc)
	if err != nil {
		omppLog.Log(err)
		fileDeleteAndLog(true, dst) // on error remove file, if any file created
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
		fileDeleteAndLog(true, src) // if move failed then delete job control file from active list
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
		fileDeleteAndLog(true, srcPath) // if move failed then delete job control file from active list
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

	if !fileMoveAndLog(true, src, dst) {
		fileDeleteAndLog(true, src) // if move failed then delete job control file from queue
		return false
	}
	return true
}

// read job control state from the file, return empty state on error or if state file not exist
func jobStateRead() (*jobControlState, bool) {

	var jcs jobControlState
	isOk, err := helper.FromJsonFile(jobStatePath(), &jcs)
	if err != nil {
		omppLog.Log(err)
	}
	if !isOk || err != nil {
		return &jobControlState{Queue: []string{}}, false
	}
	return &jcs, true
}

// return true if jobs queue processing is paused
func isPausedJobState() bool {
	return fileExist(jobPausedPath()) == nil
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
func scanOuterJobs(doneC <-chan bool) {
	if !theCfg.isJobControl {
		return // job control disabled
	}

	// map active job file path to file content (run job), it is only job where no run state in RunCatalog
	outerJobs := map[string]RunJob{}

	activeDir := filepath.Join(theCfg.jobDir, "active")
	nActive := len(activeDir)
	ptrn := activeDir + string(filepath.Separator) + "*-#-" + theCfg.omsName + "-#-*.json"

	for {
		// find active job files
		fLst := filesByPattern(ptrn, "Error at active job files search")
		if len(fLst) <= 0 {
			if doExitSleep(jobOuterScanInterval, doneC) {
				return
			}
			continue // no active jobs
		}

		// find new active jobs since last scan which do not exist in run state list of RunCatalog
		for k := range fLst {

			if _, ok := outerJobs[fLst[k]]; ok {
				continue // this file already in the outer jobs list
			}

			// get submission stamp, model name, digest and process id from active job file name
			stamp, _, mn, dgst, pid := parseActivePath(fLst[k][nActive+1:])
			if stamp == "" || mn == "" || dgst == "" || pid <= 0 {
				continue // file name is not an active job file name
			}

			// find run state by model digest and submission stamp
			isFound, _ := theRunCatalog.getRunStateBySubmitStamp(dgst, stamp)
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
				moveOuterJobToHistory(fLst[k], "", stamp, mn, dgst, "no-model-run-time-stamp") // invalid file content: move to history with unknown status
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
		if doExitSleep(jobOuterScanInterval, doneC) {
			return
		}
	}
}

// scan job control directories to read and update job lists: queue, active and history
func scanJobs(doneC <-chan bool) {
	if !theCfg.isJobControl {
		return // job control disabled
	}

	queuePtrn := filepath.Join(theCfg.jobDir, "queue") + string(filepath.Separator) + "*-#-" + theCfg.omsName + "-#-*.json"
	activePtrn := filepath.Join(theCfg.jobDir, "active") + string(filepath.Separator) + "*-#-" + theCfg.omsName + "-#-*.json"
	historyPtrn := filepath.Join(theCfg.jobDir, "history") + string(filepath.Separator) + "*-#-" + theCfg.omsName + "-#-*.json"

	jobStatePath := jobStatePath()
	nJobStateErrCount := 0

	// map job file submission stamp to file content (run job)
	toJobMap := func(fLst []string, jobMap map[string]runJobFile) []string {

		subStamps := make([]string, 0, len(fLst)) // list of submission stamps

		for _, f := range fLst {

			// get submission stamp and oms instance
			stamp, oms, mn, dgst, _ := parseJobPath(f)
			if stamp == "" || oms == "" || mn == "" || dgst == "" {
				continue // file name is not a job file name
			}
			subStamps = append(subStamps, stamp)

			if _, ok := jobMap[stamp]; ok {
				continue // this file already in the jobs list
			}

			// create run state from job file
			var jc RunJob
			isOk, err := helper.FromJsonFile(f, &jc)
			if err != nil {
				omppLog.Log(err)
				jobMap[stamp] = runJobFile{filePath: f, isError: true}
			}
			if !isOk || err != nil {
				continue // file not exist or invalid
			}

			jobMap[stamp] = runJobFile{RunJob: jc, filePath: f} // add job into jobs list
		}
		return subStamps
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
			hKeys = append(hKeys, subStamp)

			if _, ok := historyJobs[subStamp]; ok {
				continue // this file already in the history jobs list
			}

			// add job into history jobs list
			historyJobs[subStamp] = historyJobFile{
				filePath:    f,
				isError:     (mn == "" || dgst == "" || rStamp == "" || status == ""),
				SubmitStamp: subStamp,
				ModelName:   mn,
				ModelDigest: dgst,
				RunStamp:    rStamp,
				JobStatus:   status,
			}
		}

		// remove from queue files or active files which are in history
		// remove from queue files which are in active
		for stamp := range historyJobs {
			delete(queueJobs, stamp)
			delete(activeJobs, stamp)
		}
		for stamp := range activeJobs {
			delete(queueJobs, stamp)
		}

		// remove existing job entries where files are no longer exist
		sort.Strings(qKeys)
		for stamp := range queueJobs {
			k := sort.SearchStrings(qKeys, stamp)
			if k < 0 || k >= len(qKeys) || qKeys[k] != stamp {
				delete(queueJobs, stamp)
			}
		}
		sort.Strings(aKeys)
		for stamp := range activeJobs {
			k := sort.SearchStrings(aKeys, stamp)
			if k < 0 || k >= len(aKeys) || aKeys[k] != stamp {
				delete(activeJobs, stamp)
			}
		}
		sort.Strings(hKeys)
		for stamp := range historyJobs {
			k := sort.SearchStrings(hKeys, stamp)
			if k < 0 || k >= len(hKeys) || hKeys[k] != stamp {
				delete(historyJobs, stamp)
			}
		}

		// update run catalog with current job control files
		jsc := theRunCatalog.updateRunJobs(queueJobs, isPausedJobState(), activeJobs, historyJobs)

		// save persistent part of jobs state
		if nJobStateErrCount < 16 {

			err := helper.ToJsonIndentFile(jobStatePath, jsc)
			if err != nil {
				omppLog.Log(err)
				nJobStateErrCount++
			} else {
				nJobStateErrCount = 0
			}
		}

		// wait for doneC or sleep
		if doExitSleep(jobScanInterval, doneC) {
			return
		}
	}
}

// scan model run queue and start model runs
func scanRunJobs(doneC <-chan bool) {
	if !theCfg.isJobControl {
		return // job control disabled: no queue
	}

	for {
		// get job from the queue and run
		if stamp, req, isFound := theRunCatalog.getJobFromQueue(); isFound {

			_, e := theRunCatalog.runModel(stamp, req)
			if e != nil {
				omppLog.Log(e)
			}
		}

		// wait for doneC or sleep
		if doExitSleep(jobQueueScanInterval, doneC) {
			return
		}
	}
}
