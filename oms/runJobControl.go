// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"

	ps "github.com/keybase/go-ps"
)

const jobScanInterval = 1123      // timeout in msec, sleep interval between scanning all job directories
const jobQueueScanInterval = 107  // timeout in msec, sleep interval between getting next job from the queue
const jobOuterScanInterval = 5021 // timeout in msec, sleep interval between scanning active job directory

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
			stamp, _, mn, dgst, _, _, pid := parseActivePath(fLst[k][nActive+1:])
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

	// map job file submission stamp to file content (run job)
	toJobMap := func(fLst []string, jobMap map[string]runJobFile, omsActive map[string]int64) ([]string, RunRes, RunRes) {

		subStamps := make([]string, 0, len(fLst)) // list of submission stamps
		totalRes := RunRes{}
		ownRes := RunRes{}

		for _, f := range fLst {

			// get submission stamp and oms instance
			stamp, oms, mn, dgst, cpu, mem, _ := parseJobInPath(f)
			if stamp == "" || oms == "" || mn == "" || dgst == "" {
				continue // file name is not a job file name
			}
			if _, ok := omsActive[oms]; !ok {
				continue // skip: oms instance inactive
			}

			// collect total resource usage
			totalRes.Cpu = totalRes.Cpu + cpu
			totalRes.Mem = totalRes.Mem + mem

			if oms != theCfg.omsName {
				continue // done with this job: it is other oms instance
			}

			// this is own job: job to run in current oms instance
			ownRes.Cpu = ownRes.Cpu + cpu
			ownRes.Mem = ownRes.Mem + mem

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
		return subStamps, totalRes, ownRes
	}

	omsTickPath, omsTickPrefix := createOmsTick() // job processing started at this oms instance
	nTick := 0

	queuePtrn := filepath.Join(theCfg.jobDir, "queue") + string(filepath.Separator) + "*-#-*-#-*-#-*-#-cpu-#-*-#-mem-#-*.json"
	activePtrn := filepath.Join(theCfg.jobDir, "active") + string(filepath.Separator) + "*-#-*-#-*-#-*-#-cpu-#-*-#-mem-#-*.json"
	historyPtrn := filepath.Join(theCfg.jobDir, "history") + string(filepath.Separator) + "*-#-" + theCfg.omsName + "-#-*.json"
	omsTickPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "oms-#-*-#-*-#-*"
	limitCpuPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "total-limit-cpu-#-*"
	limitMemPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "total-limit-mem-#-*"

	queueJobs := map[string]runJobFile{}
	activeJobs := map[string]runJobFile{}
	historyJobs := map[string]historyJobFile{}
	omsActive := map[string]int64{}

	for {
		queueFiles := filesByPattern(queuePtrn, "Error at queue job files search")
		activeFiles := filesByPattern(activePtrn, "Error at active job files search")
		historyFiles := filesByPattern(historyPtrn, "Error at history job files search")
		omsTickFiles := filesByPattern(omsTickPtrn, "Error at oms heart beat files search")
		limitCpuFiles := filesByPattern(limitCpuPtrn, "Error at limit CPU cores file search")
		limitMemFiles := filesByPattern(limitMemPtrn, "Error at limit memory file search")

		// update available resources limits
		updateTs := time.Now()
		minStateTs := updateTs.Add(-1 * time.Minute).UnixMilli()

		jsState := JobServiceState{
			IsQueuePaused:     isPausedJobQueue(),
			JobUpdateDateTime: helper.MakeDateTime(updateTs),
		}
		if len(limitCpuFiles) > 0 {
			jsState.LimitTotalRes.Cpu = parseTotalLimitPath(limitCpuFiles[0], "cpu")
		}
		if len(limitMemFiles) > 0 {
			jsState.LimitTotalRes.Mem = parseTotalLimitPath(limitMemFiles[0], "mem")
		}

		// update oms instances heart beat status
		for _, fp := range omsTickFiles {

			oms, _, ts := parseOmsTickPath(fp)
			if oms == "" {
				continue // skip: invalid active run job state file path
			}
			if ts > minStateTs {
				omsActive[oms] = ts // oms instance is alive
			} else {
				delete(omsActive, oms) // oms instance not active
			}
		}

		// parse queue and active files
		qKeys, qTotal, qOwn := toJobMap(queueFiles, queueJobs, omsActive)
		aKeys, aTotal, aOwn := toJobMap(activeFiles, activeJobs, omsActive)

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

		// update run catalog with current job control files and save persistent part of jobs state
		jsState.ActiveTotalRes = aTotal
		jsState.ActiveOwnRes = aOwn
		jsState.QueueTotalRes = qTotal
		jsState.QueueOwnRes = qOwn

		jsc := theRunCatalog.updateRunJobs(jsState, queueJobs, activeJobs, historyJobs)
		jobStateWrite(*jsc)

		//  update oms heart beat file
		nTick++
		if nTick%7 == 0 {
			omsTickPath, _ = moveToNextOmsTick(omsTickPath, omsTickPrefix)
		}

		// wait for doneC or sleep
		if doExitSleep(jobScanInterval, doneC) {
			break
		}
	}

	fileDeleteAndLog(true, omsTickPath) // try to remove oms heart beat file, this code may never be executed due to race at shutdown
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
