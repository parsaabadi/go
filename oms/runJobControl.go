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

const jobScanInterval = 1123             // timeout in msec, sleep interval between scanning all job directories
const jobQueueScanInterval = 107         // timeout in msec, sleep interval between getting next job from the queue
const jobOuterScanInterval = 5021        // timeout in msec, sleep interval between scanning active job directory
const serverTimeoutDefault = 60          // time in seconds to start or stop compute server
const minJobTickMs int64 = 1597707959000 // unix milliseconds of 2020-08-17 23:45:59
const jobPositionDefault = 20220817      // queue job position by default, e.g. if queue is empty
const jobMinPositionDefault = 10240817   // queue minimal job position by default, e.g. on instance startup

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

	omsTickPath, omsTickPrefix := createOmsTick() // job processing started at this oms instance
	nTick := 0

	// model run files in queue, active runs, run history
	queuePtrn := filepath.Join(theCfg.jobDir, "queue") + string(filepath.Separator) + "*-#-*-#-*-#-*-#-cpu-#-*-#-mem-#-*.json"
	activePtrn := filepath.Join(theCfg.jobDir, "active") + string(filepath.Separator) + "*-#-*-#-*-#-*-#-cpu-#-*-#-mem-#-*.json"
	historyPtrn := filepath.Join(theCfg.jobDir, "history") + string(filepath.Separator) + "*-#-" + theCfg.omsName + "-#-*.json"

	// oms instances heart beat files:
	// if oms instance file does not updated more than 1 minute then oms instance is dead
	omsTickPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "oms-#-*-#-*-#-*"

	// total resources limits:
	// if limit files not exist or value <= 0 then resource is unlimited
	limitCpuPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "total-limit-cpu-#-*"
	limitMemPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "total-limit-mem-#-*"

	// max time in seconds to start compute server or cluster: max-time-start-#-123
	// max time in seconds to stop compute server or cluster:  max-time-stop-#-456
	// max idle time before stopping server or cluster:        max-time-idle-#-900
	maxTimeStartPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "max-time-start-#-*"
	maxTimeStopPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "max-time-stop-#-*"
	maxTimeIdlePtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "max-time-idle-#-*"

	// compute servers or clusters:
	// server resources: comp-#-name-#-cpu-#-8-#-mem-#-16
	// server ready:     comp-ready-#-name
	// server starting:  comp-start-#-name-#-2022_08_17_22_33_44_567
	// server stopping:  comp-stop-#-name-#-2022_08_17_22_33_44_567
	// server error:     comp-error-#-name-#-2022_08_17_22_33_44_567
	// server used by model run: comp-used-#-name-#-2022_07_08_23_03_27_555-#-_4040-#-cpu-#-4-#-mem-#-8
	compPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "comp-#-*-#-cpu-#-*-#-mem-#-*"
	compReadyPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "comp-ready-#-*"
	compStartPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "comp-start-#-*-#-*"
	compStopPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "comp-stop-#-*-#-*"
	compErrorPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "comp-error-#-*-#-*"
	compUsedPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "comp-used-#-*-#-*-#-*-#-cpu-#-*-#-mem-#-*"

	queueJobs := map[string]queueJobFile{}
	activeJobs := map[string]runJobFile{}
	historyJobs := map[string]historyJobFile{}
	omsActive := map[string]int64{}
	computeState := map[string]computeItem{}

	for {
		queueFiles := filesByPattern(queuePtrn, "Error at queue job files search")
		activeFiles := filesByPattern(activePtrn, "Error at active job files search")
		historyFiles := filesByPattern(historyPtrn, "Error at history job files search")
		omsTickFiles := filesByPattern(omsTickPtrn, "Error at oms heart beat files search")
		limitCpuFiles := filesByPattern(limitCpuPtrn, "Error at limit CPU cores file search")
		limitMemFiles := filesByPattern(limitMemPtrn, "Error at limit memory file search")
		maxTimeStartFiles := filesByPattern(maxTimeStartPtrn, "Error at max time files search")
		maxTimeStopFiles := filesByPattern(maxTimeStopPtrn, "Error at max time files search")
		maxTimeIdleFiles := filesByPattern(maxTimeIdlePtrn, "Error at max time files search")
		compFiles := filesByPattern(compPtrn, "Error at server files search")
		compReadyFiles := filesByPattern(compReadyPtrn, "Error at server ready files search")
		compStartFiles := filesByPattern(compStartPtrn, "Error at server start files search")
		compStopFiles := filesByPattern(compStopPtrn, "Error at server stop files search")
		compErrorFiles := filesByPattern(compErrorPtrn, "Error at server errors files search")
		compUsedFiles := filesByPattern(compUsedPtrn, "Error at server usage files search")

		// update available resources limits
		updateTs := time.Now()

		jsState := JobServiceState{
			IsQueuePaused:     isPausedJobQueue(),
			JobUpdateDateTime: helper.MakeDateTime(updateTs),
			jobNextPosition:   jobPositionDefault,
			jobMinPosition:    jobMinPositionDefault,
		}
		if len(limitCpuFiles) > 0 {
			jsState.LimitTotalRes.Cpu = parseLimitPath(limitCpuFiles[0], "total-limit-cpu")
		}
		if len(limitMemFiles) > 0 {
			jsState.LimitTotalRes.Mem = parseLimitPath(limitMemFiles[0], "total-limit-mem")
		}
		if len(maxTimeStartFiles) > 0 {
			jsState.maxStartTime = parseLimitPath(maxTimeStartFiles[0], "max-time-start")
		}
		if len(maxTimeStopFiles) > 0 {
			jsState.maxStopTime = parseLimitPath(maxTimeStopFiles[0], "max-time-stop")
		}
		if len(maxTimeIdleFiles) > 0 {
			jsState.maxIdleTime = parseLimitPath(maxTimeIdleFiles[0], "max-time-idle")
		}
		if jsState.maxStartTime <= 0 {
			jsState.maxStartTime = serverTimeoutDefault // time to start server or cluster by default
		}
		if jsState.maxStopTime <= 0 {
			jsState.maxStopTime = serverTimeoutDefault // time to stop server or cluster by default
		}

		// set minimal times to detect timeouts
		// one minute of missing heart beats is an interval to consider oms instance dead
		// set max time for compute server start or stop timeouts
		minOmsStateTs := updateTs.Add(-1 * time.Minute).UnixMilli()
		maxStartTs := updateTs.Add(time.Duration(jsState.maxStartTime) * time.Second).UnixMilli()
		maxStopTs := updateTs.Add(time.Duration(jsState.maxStartTime) * time.Second).UnixMilli()

		// update oms instances heart beat status
		for _, fp := range omsTickFiles {

			oms, _, ts := parseOmsTickPath(fp)
			if oms == "" {
				continue // skip: invalid active run job state file path
			}
			if ts > minOmsStateTs {
				omsActive[oms] = ts // oms instance is alive
			} else {
				delete(omsActive, oms) // oms instance not active
			}
		}

		// computational resources state
		// for each server or cluster detect current state: ready, start, stop or power off
		// total computational resources (cpu and memory), used resources and avaliable resources
		updateComputeState(
			computeState,
			omsActive,
			maxStartTs, maxStopTs,
			compFiles, compReadyFiles, compStartFiles, compStopFiles, compErrorFiles, compUsedFiles)

		// queue resources limits:
		// do sum of all computational servers cpu cores and memory and apply total resource limits
		cRes := RunRes{}

		for _, cu := range computeState {
			cRes.Cpu += cu.totalRes.Cpu
			cRes.Mem += cu.totalRes.Mem
		}
		if cRes.Cpu > 0 && jsState.LimitTotalRes.Cpu > cRes.Cpu {
			jsState.LimitTotalRes.Cpu = cRes.Cpu
		}
		if cRes.Mem > 0 && jsState.LimitTotalRes.Mem > cRes.Mem {
			jsState.LimitTotalRes.Mem = cRes.Mem
		}

		// model runs
		//
		// parse active files, use unlimited resources for already active jobs
		aKeys, aTotal, aOwn := updateActiveJobs(activeFiles, activeJobs, omsActive)

		// parse queue files
		sort.Strings(queueFiles)
		qKeys, maxPos, minPos, qTotal, qOwn := updateQueueJobs(queueFiles, queueJobs, jsState.LimitTotalRes, omsActive)

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
		jsState.jobNextPosition = maxPos
		jsState.jobMinPosition = minPos

		jsc := theRunCatalog.updateRunJobs(jsState, computeState, queueJobs, activeJobs, historyJobs)
		jobStateWrite(*jsc)

		// update oms heart beat file
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

// insert run job into job map: map job file submission stamp to file content (run job)
func updateActiveJobs(fLst []string, jobMap map[string]runJobFile, omsActive map[string]int64) ([]string, RunRes, RunRes) {

	subStamps := make([]string, 0, len(fLst)) // list of submission stamps
	totalRes := RunRes{}
	ownRes := RunRes{}

	for _, f := range fLst {

		// get submission stamp, oms instance and resources
		stamp, oms, mn, dgst, cpu, mem, _ := parseJobActPath(f)
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

		if jc, ok := jobMap[stamp]; ok {
			jobMap[stamp] = jc
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

// insert run job into queue job map: map job file submission stamp to file content (run job)
func updateQueueJobs(fLst []string, jobMap map[string]queueJobFile, aRes RunRes, omsActive map[string]int64) ([]string, int, int, RunRes, RunRes) {

	nFiles := len(fLst)
	maxPos := jobPositionDefault + 1
	minPos := jobPositionDefault - 1

	// queue file name parts
	type fileH struct {
		fileIdx  int    // source file index
		oms      string // instance name
		stamp    string // submission stamp
		position int    // queue position file name part
		res      RunRes // resources required to run the model
		preRes   RunRes // resources required for queue jobs before this job
		isOver   bool   // if true then resources required are exceeding total resource(s) limit(s)
	}
	type omsQ struct {
		top int     // current top queue file index
		q   []fileH // instance queue
	}
	allQ := make(map[string]omsQ, nFiles) // queue for each oms instance

	// for each oms instance append files to job queue
	for k, f := range fLst {

		// get submission stamp, oms instance and queue position
		stamp, oms, mn, dgst, cpu, mem, pos := parseJobActPath(f)
		if stamp == "" || oms == "" || mn == "" || dgst == "" {
			continue // file name is not a job file name
		}
		if maxPos <= pos {
			maxPos = pos + 1
		}
		if minPos >= pos {
			minPos = pos - 1
		}
		if _, ok := omsActive[oms]; !ok {
			continue // skip: oms instance inactive
		}

		// append to the instance queue
		aq, ok := allQ[oms]
		if !ok {
			aq = omsQ{q: make([]fileH, 0, nFiles)}
		}
		aq.q = append(aq.q, fileH{
			fileIdx:  k,
			oms:      oms,
			stamp:    stamp,
			position: pos,
			res:      RunRes{Cpu: cpu, Mem: mem},
			isOver:   (aRes.Cpu > 0 && cpu > aRes.Cpu) || (aRes.Mem > 0 && mem > aRes.Mem),
		})
		allQ[oms] = aq
	}

	// sort each job queue in order of position file name part and submission stamp
	for _, aq := range allQ {
		sort.SliceStable(aq.q, func(i, j int) bool {
			return aq.q[i].position < aq.q[j].position || aq.q[i].position == aq.q[j].position && aq.q[i].stamp < aq.q[j].stamp
		})
	}

	// sort oms instance names
	nOms := len(allQ)
	omsKeys := make([]string, nOms)
	n := 0
	for oms := range allQ {
		omsKeys[n] = oms
		n++
	}
	sort.Strings(omsKeys)

	// order combined queue jobs by submission stamp and instance name
	// inside of each oms instance queue jobs are ordered by position and submission stamps
	// for example:
	//   _4040 [ {1234, 2022_08_17}, {4567, 2022_08_12} ]
	//   _8080 [ {1212, 2022_08_17}, {3434, 2022_08_16} ]
	// result:
	//   [ {_4040, 1234, 2022_08_17}, {_8080, 1212, 2022_08_17}, {_4040, 4567, 2022_08_12}, {_8080, 3434, 2022_08_16} ]

	totalRes := RunRes{} // total resources required to serve all queues

	for isAll := false; !isAll; {

		// find oms instance where curent job has minimal submission stamp
		// if there is the same stamp in multiple instances then use minimal oms instance name
		topStamp := ""
		topOms := ""
		isAll = true

		for k := 0; k < nOms; k++ {

			aq := allQ[omsKeys[k]]
			if aq.top >= len(aq.q) {
				continue // all jobs in that queue are already processed
			}
			isAll = false

			if topOms == "" {
				topOms = omsKeys[k]
				topStamp = aq.q[aq.top].stamp
			} else {
				if aq.q[aq.top].stamp < topStamp {
					topOms = omsKeys[k]
					topStamp = aq.q[aq.top].stamp
				}
			}
		}
		if isAll {
			break // all jobs in all queues are sorted
		}

		aq := allQ[topOms] // this queue contains minimal submission stamp at current queue top position

		// collect total resource usage
		preRes := totalRes
		if !aq.q[aq.top].isOver {
			totalRes.Cpu = totalRes.Cpu + aq.q[aq.top].res.Cpu
			totalRes.Mem = totalRes.Mem + aq.q[aq.top].res.Mem
		}
		aq.q[aq.top].preRes = preRes // resource used by jobs before current

		// move top of this queue to the next position and update current top job
		aq.top++
		allQ[topOms] = aq
	}

	// update current instance job map, queue files and resources
	qKeys := make([]string, 0, nFiles)
	ownRes := RunRes{}

	ownQ, isOwn := allQ[theCfg.omsName]
	if !isOwn {
		return qKeys, maxPos, minPos, totalRes, ownRes // there are no jobs for current oms instance
	}

	for _, f := range ownQ.q {

		ownRes.Cpu = ownRes.Cpu + f.res.Cpu
		ownRes.Mem = ownRes.Mem + f.res.Mem

		qKeys = append(qKeys, f.stamp)

		// if this file already in the queue jobs map then update resources
		if jc, ok := jobMap[f.stamp]; ok {

			jc.filePath = fLst[f.fileIdx]
			jc.position = f.position
			jc.preRes = f.preRes
			jc.IsOverLimit = f.isOver
			jobMap[f.stamp] = jc // update exsiting job in the queue with current resources info
			continue
		}
		// else create run state from job file and insert into the queue map
		var jc RunJob

		isOk, err := helper.FromJsonFile(fLst[f.fileIdx], &jc)
		if err != nil {
			omppLog.Log(err)
			jobMap[f.stamp] = queueJobFile{runJobFile: runJobFile{filePath: fLst[f.fileIdx], isError: true}}
		}
		if !isOk || err != nil {
			continue // file does not exist or invalid
		}
		jc.IsOverLimit = f.isOver

		// add new job into queue jobs map
		jobMap[f.stamp] = queueJobFile{
			runJobFile: runJobFile{RunJob: jc, filePath: fLst[f.fileIdx]},
			position:   f.position,
			preRes:     f.preRes,
		}
	}
	return qKeys, maxPos, minPos, totalRes, ownRes
}

// Update computational serveres or clusters map.
// For each server or cluster find current state (ready, start, stop or power off),
// total computational resources (cpu and memory), used resources and avaliable resources.
func updateComputeState(
	computeState map[string]computeItem,
	omsActive map[string]int64,
	maxStartTs, maxStopTs int64,
	compFiles, compReadyFiles, compStartFiles, compStopFiles, compErrorFiles, compUsedFiles []string,
) {

	cKeys := make([]string, 0, len(computeState))

	for _, f := range compFiles {

		// get server name, cpu count and memory size
		name, cpu, mem := parseCompPath(f)
		if name == "" {
			continue // skip: this is not a compute server file
		}
		cKeys = append(cKeys, name)

		// add to the servers list
		computeState[name] = computeItem{
			totalRes: RunRes{Cpu: cpu, Mem: mem},
		}
	}
	sort.Strings(cKeys)
	for name := range computeState {
		k := sort.SearchStrings(cKeys, name)
		if k < 0 || k >= len(cKeys) || cKeys[k] != name {
			delete(computeState, name)
		}
	}

	for _, f := range compReadyFiles {

		// get server name
		name := parseCompReadyPath(f)
		if name == "" {
			continue // skip: this is not a compute server ready file
		}

		// update server state to ready
		if cs, ok := computeState[name]; ok {
			cs.state = "ready"
			computeState[name] = cs
		}
	}

	for _, f := range compStartFiles {

		// get server name and time stamp
		name, _, ts := parseCompStatePath(f, "start")
		if name == "" {
			continue // skip: this is not a compute server state file
		}

		if cs, ok := computeState[name]; ok { // if this server still exist

			// update server state to start or detect error
			if (cs.state == "" || cs.state == "start") && ts <= maxStartTs {
				cs.state = "start"
			} else {
				if cs.lastErrorTs < ts {
					cs.lastErrorTs = ts
				}
				cs.errorCount++
			}
			computeState[name] = cs
		}
	}

	for _, f := range compStopFiles {

		// get server name and time stamp
		name, _, ts := parseCompStatePath(f, "stop")
		if name == "" {
			continue // skip: this is not a compute server state file
		}

		if cs, ok := computeState[name]; ok { // if this server still exist

			// update server state to stop or detect error
			if (cs.state == "ready" || cs.state == "stop") && ts <= maxStopTs {
				cs.state = "stop"
			} else {
				if cs.lastErrorTs < ts {
					cs.lastErrorTs = ts
				}
				cs.errorCount++
			}
			computeState[name] = cs
		}
	}

	for _, f := range compErrorFiles {

		// get server name and time stamp
		name, _, ts := parseCompStatePath(f, "stop")
		if name == "" {
			continue // skip: this is not a compute server state file
		}

		if cs, ok := computeState[name]; ok { // if this server still exist

			// count server errors
			if cs.lastErrorTs < ts {
				cs.lastErrorTs = ts
			}
			cs.errorCount++

			computeState[name] = cs
		}
	}

	for _, f := range compUsedFiles {

		// get server name and time stamp
		name, _, oms, cpu, mem := parseCompUsedPath(f)
		if name == "" || oms == "" {
			continue // skip: this is not a compute server state file
		}

		// if this server stil exist
		if cs, ok := computeState[name]; ok {

			if _, ok = omsActive[oms]; !ok {
				continue // oms instance not active
			}

			// update resources used by model runs
			cs.usedRes.Cpu += cpu
			cs.usedRes.Mem += mem
			if oms == theCfg.omsName {
				cs.ownRes.Cpu += cpu
				cs.ownRes.Mem += mem
			}
			computeState[name] = cs
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
		if job, isFound := theRunCatalog.selectJobFromQueue(); isFound {

			_, e := theRunCatalog.runModel(job)
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
