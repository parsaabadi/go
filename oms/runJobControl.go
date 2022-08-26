// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"

	ps "github.com/keybase/go-ps"
)

const jobScanInterval = 1123             // timeout in msec, sleep interval between scanning all job directories
const computeStartStopInterval = 3373    // timeout in msec, interval between start or stop computational servers, must be at least 2 * jobScanInterval
const jobQueueScanInterval = 107         // timeout in msec, sleep interval between getting next job from the queue
const jobOuterScanInterval = 5021        // timeout in msec, sleep interval between scanning active job directory
const serverTimeoutDefault = 60          // time in seconds to start or stop compute server
const minJobTickMs int64 = 1597707959000 // unix milliseconds of 2020-08-17 23:45:59
const jobPositionDefault = 20220817      // queue job position by default, e.g. if queue is empty
const maxComputeErrors = 8               // errors threshold for compute server or cluster

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
			stamp, _, mn, dgst, _, _, _, pid := parseActivePath(fLst[k][nActive+1:])
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
				moveActiveJobToHistory(fLst[k], "", stamp, mn, dgst, "no-model-run-time-stamp") // invalid file content: move to history with unknown status
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
			moveActiveJobToHistory(fp, rStat, jc.SubmitStamp, jc.ModelName, jc.ModelDigest, jc.RunStamp)
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

	// path to job.ini: available resources limits and computational servers configuration
	jobIniPath := filepath.Join(theCfg.jobDir, "job.ini")

	// model run files in queue, active runs, run history
	queuePtrn := filepath.Join(theCfg.jobDir, "queue") + string(filepath.Separator) + "*-#-*-#-*-#-*-#-*-#-cpu-#-*-#-mem-#-*.json"
	activePtrn := filepath.Join(theCfg.jobDir, "active") + string(filepath.Separator) + "*-#-*-#-*-#-*-#-*-#-cpu-#-*-#-mem-#-*.json"
	historyPtrn := filepath.Join(theCfg.jobDir, "history") + string(filepath.Separator) + "*-#-" + theCfg.omsName + "-#-*.json"

	// oms instances heart beat files:
	// if oms instance file does not updated more than 1 minute then oms instance is dead
	omsTickPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "oms-#-*-#-*-#-*"

	// compute servers or clusters:
	// server ready: comp-ready-#-name
	// server start: comp-start-#-name-#-2022_08_17_22_33_44_567
	// server stop:  comp-stop-#-name-#-2022_08_17_22_33_44_567
	// server error: comp-error-#-name-#-2022_08_17_22_33_44_567
	// server used by model run: comp-used-#-name-#-2022_07_08_23_03_27_555-#-_4040-#-cpu-#-4-#-mem-#-8
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
		// get jobs service state and computational resources state: servers or clustres definition
		updateTs := time.Now()
		nowTs := updateTs.UnixMilli()

		jsState := initJobComputeState(jobIniPath, updateTs, computeState)

		queueFiles := filesByPattern(queuePtrn, "Error at queue job files search")
		activeFiles := filesByPattern(activePtrn, "Error at active job files search")
		historyFiles := filesByPattern(historyPtrn, "Error at history job files search")
		omsTickFiles := filesByPattern(omsTickPtrn, "Error at oms heart beat files search")
		compReadyFiles := filesByPattern(compReadyPtrn, "Error at server ready files search")
		compStartFiles := filesByPattern(compStartPtrn, "Error at server start files search")
		compStopFiles := filesByPattern(compStopPtrn, "Error at server stop files search")
		compErrorFiles := filesByPattern(compErrorPtrn, "Error at server errors files search")
		compUsedFiles := filesByPattern(compUsedPtrn, "Error at server usage files search")

		jsState.jobLastPosition = jobPositionDefault + (1 + len(queueFiles))
		jsState.jobFirstPosition = jobPositionDefault - (1 + len(queueFiles))

		// update oms instances heart beat status
		// one minute of missing heart beats is an interval to consider oms instance dead
		minOmsStateTs := updateTs.Add(-1 * time.Minute).UnixMilli()
		leaderName := ""

		for _, fp := range omsTickFiles {

			oms, _, ts := parseOmsTickPath(fp)
			if oms == "" {
				continue // skip: invalid active run job state file path
			}

			if ts > minOmsStateTs {
				omsActive[oms] = ts // oms instance is alive

				if leaderName == "" || leaderName > oms {
					leaderName = oms
				}
			} else {
				delete(omsActive, oms) // oms instance not active
			}
		}
		jsState.isLeader = theCfg.omsName == leaderName // this oms instance is a leader instance

		// computational resources state
		// for each server or cluster detect current state: ready, start, stop or power off
		// total computational resources (cpu and memory), used resources and avaliable resources
		updateComputeState(
			computeState,
			omsActive,
			nowTs,
			jsState.maxStartTime,
			jsState.maxStopTime,
			compReadyFiles, compStartFiles, compStopFiles, compErrorFiles, compUsedFiles)

		jsState.ComputeErrorRes = RunRes{}

		for _, cs := range computeState {
			if cs.state == "error" {
				jsState.ComputeErrorRes.Cpu += cs.totalRes.Cpu
				jsState.ComputeErrorRes.Mem += cs.totalRes.Mem
			}
		}

		aRes := RunRes{}
		isMpiLimit := true

		if len(computeState) > 0 {

			aRes.Cpu = jsState.MpiRes.Cpu - jsState.ComputeErrorRes.Cpu
			aRes.Mem = jsState.MpiRes.Mem - jsState.ComputeErrorRes.Mem
		} else {

			isMpiLimit = jsState.LocalRes.Cpu > 0
			if isMpiLimit {
				aRes = jsState.LocalRes
			}
		}

		// model runs
		//
		// parse active files, use unlimited resources for already active jobs
		aKeys, aTotal, aOwn, aLocal := updateActiveJobs(activeFiles, activeJobs, omsActive)

		// parse queue files
		sort.Strings(queueFiles)
		qKeys, maxPos, minPos, qTotal, qOwn, qLocal := updateQueueJobs(queueFiles, queueJobs, aRes, isMpiLimit, jsState.LocalRes, omsActive)

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
		jsState.LocalActiveRes = aLocal
		jsState.QueueTotalRes = qTotal
		jsState.QueueOwnRes = qOwn
		jsState.LocalQueueRes = qLocal
		jsState.jobLastPosition = maxPos
		jsState.jobFirstPosition = minPos

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
func updateActiveJobs(fLst []string, jobMap map[string]runJobFile, omsActive map[string]int64) ([]string, RunRes, RunRes, RunRes) {

	subStamps := make([]string, 0, len(fLst)) // list of submission stamps
	totalRes := RunRes{}
	ownRes := RunRes{}
	localOwnRes := RunRes{}

	for _, f := range fLst {

		// get submission stamp, oms instance and resources
		stamp, oms, mn, dgst, isMpi, cpu, mem, _ := parseActivePath(f)
		if stamp == "" || oms == "" || mn == "" || dgst == "" {
			continue // file name is not a job file name
		}
		if _, ok := omsActive[oms]; !ok {
			continue // skip: oms instance inactive
		}

		// collect total resource usage
		if isMpi {
			totalRes.Cpu = totalRes.Cpu + cpu
			totalRes.Mem = totalRes.Mem + mem
		}

		if oms != theCfg.omsName {
			continue // done with this job: it is other oms instance
		}

		// this is own job: job to run in current oms instance on MPI cluster or localhost server
		if isMpi {
			ownRes.Cpu = ownRes.Cpu + cpu
			ownRes.Mem = ownRes.Mem + mem
		} else {
			localOwnRes.Cpu = localOwnRes.Cpu + cpu
			localOwnRes.Mem = localOwnRes.Mem + mem
		}

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
	return subStamps, totalRes, ownRes, localOwnRes
}

// insert run job into queue job map: map job file submission stamp to file content (run job)
func updateQueueJobs(
	fLst []string, jobMap map[string]queueJobFile, aRes RunRes, isMpiLimit bool, localRes RunRes, omsActive map[string]int64,
) (
	[]string, int, int, RunRes, RunRes, RunRes) {

	nFiles := len(fLst)
	maxPos := jobPositionDefault + 1
	minPos := jobPositionDefault - 1

	// queue file name parts
	type fileH struct {
		fileIdx  int    // source file index
		oms      string // instance name
		stamp    string // submission stamp
		position int    // queue position: file name part
		allQi    int    // queue position: index in combined queue (queues from all oms instances)
		res      RunRes // resources required to run the model
		preRes   RunRes // resources required for queue jobs before this job
		isOver   bool   // if true then resources required are exceeding total resource(s) limit(s)
	}
	type omsQ struct {
		top int     // current top queue file index
		q   []fileH // instance queue
	}
	allQ := make(map[string]omsQ, nFiles) // queue for each oms instance

	// for each oms instance append MPI job files to job queue
	for k, f := range fLst {

		// get submission stamp, oms instance and queue position
		stamp, oms, mn, dgst, isMpi, cpu, mem, pos := parseQueuePath(f)
		if stamp == "" || oms == "" || mn == "" || dgst == "" {
			continue // file name is not a job file name
		}
		if maxPos < pos {
			maxPos = pos
		}
		if minPos > pos {
			minPos = pos
		}
		if !isMpi {
			continue // this is not MPI cluster job
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
			isOver:   isMpiLimit && (cpu > aRes.Cpu || (aRes.Mem > 0 && mem > aRes.Mem)),
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
	nextQueueIdx := 0    // global queue index position

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

		// collect total resource usage and if top job is not exceeding available resources then assign globdl queue index position
		preRes := totalRes
		if !aq.q[aq.top].isOver {
			totalRes.Cpu = totalRes.Cpu + aq.q[aq.top].res.Cpu
			totalRes.Mem = totalRes.Mem + aq.q[aq.top].res.Mem
			nextQueueIdx++
			aq.q[aq.top].allQi = nextQueueIdx
		}
		aq.q[aq.top].preRes = preRes // resource used by jobs before current

		// move top of this queue to the next position and update current top job
		aq.top++
		allQ[topOms] = aq
	}

	// update current instance job map, queue files and resources
	// append localhost job queue for current oms instance
	qKeys := make([]string, 0, nFiles)
	usedLocal := RunRes{}

	for _, f := range fLst {

		// get submission stamp, oms instance and queue position
		stamp, oms, mn, dgst, isMpi, cpu, mem, pos := parseQueuePath(f)
		if stamp == "" || oms == "" || mn == "" || dgst == "" {
			continue // file name is not a job file name
		}
		if isMpi || oms != theCfg.omsName {
			continue // skip: this is MPI model run or it is local run from other oms instance
		}

		preRes := usedLocal
		isOver := (localRes.Cpu > 0 && cpu > localRes.Cpu) || (localRes.Mem > 0 && mem > localRes.Mem)

		if !isOver {
			usedLocal.Cpu = usedLocal.Cpu + cpu
			usedLocal.Mem = usedLocal.Mem + mem
		}
		qKeys = append(qKeys, stamp)

		// if this file already in the queue jobs map then update resources
		if jc, ok := jobMap[stamp]; ok {

			jc.filePath = f
			jc.position = pos
			jc.preRes = preRes
			jc.IsOverLimit = isOver
			jc.QueuePos = len(qKeys)
			jobMap[stamp] = jc // update exsiting job in the queue with current resources info
			continue
		}
		// else create run state from job file and insert into the queue map
		var jc RunJob

		isOk, err := helper.FromJsonFile(f, &jc)
		if err != nil {
			omppLog.Log(err)
			jobMap[stamp] = queueJobFile{runJobFile: runJobFile{filePath: f, isError: true}}
		}
		if !isOk || err != nil {
			continue // file does not exist or invalid
		}
		jc.IsOverLimit = isOver
		jc.QueuePos = len(qKeys)

		// add new job into queue jobs map
		jobMap[stamp] = queueJobFile{
			runJobFile: runJobFile{RunJob: jc, filePath: f},
			position:   pos,
			preRes:     preRes,
		}
	}

	// update current instance job map, queue files and resources
	// append MPI jobs queue for current oms instance
	ownRes := RunRes{}

	ownQ, isOwn := allQ[theCfg.omsName]
	if !isOwn {
		return qKeys, maxPos, minPos, totalRes, ownRes, usedLocal // there are no MPI jobs for current oms instance
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
			jc.QueuePos = f.allQi
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
		jc.QueuePos = f.allQi

		// add new job into queue jobs map
		jobMap[f.stamp] = queueJobFile{
			runJobFile: runJobFile{RunJob: jc, filePath: fLst[f.fileIdx]},
			position:   f.position,
			preRes:     f.preRes,
		}
	}

	return qKeys, maxPos, minPos, totalRes, ownRes, usedLocal
}

// read job service state and computational servers definition from job.ini
func initJobComputeState(jobIniPath string, updateTs time.Time, computeState map[string]computeItem) JobServiceState {

	jsState := JobServiceState{
		IsQueuePaused:     isPausedJobQueue(),
		JobUpdateDateTime: helper.MakeDateTime(updateTs),
		maxStartTime:      serverTimeoutDefault,
		maxStopTime:       serverTimeoutDefault,
	}

	// read available resources limits and computational servers configuration from job.ini
	if jobIniPath == "" || fileExist(jobIniPath) != nil {
		return jsState
	}

	opts, err := config.FromIni(jobIniPath, theCfg.codePage)
	if err != nil {
		omppLog.Log(err)
		return jsState
	}
	nowTs := updateTs.UnixMilli()

	// total available resources limits and timeouts
	jsState.LocalRes.Cpu = opts.Int("Common.LocalCpu", 0)    // localhost unlimited cpu cores by default
	jsState.LocalRes.Mem = opts.Int("Common.LocalMemory", 0) // localhost unlimited memory by default

	jsState.maxIdleTime = opts.Int("Common.IdleTimeout", 0) // never stop servers by default
	jsState.maxStartTime = opts.Int("Common.StartTimeout", serverTimeoutDefault)
	jsState.maxStopTime = opts.Int("Common.StopTimeout", serverTimeoutDefault)

	// MPI jobs process, threads and hostfile config
	jsState.hostFile.maxThreads = opts.Int("Common.MpiMaxThreads", 0) // max number of modelling threads per MPI process, zero means unlimited
	jsState.hostFile.hostName = opts.String("hostfile.HostName")
	jsState.hostFile.cpuCores = opts.String("hostfile.CpuCores")
	jsState.hostFile.rootLine = opts.String("hostfile.RootLine")
	jsState.hostFile.hostLine = opts.String("hostfile.HostLine")
	jsState.hostFile.dir = opts.String("hostfile.HostFileDir")

	jsState.hostFile.isUse = jsState.hostFile.dir != "" &&
		jsState.hostFile.dir != "." && jsState.hostFile.dir != ".." &&
		jsState.hostFile.dir != "./" && jsState.hostFile.dir != "../" && jsState.hostFile.dir != "/" &&
		(jsState.hostFile.rootLine != "" || jsState.hostFile.hostLine != "")

	if jsState.hostFile.isUse {
		jsState.hostFile.isUse = dirExist(jsState.hostFile.dir) == nil
	}

	// default settings for compute servers or clusters

	splitOpts := func(key, sep string) []string {
		v := strings.Split(opts.String(key), sep)
		if len(v) <= 0 || len(v) == 1 && v[0] == "" {
			return []string{}
		}
		return v
	}

	exeStart := opts.String("Common.StartExe")
	exeStop := opts.String("Common.StopExe")
	argsBreak := opts.String("Common.ArgsBreak")
	argsStart := splitOpts("Common.StartArgs", argsBreak)
	argsStop := splitOpts("Common.StopArgs", argsBreak)

	// compute servers or clusters defaults
	srvNames := splitOpts("Common.Servers", ",")

	for k, s := range srvNames {

		srvNames[k] = strings.TrimSpace(s)
		if srvNames[k] == "" {
			continue // skip empty name
		}

		// add or clean existing computational server state
		cs, ok := computeState[srvNames[k]]
		if !ok {
			cs = computeItem{
				name:       srvNames[k],
				lastUsedTs: nowTs,
				startArgs:  []string{},
				stopArgs:   []string{},
			}
		}
		cs.usedRes.Cpu = 0 // updated as sum of comp-used files
		cs.usedRes.Mem = 0 // updated as sum of comp-used files
		cs.ownRes.Cpu = 0  // updated as sum of comp-used files
		cs.ownRes.Mem = 0  // updated as sum of comp-used files
		cs.errorCount = 0  // updated as count of comp-start, comp-stop, comp-error files

		cs.totalRes.Cpu = opts.Int(cs.name+".Cpu", 0)    // unlimited cpu cores by default
		cs.totalRes.Mem = opts.Int(cs.name+".Memory", 0) // unlimited memory cores by default

		cs.startExe = opts.String(cs.name + ".StartExe")
		if cs.startExe == "" {
			cs.startExe = exeStart // default executable to start server
		}

		cs.startArgs = splitOpts(cs.name+".StartArgs", argsBreak)
		if len(cs.startArgs) <= 0 {
			cs.startArgs = append(argsStart, cs.name) // default start arguments and server name as last argument
		}

		cs.stopExe = opts.String(cs.name + ".StopExe")
		if cs.stopExe == "" {
			cs.stopExe = exeStop // default executable to stop server
		}

		cs.stopArgs = splitOpts(cs.name+".StopArgs", argsBreak)
		if len(cs.stopArgs) <= 0 {
			cs.stopArgs = append(argsStop, cs.name) // default stop arguments and server name as last argument
		}
		computeState[cs.name] = cs
	}

	// remove compute servers or clusters which are no longer exist
	sort.Strings(srvNames)
	for name := range computeState {
		k := sort.SearchStrings(srvNames, name)
		if k < 0 || k >= len(srvNames) || srvNames[k] != name {
			delete(computeState, name)
		}
	}

	// update total available MPI resources
	for _, cs := range computeState {
		jsState.MpiRes.Cpu += cs.totalRes.Cpu
		jsState.MpiRes.Mem += cs.totalRes.Mem
	}

	return jsState
}

// Update computational serveres or clusters map.
// For each server or cluster find current state (ready, start, stop or power off),
// total computational resources (cpu and memory), used resources and avaliable resources.
func updateComputeState(
	computeState map[string]computeItem,
	omsActive map[string]int64,
	nowTs int64,
	maxStartTime int,
	maxStopTime int,
	compReadyFiles, compStartFiles, compStopFiles, compErrorFiles, compUsedFiles []string,
) {

	// ready compute resources: powered on servers or clusters
	readyNames := []string{}

	for _, f := range compReadyFiles {

		// get server name
		name := parseCompReadyPath(f)
		if name == "" {
			continue // skip: this is not a compute server ready file
		}
		cs, ok := computeState[name]
		if !ok {
			continue // this server does not exist anymore
		}
		readyNames = append(readyNames, name)

		// update server state to ready
		cs.state = "ready"
		cs.errorCount = 0

		if cs.lastUsedTs <= 0 {
			cs.lastUsedTs = nowTs
		}
		if cs.lastStartTs <= 0 {
			cs.lastStartTs = nowTs
		}
		computeState[name] = cs
	}

	// clear ready state for server or cluster if ready state file does not exist
	for name, cs := range computeState {

		if cs.state == "ready" {
			isReady := false
			for k := 0; !isReady && k < len(readyNames); k++ {
				isReady = readyNames[k] == name
			}
			if !isReady {
				cs.state = "" // power off state
				computeState[name] = cs
			}
		}
	}

	// start up servers or clusters: check startup timeout
	for _, f := range compStartFiles {

		// get server name and time stamp
		name, _, ts := parseCompStatePath(f, "start")
		if name == "" {
			continue // skip: this is not a compute server state file
		}
		cs, ok := computeState[name]
		if !ok {
			continue // this server does not exist anymore
		}

		// update server state to start or detect error
		if (cs.state == "" || cs.state == "start" || cs.state == "ready") && (ts+int64(maxStartTime)) >= nowTs {

			cs.state = "start"
			if cs.lastStartTs < ts {
				cs.lastStartTs = ts
			}
		} else {
			if cs.lastErrorTs < ts {
				cs.lastErrorTs = ts
			}
			cs.errorCount++ // start timeout error
		}
		computeState[name] = cs
	}

	// stop servers or clusters: check shutdown timeout
	for _, f := range compStopFiles {

		// get server name and time stamp
		name, _, ts := parseCompStatePath(f, "stop")
		if name == "" {
			continue // skip: this is not a compute server state file
		}
		cs, ok := computeState[name]
		if !ok {
			continue // this server does not exist anymore
		}

		// update server state to stop or detect error
		if (cs.state == "ready" || cs.state == "stop" || cs.state == "") && (ts+int64(maxStopTime)) >= nowTs {

			cs.state = "stop"
			if cs.lastStopTs < ts {
				cs.lastStopTs = ts
			}
		} else {
			if cs.lastErrorTs < ts {
				cs.lastErrorTs = ts
			}
			cs.errorCount++ // stop timeout error
		}
		computeState[name] = cs
	}

	// server or cluster in "error" state
	for _, f := range compErrorFiles {

		// get server name and time stamp
		name, _, ts := parseCompStatePath(f, "error")
		if name == "" {
			continue // skip: this is not a compute server state file
		}
		cs, ok := computeState[name]
		if !ok {
			continue // this server does not exist anymore
		}

		// count server errors
		if cs.lastErrorTs < ts {
			cs.lastErrorTs = ts
		}
		cs.errorCount++ // error logged for that server or cluster

		computeState[name] = cs
	}

	// set error state for server or cluster if error count exceed max error limit
	for name, cs := range computeState {

		if cs.errorCount > maxComputeErrors {
			cs.state = "error"
			computeState[name] = cs
		}
	}

	// servers or clusters used for model runs: sum up resoureces used by current oms instance and all oms inatances
	for _, f := range compUsedFiles {

		// get server name and time stamp
		name, _, oms, cpu, mem := parseCompUsedPath(f)
		if name == "" || oms == "" {
			continue // skip: this is not a compute server state file
		}
		if _, ok := omsActive[oms]; !ok {
			continue // oms instance not active
		}
		cs, ok := computeState[name]
		if !ok {
			continue // this server does not exist anymore
		}

		// update resources used by model runs
		cs.usedRes.Cpu += cpu
		cs.usedRes.Mem += mem
		if oms == theCfg.omsName {
			cs.ownRes.Cpu += cpu
			cs.ownRes.Mem += mem
		}
		cs.lastUsedTs = nowTs

		computeState[name] = cs
	}
}

// scan model run queue, start model runs, start or stop MPI servers
func scanRunJobs(doneC <-chan bool) {
	if !theCfg.isJobControl {
		return // job control disabled: no queue
	}

	lastStartStopTs := time.Now().UnixMilli() // last time when start and stop of computational servers done

	for {
		// get job from the queue and run
		if job, isFound, qPath, compUse, hf, e := theRunCatalog.selectJobFromQueue(); isFound && e == nil {

			_, e = theRunCatalog.runModel(job, qPath, hf, compUse)
			if e != nil {
				omppLog.Log(e)
			}
		} else {
			if e != nil {
				omppLog.Log(e)
				if qPath != "" {
					moveJobQueueToFailed(qPath, job.SubmitStamp, job.ModelName, job.ModelDigest) // can not run this job: remove from the queue
				}
			}
		}

		// check if this is a time to do start or stop computational servers:
		// interval must be at least double of job files scan to make sure server state files updated
		if lastStartStopTs+computeStartStopInterval < time.Now().UnixMilli() {

			// start computational servers or clusters
			startNames, startMax, startExes, startArgs := theRunCatalog.selectToStartCompute()

			for k := range startNames {
				if startExes[k] != "" {
					go doStartStopCompute(startNames[k], "start", startExes[k], startArgs[k], startMax)
				} else {
					doStartOnceCompute(startNames[k]) // special case: server always ready
				}
			}

			// stop computational servers or clusters
			stopNames, stopMax, stopExes, stopArgs := theRunCatalog.selectToStopCompute()

			for k := range stopNames {
				if stopExes[k] != "" {
					go doStartStopCompute(stopNames[k], "stop", stopExes[k], stopArgs[k], stopMax)
				} else {
					doStopCleanupCompute(stopNames[k]) // special case: server never stop
				}
			}

			lastStartStopTs = time.Now().UnixMilli()
		}

		// wait for doneC or sleep
		if doExitSleep(jobQueueScanInterval, doneC) {
			return
		}
	}
}

// start (or stop) computational server or cluster and create (or delete) ready state file
func doStartStopCompute(name, state, exe string, args []string, maxTime int) {

	omppLog.Log(state, ": ", name)

	// create server start or stop file to signal server state
	sf := createCompStateFile(name, state)
	if sf == "" {
		omppLog.Log("FAILED to create state file: ", state, " ", name)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxTime)*time.Second)
	defer cancel()

	// make a command, run it and return combined output
	out, err := exec.CommandContext(ctx, exe, args...).CombinedOutput()
	if len(out) > 0 {
		omppLog.Log(string(out))
	}

	// create or delete server ready file and delete state file
	isOk := false
	if err == nil {

		readyPath := compReadyPath(name)
		if state == "start" {
			isOk = fileCreateEmpty(false, readyPath)
			if !isOk {
				omppLog.Log("FAILED to create server ready file: ", readyPath)
			}
		} else {
			isOk = fileDeleteAndLog(false, readyPath)
			if !isOk {
				omppLog.Log("FAILED to delete server ready file: ", readyPath)
			}
		}

		if isOk {
			okStart := deleteCompStateFiles(name, "start")
			okStop := deleteCompStateFiles(name, "stop")
			okErr := deleteCompStateFiles(name, "error")
			isOk = okStart && okStop && okErr
		}
		if isOk {
			omppLog.Log("Done: ", state, " ", name)
		}

	} else {
		if ctx.Err() == context.DeadlineExceeded {
			omppLog.Log("ERROR server timeout: ", state, " ", name)
		}
		omppLog.Log("Error: ", err)
		omppLog.Log("FAILED: ", state, " ", name)
	}

	// remove this server from startup or shutdown list
	if state == "start" {
		theRunCatalog.startupCompleted(isOk, name)
	} else {
		theRunCatalog.shutdownCompleted(isOk, name)
	}
}

// start computational server, special case: if server always ready then only create ready state file
func doStartOnceCompute(name string) {

	omppLog.Log("Start: ", name)

	readyPath := compReadyPath(name)
	isOk := fileCreateEmpty(false, readyPath)
	if !isOk {
		omppLog.Log("FAILED to create server ready file: ", readyPath)
	}

	if isOk {
		okStart := deleteCompStateFiles(name, "start")
		okStop := deleteCompStateFiles(name, "stop")
		okErr := deleteCompStateFiles(name, "error")
		isOk = okStart && okStop && okErr
	}
	if isOk {
		omppLog.Log("Done start of: ", name)
	}
}

// stop computational server, special case: if server never stop then only cleanup state files
func doStopCleanupCompute(name string) {

	deleteCompStateFiles(name, "start")
	deleteCompStateFiles(name, "stop")
	deleteCompStateFiles(name, "error")
}
