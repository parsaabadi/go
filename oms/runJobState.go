// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// scan job control directories to read and update job lists: queue, active and history
func scanStateJobs(doneC <-chan bool) {
	if !theCfg.isJobControl {
		return // job control disabled
	}

	omsTickPath, _ := makeOmsTick() // job processing started at this oms instance
	nTick := 0

	// path to job.ini: available resources limits and computational servers configuration
	jobIniPath := filepath.Join(theCfg.jobDir, "job.ini")

	// model run files in queue, active runs, run history
	queuePtrn := filepath.Join(theCfg.jobDir, "queue") + string(filepath.Separator) + "*-#-*-#-*-#-*-#-*-#-cpu-#-*-#-mem-#-*.json"
	activePtrn := filepath.Join(theCfg.jobDir, "active") + string(filepath.Separator) + "*-#-*-#-*-#-*-#-*-#-cpu-#-*-#-mem-#-*.json"
	historyPtrn := filepath.Join(theCfg.jobDir, "history") + string(filepath.Separator) + "*-#-" + theCfg.omsName + "-#-*.json"

	// oms instances heart beat and oms instance queue paused files:
	// if oms instance file does not updated more than 1 minute then oms instance is dead
	// oms instance heart beat tick:  oms-#-_4040-#-2022_07_08_23_45_12_123-#-1257894000000
	// oms instance job queue paused: jobs.queue-#-_4040-#-paused
	omsTickPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "oms-#-*-#-*-#-*"
	omsPausedPtrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "jobs.queue-#-*-#-paused"

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
	omsPaused := map[string]bool{}
	computeState := map[string]computeItem{}
	hostByCpu := []string{} // names of computational servers or clusters sorted by available CPU cores
	hostByMem := []string{} // names of computational servers or clusters sorted by available memory

	for {
		// get jobs service state and computational resources state: servers or clustres definition
		updateTs := time.Now()
		nowTs := updateTs.UnixMilli()

		jsState, cfgRes := initJobComputeState(jobIniPath, updateTs, computeState)

		queueFiles := filesByPattern(queuePtrn, "Error at queue job files search")
		activeFiles := filesByPattern(activePtrn, "Error at active job files search")
		historyFiles := filesByPattern(historyPtrn, "Error at history job files search")
		omsTickFiles := filesByPattern(omsTickPtrn, "Error at oms heart beat files search")
		omsPausedFiles := filesByPattern(omsPausedPtrn, "Error at queue paused files search")
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

		// update oms instances paused status
		clear(omsPaused)

		for _, fp := range omsPausedFiles {

			oms := parseQueuePausedPath(fp)
			if oms != "" {
				omsPaused[oms] = true
			}
		}

		// computational resources state
		// for each server or cluster detect current state: ready, start, stop or power off
		// total computational resources (cpu and memory), used resources and avaliable resources
		updateComputeState(
			computeState,
			omsActive,
			nowTs,
			jsState.maxStartTime,
			jsState.maxStopTime,
			jsState.maxComputeErrors,
			compReadyFiles, compStartFiles, compStopFiles, compErrorFiles, compUsedFiles)

		jsState.MpiErrorRes = ComputeRes{}
		hostByCpu = hostByCpu[:0]
		hostByMem = hostByMem[:0]

		for _, cs := range computeState {
			if cs.state == "error" {
				jsState.MpiErrorRes.Cpu += cs.totalRes.Cpu
				jsState.MpiErrorRes.Mem += cs.totalRes.Mem
			} else {
				hostByCpu = append(hostByCpu, cs.name)
				hostByMem = append(hostByMem, cs.name)
			}
		}

		// sort computational servers:
		//   if ready then use where more cpu and memory
		//   if not ready then use where less errors and longest unused time
		cmpHost := func(isByMem bool, nameLst []string) func(i, j int) bool {

			return func(i, j int) bool {
				ics := computeState[nameLst[i]]
				iCpu := ics.totalRes.Cpu - ics.usedRes.Cpu
				iMem := ics.totalRes.Mem - ics.usedRes.Mem
				jcs := computeState[nameLst[j]]
				jCpu := jcs.totalRes.Cpu - jcs.usedRes.Cpu
				jMem := jcs.totalRes.Mem - jcs.usedRes.Mem

				switch {
				case ics.state == "ready" && jcs.state == "ready":
					if !isByMem {
						return iCpu > jCpu || iCpu == jCpu && iMem > jMem
					}
					return iMem > jMem || iMem == jMem && iCpu > jCpu
				case ics.state == "ready" && jcs.state != "ready":
					return true
				case ics.state != "ready" && jcs.state == "ready":
					return false
				}
				if iCpu != jCpu || iMem != jMem {
					if !isByMem {
						return iCpu > jCpu || iCpu == jCpu && iMem > jMem
					}
					return iMem > jMem || iMem == jMem && iCpu > jCpu
				}
				return ics.errorCount < jcs.errorCount ||
					ics.errorCount == jcs.errorCount && ics.lastUsedTs < jcs.lastUsedTs ||
					ics.errorCount == jcs.errorCount && ics.lastUsedTs == jcs.lastUsedTs && ics.name < jcs.name
			}
		}
		sort.SliceStable(hostByCpu, cmpHost(false, hostByCpu))
		sort.SliceStable(hostByMem, cmpHost(true, hostByMem))

		mpiRes := ComputeRes{}
		isMpiLimit := false

		if len(computeState) <= 0 {

			mpiRes = jsState.LocalRes
			isMpiLimit = jsState.LocalRes.Cpu > 0
		} else {

			isMpiLimit = true
			mpiRes.Cpu = jsState.MpiRes.Cpu - jsState.MpiErrorRes.Cpu
			mpiRes.Mem = jsState.MpiRes.Mem - jsState.MpiErrorRes.Mem
		}

		// model runs
		//
		// parse active files, use unlimited resources for already active jobs
		aKeys, aTotal, aOwn, aLocal := updateActiveJobs(activeFiles, activeJobs, omsActive)

		// parse queue files and re-build model runs queue
		sort.Strings(queueFiles)
		qKeys, maxPos, minPos, qTotal, qOwn, qLocal, firstHostUse := updateQueueJobs(
			queueFiles,
			queueJobs,
			mpiRes,
			isMpiLimit,
			jsState.MpiMaxThreads,
			jsState.LocalRes,
			hostByCpu,
			hostByMem,
			computeState,
			omsActive,
			jsState.IsAllQueuePaused,
			omsPaused,
		)

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
				RunTitle:    getJobRunTitle(f),
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

		jsc := theRunCatalog.updateRunJobs(jsState, computeState, firstHostUse, cfgRes, queueJobs, activeJobs, historyJobs)
		jobStateWrite(*jsc)

		// update oms heart beat file
		nTick++
		if nTick%7 == 0 {
			omsTickPath, _ = makeOmsTick()
		}

		// wait for doneC or sleep
		if isExitSleep(jobScanInterval, doneC) {
			break
		}
	}

	fileDeleteAndLog(true, omsTickPath) // try to remove oms heart beat file, this code may never be executed due to race at shutdown
}

// insert run job into job map: map job file submission stamp to file content (run job)
func updateActiveJobs(fLst []string, jobMap map[string]runJobFile, omsActive map[string]int64) ([]string, ComputeRes, ComputeRes, ComputeRes) {

	subStamps := make([]string, 0, len(fLst)) // list of submission stamps
	totalRes := ComputeRes{}
	ownRes := ComputeRes{}
	localOwnRes := ComputeRes{}

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
	fLst []string,
	jobMap map[string]queueJobFile,
	mpiRes ComputeRes,
	isMpiLimit bool,
	mpiMaxTh int,
	localRes ComputeRes,
	hostByCpu []string,
	hostByMem []string,
	computeState map[string]computeItem,
	omsActive map[string]int64,
	isAllPaused bool,
	omsPaused map[string]bool,
) (
	[]string, int, int, ComputeRes, ComputeRes, ComputeRes, jobHostUse) {

	nFiles := len(fLst)
	maxPos := jobPositionDefault + 1
	minPos := jobPositionDefault - 1

	// queue file name parts, position in the queue and resources
	type qFileHdr struct {
		fileIdx  int    // source file index
		oms      string // instance name
		stamp    string // submission stamp
		position int    // queue position: file name part
		allQPos  int    // queue position: one based index in combined queue (queues from all oms instances)
		res      RunRes // resources required to run the model
		isPaused bool   // if true then job queue is paused
		isOver   bool   // if true then resources required are exceeding total resource(s) limit(s)
		isFirst  bool   // if true the it is the first job in global queue
	}
	type omsQ struct {
		top int        // current top queue file index
		q   []qFileHdr // instance queue files
	}
	qAll := make(map[string]omsQ, nFiles) // queue for each oms instance

	// for each oms instance append MPI job files to job queue
	for k, f := range fLst {

		// get submission stamp, oms instance and queue position
		stamp, oms, mn, dgst, isMpi, procCount, thCount, procMem, thMem, pos := parseQueuePath(f)
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
		cpu := procCount * thCount
		mem := memoryRunSize(procCount, thCount, procMem, thMem)

		// append to the instance queue
		qOms, ok := qAll[oms]
		if !ok {
			qOms = omsQ{q: make([]qFileHdr, 0, nFiles)}
		}
		qOms.q = append(qOms.q, qFileHdr{
			fileIdx:  k,
			oms:      oms,
			stamp:    stamp,
			position: pos,
			res: RunRes{
				ComputeRes: ComputeRes{
					Cpu: cpu,
					Mem: mem,
				},
				ProcessCount: procCount,
				ThreadCount:  thCount,
				ProcessMemMb: procMem,
				ThreadMemMb:  thMem,
			},
			isPaused: isAllPaused || omsPaused[oms],
			isOver:   (isMpiLimit && cpu > mpiRes.Cpu) || (mpiRes.Mem > 0 && mem > mpiRes.Mem),
		})
		qAll[oms] = qOms
	}

	// sort each job queue in order of position file name part and submission stamp
	for _, qOms := range qAll {
		sort.SliceStable(qOms.q, func(i, j int) bool {
			return qOms.q[i].position < qOms.q[j].position || qOms.q[i].position == qOms.q[j].position && qOms.q[i].stamp < qOms.q[j].stamp
		})
	}

	// sort oms instance names
	nOms := len(qAll)
	omsKeys := make([]string, nOms)
	n := 0
	for oms := range qAll {
		omsKeys[n] = oms
		n++
	}
	sort.Strings(omsKeys)

	// order combined queue jobs by:
	// position in the queue (position which user can adjust), by submission stamp and by instance name
	// inside of each oms instance queue jobs are ordered by position and submission stamps
	// for example, two queues of oms instances _4040 and _8080:
	//   _4040 [ {1234, 2022_08_17}, {4567, 2022_08_12} ]
	//   _8080 [ {1212, 2022_08_17}, {3434, 2022_08_16} ]
	// merged global queue result:
	//   [ {_4040, 1234, 2022_08_17}, {_8080, 1212, 2022_08_17}, {_4040, 4567, 2022_08_12}, {_8080, 3434, 2022_08_16} ]

	totalRes := ComputeRes{} // total resources required to serve all queues
	nextQueueIdx := 0        // global queue index position
	isFirstJob := true
	firstHostUse := jobHostUse{hostUse: []computeUse{}}

	for isAll := false; !isAll; {

		// find oms instance where curent job has minimal submission stamp
		// if there is the same stamp in multiple instances then use minimal oms instance name
		topStamp := ""
		topOms := ""
		isAll = true

		for k := 0; k < nOms; k++ {

			qOms := qAll[omsKeys[k]]
			if qOms.top >= len(qOms.q) {
				continue // all jobs in that queue are already processed
			}
			isAll = false

			if topOms == "" {
				topOms = omsKeys[k]
				topStamp = qOms.q[qOms.top].stamp
			} else {
				if qOms.q[qOms.top].stamp < topStamp {
					topOms = omsKeys[k]
					topStamp = qOms.q[qOms.top].stamp
				}
			}
		}
		if isAll {
			break // all jobs in all queues are sorted
		}

		qOms := qAll[topOms] // this queue contains minimal submission stamp at current queue top position

		// collect total resource usage
		// if current top job is not exceeding available resources then assign global queue index position
		if !qOms.q[qOms.top].isOver {

			totalRes.Cpu = totalRes.Cpu + qOms.q[qOms.top].res.Cpu
			totalRes.Mem = totalRes.Mem + qOms.q[qOms.top].res.Mem

			// check if there are any server(s) exists to run the job
			srcJhu := jobHostUse{oms: topOms, stamp: topStamp, res: qOms.q[qOms.top].res, hostUse: []computeUse{}}

			if qOms.q[qOms.top].res.Mem > 0 {
				qOms.q[qOms.top].isOver, _ = findComputeRes(srcJhu, false, mpiMaxTh, hostByMem, computeState)
			} else {
				qOms.q[qOms.top].isOver, _ = findComputeRes(srcJhu, false, mpiMaxTh, hostByCpu, computeState)
			}

			// if job queue not paused then allocate job to the servers and add servers to startup list
			if !qOms.q[qOms.top].isOver && !qOms.q[qOms.top].isPaused {

				isOver := false
				var dst jobHostUse
				if qOms.q[qOms.top].res.Mem > 0 {
					isOver, dst = findComputeRes(srcJhu, true, mpiMaxTh, hostByMem, computeState)
				} else {
					isOver, dst = findComputeRes(srcJhu, true, mpiMaxTh, hostByCpu, computeState)
				}

				// if this is the first job in global queue then save host ini servers
				if !isOver && isFirstJob {
					isFirstJob = false
					qOms.q[qOms.top].isFirst = true
					firstHostUse = dst
				}
			}

			if !qOms.q[qOms.top].isOver {
				nextQueueIdx++
				qOms.q[qOms.top].allQPos = nextQueueIdx // one based index in global queue
			}
		}

		// move top of this queue to the next position and update current top job
		qOms.top++
		qAll[topOms] = qOms
	}

	// update current instance job map, queue files and resources
	// append localhost job queue for current oms instance

	qKeys := make([]string, 0, nFiles) // model run submission stamps for current oms instance
	usedLocal := ComputeRes{}
	isOmsPaused := isAllPaused || omsPaused[theCfg.omsName] // if current oms instance is paused
	isFirstJob = true

	for _, f := range fLst {

		// get submission stamp, oms instance and queue position
		stamp, oms, mn, dgst, isMpi, procCount, thCount, procMem, thMem, pos := parseQueuePath(f)
		if stamp == "" || oms == "" || mn == "" || dgst == "" {
			continue // file name is not a job file name
		}
		if isMpi || oms != theCfg.omsName {
			continue // skip: this is MPI model run or it is from other oms instance
		}
		cpu := procCount * thCount
		mem := memoryRunSize(procCount, thCount, procMem, thMem)

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
			jc.QueuePos = len(qKeys)
			jc.isPaused = isOmsPaused
			jc.IsOverLimit = isOver
			jc.isFirst = !isOver && !isOmsPaused && isFirstJob
			jobMap[stamp] = jc // update exsiting job in the queue with current resources info

			if jc.isFirst {
				isFirstJob = false
			}
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
		jc.QueuePos = len(qKeys) // one-based position in local queue of the current oms instance

		// add new job into queue jobs map
		isFirst := !isOver && !isOmsPaused && isFirstJob

		jobMap[stamp] = queueJobFile{
			runJobFile: runJobFile{RunJob: jc, filePath: f},
			position:   pos,
			isPaused:   isOmsPaused,
			isFirst:    isFirst,
		}
		if isFirst {
			isFirstJob = false
		}
	}

	// update current instance job map, queue files and resources
	// append MPI jobs queue for current oms instance
	ownRes := ComputeRes{}

	ownQ, isOwn := qAll[theCfg.omsName]
	if !isOwn {
		return qKeys, maxPos, minPos, totalRes, ownRes, usedLocal, firstHostUse // there are no MPI jobs for current oms instance
	}

	for _, f := range ownQ.q {

		ownRes.Cpu = ownRes.Cpu + f.res.Cpu
		ownRes.Mem = ownRes.Mem + f.res.Mem

		qKeys = append(qKeys, f.stamp)

		// if this file already in the queue jobs map then update resources
		if jc, ok := jobMap[f.stamp]; ok {

			jc.filePath = fLst[f.fileIdx]
			jc.position = f.position
			jc.isPaused = isOmsPaused
			jc.IsOverLimit = f.isOver
			jc.isFirst = f.isFirst
			jc.QueuePos = f.allQPos
			jc.Res = f.res
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
		jc.QueuePos = f.allQPos
		jc.Res = f.res

		// add new job into queue jobs map
		jobMap[f.stamp] = queueJobFile{
			runJobFile: runJobFile{RunJob: jc, filePath: fLst[f.fileIdx]},
			position:   f.position,
			isPaused:   isOmsPaused,
			isFirst:    f.isFirst,
		}
	}

	return qKeys, maxPos, minPos, totalRes, ownRes, usedLocal, firstHostUse
}

// check if there are any server(s) exists to run the job and find additional servers to start
func findComputeRes(src jobHostUse, isUse bool, mpiMaxTh int, computeHost []string, computeState map[string]computeItem) (bool, jobHostUse) {

	// max threads per process: limited by job.ini MpiMaxThreads value and by Threads value from model run request
	maxTh := src.res.Cpu
	if mpiMaxTh > 0 && maxTh > mpiMaxTh {
		maxTh = mpiMaxTh
	}
	if src.res.ThreadCount > 0 && maxTh > src.res.ThreadCount {
		maxTh = src.res.ThreadCount
	}
	dst := src
	dst.hostUse = []computeUse{}

	// check if model run request not exceed cpu and memory available resources
	minMem := memoryRunSize(1, 1, src.res.ProcessMemMb, src.res.ThreadMemMb) // memory required to run single thread
	cSel := map[string]int{}                                                 // map server name to number of process to run
	nTh := maxTh
	nCpu := 0

	for _, cn := range computeHost {

		if nCpu >= src.res.Cpu {
			break // done: all cores allocated
		}

		if _, isSel := cSel[cn]; isSel {
			continue // this server already selected
		}
		cs := computeState[cn]
		if cs.state == "error" {
			continue // skip: failed server
		}

		// try to allocate cores for at least one process
		nc := cs.totalRes.Cpu
		aMem := cs.totalRes.Mem
		if isUse {
			nc = nc - cs.usedRes.Cpu
			aMem = aMem - cs.usedRes.Mem
		}
		if nc > nTh {
			nc = nTh
		}

		if nc <= 0 || minMem > 0 && aMem < minMem {
			continue // no free resources on that server
		}

		// limit core usage by memory
		if minMem > 0 {

			for ; nc > 0; nc-- {
				if aMem >= memoryRunSize(1, nc, src.res.ProcessMemMb, src.res.ThreadMemMb) {
					break
				}
			}
		}
		if nc <= 0 {
			continue // not enough cores or memory to run model single process single thread
		}
		nTh = nc // max possible number of threads to run on all servers

		// re-calculate cpu and memory usage for all allocated servers
		// clear previous process allocation for all servers
		for name := range cSel {
			cSel[name] = 0
		}
		cSel[cn] = 0 // add new server

		// allocate to each server max number of model processes with that thread count and memory size
		mp := memoryRunSize(1, nTh, src.res.ProcessMemMb, src.res.ThreadMemMb)
		nCpu = 0

		for name := range cSel {

			c := computeState[name].totalRes.Cpu
			m := computeState[name].totalRes.Mem
			if isUse {
				c = c - computeState[name].usedRes.Cpu
				m = m - computeState[name].usedRes.Mem
			}

			np := 1
			for ; nCpu+np*nTh < src.res.Cpu; np++ {
				if (np+1)*nTh > c || minMem > 0 && (np+1)*mp > m {
					break
				}
			}
			cSel[name] = np
			nCpu += np * nTh

			if nCpu >= src.res.Cpu { // all cores allocated
				break
			}
		}

		if nCpu >= src.res.Cpu { // all cores allocated
			break
		}
	}

	// check search result: if enough resources found
	isOver := nCpu < src.res.Cpu
	if isOver {
		return true, dst // over limit: not enough resources to run the job
	}

	// update cpu and memory usage
	dst.hostUse = make([]computeUse, 0, len(cSel))

	if isUse {
		mp := memoryRunSize(1, nTh, src.res.ProcessMemMb, src.res.ThreadMemMb)
		procCount := 0

		for j := range computeHost {

			if np, ok := cSel[computeHost[j]]; ok && np > 0 {

				c := np * nTh
				m := np * mp
				dst.hostUse = append(dst.hostUse,
					computeUse{name: computeHost[j], ComputeRes: ComputeRes{Cpu: c, Mem: m}},
				)
				procCount += np
			}
		}
		dst.res.ProcessCount = procCount
		dst.res.ThreadCount = nTh
		dst.res.Mem = procCount * mp // actual memory usage
	}

	return isOver, dst
}

// read job service state and computational servers definition from job.ini
func initJobComputeState(jobIniPath string, updateTs time.Time, computeState map[string]computeItem) (JobServiceState, []modelCfgRes) {

	jsState := JobServiceState{
		IsQueuePaused:     isPausedJobQueue(),
		IsAllQueuePaused:  isPausedJobAllQueue(),
		JobUpdateDateTime: helper.MakeDateTime(updateTs),
		maxStartTime:      serverTimeoutDefault,
		maxStopTime:       serverTimeoutDefault,
	}
	cfgRes := []modelCfgRes{}

	// read available resources limits and computational servers configuration from job.ini
	if jobIniPath == "" || !fileExist(jobIniPath) {
		return jsState, cfgRes
	}

	opts, err := config.FromIni(jobIniPath, theCfg.codePage)
	if err != nil {
		omppLog.Log(err)
		return jsState, cfgRes
	}
	nowTs := updateTs.UnixMilli()

	// total available resources limits and timeouts
	jsState.LocalRes.Cpu = opts.Int("Common.LocalCpu", 0)    // localhost unlimited cpu cores by default
	jsState.LocalRes.Mem = opts.Int("Common.LocalMemory", 0) // localhost unlimited memory by default

	jsState.maxIdleTime = 1000 * opts.Int64("Common.IdleTimeout", 0) // zero default timeout: never stop servers by default
	jsState.maxStartTime = 1000 * opts.Int64("Common.StartTimeout", serverTimeoutDefault)
	jsState.maxStopTime = 1000 * opts.Int64("Common.StopTimeout", serverTimeoutDefault)
	jsState.maxComputeErrors = opts.Int("Common.MaxErrors", maxComputeErrorsDefault)

	// MPI jobs process, threads and hostfile config
	jsState.MpiMaxThreads = opts.Int("Common.MpiMaxThreads", 0) // max number of modelling threads per MPI process, zero means unlimited
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
		jsState.hostFile.isUse = dirExist(jsState.hostFile.dir)
	}

	// default settings for compute servers or clusters
	//
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

		cs.totalRes.Cpu = opts.Int(cs.name+".Cpu", 1)    // one cpu core by default
		cs.totalRes.Mem = opts.Int(cs.name+".Memory", 0) // unlimited memory by default

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

	// model resources requirements
	mpLst := splitOpts("Common.Models", ",")
	cfgRes = make([]modelCfgRes, 0, len(mpLst))

	for k := range mpLst {

		p := strings.TrimSpace(mpLst[k])
		if p == "" {
			continue // skip empty path
		}
		mp := opts.Int(p+".MemoryProcessMb", 0) // unlimited memory by default
		if mp < 0 {
			mp = 0
		}
		mt := opts.Int(p+".MemoryThreadMb", 0) // unlimited memory by default
		if mt < 0 {
			mt = 0
		}
		cfgRes = append(cfgRes, modelCfgRes{Path: p, ProcessMemMb: mp, ThreadMemMb: mt})
	}

	return jsState, cfgRes
}

// Update computational serveres or clusters map.
// For each server or cluster find current state (ready, start, stop or power off),
// total computational resources (cpu and memory), used resources and avaliable resources.
func updateComputeState(
	computeState map[string]computeItem,
	omsActive map[string]int64,
	nowTs int64,
	maxStartTime int64,
	maxStopTime int64,
	maxComputeErrors int,
	compReadyFiles, compStartFiles, compStopFiles, compErrorFiles, compUsedFiles []string,
) {

	// clear state for server or cluster: initial state is "" power off
	for name, cs := range computeState {

		cs.state = "" // power off state
		computeState[name] = cs
	}

	// ready compute resources: powered on servers or clusters
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

		// update server state to ready
		cs.state = "ready"
		cs.errorCount = 0

		if cs.lastUsedTs <= 0 {
			cs.lastUsedTs = nowTs
		}
		computeState[name] = cs
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
		if (cs.state == "" || cs.state == "start" || cs.state == "ready") && (ts+maxStartTime) >= nowTs {
			cs.state = "start"
		} else {
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
		if (cs.state == "ready" || cs.state == "stop" || cs.state == "") && (ts+maxStopTime) >= nowTs {
			cs.state = "stop"
		} else {
			cs.errorCount++ // stop timeout error
		}
		computeState[name] = cs
	}

	// server or cluster in "error" state
	for _, f := range compErrorFiles {

		// get server name and time stamp
		name, _, _ := parseCompStatePath(f, "error")
		if name == "" {
			continue // skip: this is not a compute server state file
		}
		cs, ok := computeState[name]
		if !ok {
			continue // this server does not exist anymore
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

	// servers or clusters used for model runs: sum up resources used by current oms instance and all oms inatances
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
		if cs.lastUsedTs < nowTs {
			cs.lastUsedTs = nowTs
		}

		computeState[name] = cs
	}
}
