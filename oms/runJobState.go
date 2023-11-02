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

		jsState, mpRes := initJobComputeState(jobIniPath, updateTs, computeState)

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
			jsState.maxComputeErrors,
			compReadyFiles, compStartFiles, compStopFiles, compErrorFiles, compUsedFiles)

		jsState.MpiErrorRes = RunRes{}

		for _, cs := range computeState {
			if cs.state == "error" {
				jsState.MpiErrorRes.Cpu += cs.totalRes.Cpu
				jsState.MpiErrorRes.Mem += cs.totalRes.Mem
			}
		}

		mpiRes := RunRes{}
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

		// parse queue files
		sort.Strings(queueFiles)
		qKeys, maxPos, minPos, qTotal, qOwn, qTop, qLocal := updateQueueJobs(queueFiles, queueJobs, mpiRes, isMpiLimit, jsState.LocalRes, omsActive)

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
		jsState.topQueueRes = qTop
		jsState.LocalQueueRes = qLocal
		jsState.jobLastPosition = maxPos
		jsState.jobFirstPosition = minPos

		jsc := theRunCatalog.updateRunJobs(jsState, computeState, mpRes, queueJobs, activeJobs, historyJobs)
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
	fLst []string, jobMap map[string]queueJobFile, mpiRes RunRes, isMpiLimit bool, localRes RunRes, omsActive map[string]int64,
) (
	[]string, int, int, RunRes, RunRes, RunRes, RunRes) {

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
			isOver:   (isMpiLimit && cpu > mpiRes.Cpu) || (mpiRes.Mem > 0 && mem > mpiRes.Mem),
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
	topRes := RunRes{}   // resources required for to run first job in all queues
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

		// collect total resource usage
		// if current top job is not exceeding available resources then assign global queue index position
		// if current top job is a globally first job then store top job resources
		preRes := totalRes
		if !aq.q[aq.top].isOver {

			totalRes.Cpu = totalRes.Cpu + aq.q[aq.top].res.Cpu
			totalRes.Mem = totalRes.Mem + aq.q[aq.top].res.Mem

			if preRes.Cpu <= 0 && preRes.Mem <= 0 { // current job is first job in global queue
				topRes = totalRes
			}
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
		return qKeys, maxPos, minPos, totalRes, ownRes, topRes, usedLocal // there are no MPI jobs for current oms instance
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

	return qKeys, maxPos, minPos, totalRes, ownRes, topRes, usedLocal
}

// read job service state and computational servers definition from job.ini
func initJobComputeState(jobIniPath string, updateTs time.Time, computeState map[string]computeItem) (JobServiceState, map[string]modelRunRes) {

	jsState := JobServiceState{
		IsQueuePaused:     isPausedJobQueue(),
		IsAllQueuePaused:  isPausedJobAllQueue(),
		JobUpdateDateTime: helper.MakeDateTime(updateTs),
		maxStartTime:      serverTimeoutDefault,
		maxStopTime:       serverTimeoutDefault,
	}
	mpRes := map[string]modelRunRes{}

	// read available resources limits and computational servers configuration from job.ini
	if jobIniPath == "" || !fileExist(jobIniPath) {
		return jsState, mpRes
	}

	opts, err := config.FromIni(jobIniPath, theCfg.codePage)
	if err != nil {
		omppLog.Log(err)
		return jsState, mpRes
	}
	nowTs := updateTs.UnixMilli()

	// total available resources limits and timeouts
	jsState.LocalRes.Cpu = opts.Int("Common.LocalCpu", 0)    // localhost unlimited cpu cores by default
	jsState.LocalRes.Mem = opts.Int("Common.LocalMemory", 0) // localhost unlimited memory by default

	jsState.maxIdleTime = 1000 * opts.Int64("Common.IdleTimeout", 0) // zero deafult timeout: never stop servers by default
	jsState.maxStartTime = 1000 * opts.Int64("Common.StartTimeout", serverTimeoutDefault)
	jsState.maxStopTime = 1000 * opts.Int64("Common.StopTimeout", serverTimeoutDefault)
	jsState.maxComputeErrors = opts.Int("Common.MaxErrors", maxComputeErrorsDefault)

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
		mpRes[p] = modelRunRes{path: p, MemProcessMb: mp, MemThreadMb: mt}
	}

	return jsState, mpRes
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
		if cs.lastUsedTs < nowTs {
			cs.lastUsedTs = nowTs
		}

		computeState[name] = cs
	}
}
