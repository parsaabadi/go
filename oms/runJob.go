// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// get model run job from the queue
// return job to run, bool job found flag, queue job file path, list of server names to run the job, list of resources to use on each server
func (rsc *RunCatalog) selectJobFromQueue() (*RunJob, bool, string, []computeUse, hostIni, error) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if rsc.IsQueuePaused || len(rsc.queueKeys) <= 0 {
		return nil, false, "", []computeUse{}, hostIni{}, nil // queue is paused or empty
	}

	// resource available to rum MPI jobs from global MPI queue and form localhost queue of this oms instance jobs
	qLocal := RunRes{
		Cpu: rsc.LocalRes.Cpu - rsc.LocalActiveRes.Cpu,
		Mem: rsc.LocalRes.Mem - rsc.LocalActiveRes.Mem,
	}

	qMpi := RunRes{}
	isMpiLimit := false

	if len(rsc.computeState) <= 0 {

		qMpi = qLocal               // no computational servers, use localhost to run MPI jobs
		isMpiLimit = qLocal.Cpu > 0 // MPI cores limited if locahost CPU is limited
	} else {
		isMpiLimit = true // MPI cores are limited by sum of ready to use servers CPU cores

		for _, cs := range rsc.computeState {
			if cs.state == "ready" {
				qMpi.Cpu += cs.totalRes.Cpu
				qMpi.Mem += cs.totalRes.Mem
			}
		}
		qMpi.Cpu = qMpi.Cpu - (rsc.ComputeErrorRes.Cpu + rsc.ActiveTotalRes.Cpu)
		qMpi.Mem = qMpi.Mem - (rsc.ComputeErrorRes.Mem + rsc.ActiveTotalRes.Mem)
	}

	// first job in localhost non-MPI queue or global MPI queue where zero resources required for previous jobs
	// check if there are enough available resources to run the job
	stamp := ""
	for _, qKey := range rsc.queueKeys {

		jc, ok := rsc.queueJobs[qKey]

		if !ok || jc.isError || jc.IsOverLimit || jc.preRes.Cpu > 0 || jc.preRes.Mem > 0 {
			continue // skip invalid job or if required resources exceeding limits or if it is not a first job in global queue
		}
		isSel := false
		for j := 0; !isSel && j < len(rsc.selectedKeys); j++ {
			isSel = rsc.selectedKeys[j] == qKey
		}
		if isSel {
			continue // this job is already selected to run
		}

		// check avaliable resource: cpu cores and memory
		if !jc.IsMpi {

			if rsc.LocalRes.Cpu > 0 && jc.Res.Cpu > 0 && qLocal.Cpu < jc.Res.Cpu+jc.preRes.Cpu {
				continue // localhost cpu cores limited and job cpu limit set to non zero and localhost available cpu less than required cpu
			}
			if rsc.LocalRes.Mem > 0 && jc.Res.Mem > 0 && qLocal.Mem < jc.Res.Mem+jc.preRes.Mem {
				continue // localhost memory limited and job memory limit set to non zero and localhost available memory less than required memory
			}
		} else { // check resources available for MPI jobs

			if isMpiLimit && qMpi.Cpu < jc.Res.Cpu+jc.preRes.Cpu {
				continue // job cpu limit set to non zero and MPI cluster available cpu less than required cpu
			}
			if qMpi.Mem > 0 && jc.Res.Mem > 0 && qMpi.Mem < jc.Res.Mem+jc.preRes.Mem {
				continue // MPI cluster memory limited and job memory limit set to non zero and MPI cluster available memory less than required memory
			}
		}

		stamp = qKey // first job in queue which not yet selected to run and enough resources available to run
	}
	if stamp == "" {
		return nil, false, "", []computeUse{}, hostIni{}, nil // queue is empty or all jobs already selected to run
	}
	qj := rsc.queueJobs[stamp] // job found

	// if this is MPI job then
	// select servers where state is ready and max cpu cores available
	// until all required cores assigned to the servers
	compUse := []computeUse{}

	if qj.IsMpi && len(rsc.computeState) > 0 {

		nCpu := qj.Res.Cpu
		nMem := qj.Res.Mem

		for nCpu > 0 {

			// find serever with max cpu cores available
			cUse := computeUse{}

			for name, cs := range rsc.computeState {

				if cs.state != "ready" {
					continue // this server is not ready
				}

				isUse := false
				for k := 0; !isUse && k < len(compUse); k++ {
					isUse = name == compUse[k].name
				}
				if isUse {
					continue // this server already selected for tha model run
				}

				if cUse.Cpu < cs.totalRes.Cpu-cs.usedRes.Cpu {
					cUse.name = name
					cUse.Cpu = cs.totalRes.Cpu - cs.usedRes.Cpu
					cUse.Mem = cs.totalRes.Mem - cs.usedRes.Mem
				}
			}
			if cUse.name == "" {

				if len(compUse) <= 0 {
					qj.isError = true
					rsc.queueJobs[stamp] = qj
					e := errors.New("ERROR: resources not found to run the model, CPU: " + strconv.Itoa(qj.Res.Cpu) + ": " + stamp)
					return &qj.RunJob, false, qj.filePath, []computeUse{}, hostIni{}, e
				}
				// else assign the rest of the job to the last server
				compUse[len(compUse)-1].Cpu = compUse[len(compUse)-1].Cpu + nCpu

				omppLog.Log("WARNING: oversubscribe resources run the model, CPU: ", qj.Res.Cpu, ": ", stamp)
				break // oversubscribe last server
			}

			if cUse.Cpu > nCpu {
				cUse.Cpu = nCpu
			}
			if cUse.Mem > nMem {
				cUse.Mem = nMem
			}
			if cUse.Mem < 0 {
				cUse.Mem = 0
			}

			compUse = append(compUse, cUse)

			nCpu = nCpu - cUse.Cpu
			if nMem > 0 {
				nMem = nMem - (rsc.computeState[cUse.name].totalRes.Mem - rsc.computeState[cUse.name].usedRes.Mem)
			}
		}
	}

	// job found: copy run request from the queue
	rsc.selectedKeys = append(rsc.selectedKeys, stamp)

	job := qj.RunJob

	job.Opts = make(map[string]string, len(qj.Opts))
	for key, val := range qj.Opts {
		job.Opts[key] = val
	}

	job.Env = make(map[string]string, len(qj.Env))
	for key, val := range qj.Env {
		job.Env[key] = val
	}

	job.Tables = make([]string, len(qj.Tables))
	copy(job.Tables, qj.Tables)

	job.RunNotes = make(
		[]struct {
			LangCode string // model language code
			Note     string // run notes
		},
		len(qj.RunNotes))
	copy(job.RunNotes, qj.RunNotes)

	return &job, true, qj.filePath, compUse, rsc.hostFile, nil
}

// return copy of submission stamps and job control items for queue, active and history model run jobs
func (rsc *RunCatalog) getRunJobs() (JobServiceState, []string, []RunJob, []string, []RunJob, []string, []historyJobFile) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// jobs queue: sort in order of submission stamps, which user may change through UI
	qKeys := make([]string, len(rsc.queueKeys))
	qJobs := make([]RunJob, len(rsc.queueKeys))
	for k, stamp := range rsc.queueKeys {
		qKeys[k] = stamp
		qJobs[k] = rsc.queueJobs[stamp].RunJob
	}

	// active jobs: sort by submission time
	aKeys := make([]string, len(rsc.activeJobs))
	n := 0
	for stamp := range rsc.activeJobs {
		aKeys[n] = stamp
		n++
	}
	sort.Strings(aKeys)

	aJobs := make([]RunJob, len(aKeys))
	for k, stamp := range aKeys {
		aJobs[k] = rsc.activeJobs[stamp].RunJob
	}

	// history jobs: sort by submission time
	hKeys := make([]string, len(rsc.historyJobs))
	n = 0
	for stamp := range rsc.historyJobs {
		hKeys[n] = stamp
		n++
	}
	sort.Strings(hKeys)

	hJobs := make([]historyJobFile, len(hKeys))
	for k, stamp := range hKeys {
		hJobs[k] = rsc.historyJobs[stamp]
	}

	return rsc.JobServiceState, qKeys, qJobs, aKeys, aJobs, hKeys, hJobs
}

// return active job control item and is found boolean flag
func (rsc *RunCatalog) getActiveJobItem(submitStamp string) (runJobFile, bool) {

	if submitStamp == "" {
		return runJobFile{}, false // empty job submission stamp: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if aj, ok := rsc.activeJobs[submitStamp]; ok {
		return aj, true
	}
	return runJobFile{}, false // not found
}

// return queue job control item and is found boolean flag
func (rsc *RunCatalog) getQueueJobItem(submitStamp string) (queueJobFile, bool) {

	if submitStamp == "" {
		return queueJobFile{}, false // empty job submission stamp: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if qj, ok := rsc.queueJobs[submitStamp]; ok {
		return qj, true
	}
	return queueJobFile{}, false // not found
}

// return history job control item and is found boolean flag
func (rsc *RunCatalog) getHistoryJobItem(submitStamp string) (historyJobFile, bool) {

	if submitStamp == "" {
		return historyJobFile{}, false // empty job submission stamp: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if hj, ok := rsc.historyJobs[submitStamp]; ok {
		return hj, true
	}
	return historyJobFile{}, false // not found
}

// write new run request into job queue file, return queue job file path
func (rsc *RunCatalog) addJobToQueue(job *RunJob) (string, error) {
	if !theCfg.isJobControl {
		return "", nil // job control disabled
	}

	fp := jobQueuePath(job.SubmitStamp, job.ModelName, job.ModelDigest, job.IsMpi, rsc.nextJobPosition(), job.Res.Cpu, job.Res.Mem)

	err := helper.ToJsonIndentFile(fp, job)
	if err != nil {
		omppLog.Log(err)
		fileDeleteAndLog(true, fp) // on error remove file, if any file created
		return "", err
	}

	return "", nil
}

// return next job position in the queue, it is not a queue index but "ticket number" to establish queue jobs order
func (rsc *RunCatalog) nextJobPosition() int {
	if !theCfg.isJobControl {
		return 0 // job control disabled
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rsc.jobLastPosition++

	if rsc.jobLastPosition <= jobPositionDefault {
		rsc.jobLastPosition = jobPositionDefault + 1
	}
	return rsc.jobLastPosition
}

// move job into the specified queue index position.
// Top of the queue position is zero, negative position treated as zero.
// If position number exceeds queue length then job moved to the bottom of the queue.
// Return false if job not found in the queue
func (rsc *RunCatalog) moveJobInQueue(submitStamp string, index int) (bool, [][2]string) {

	if submitStamp == "" {
		return false, [][2]string{} // empty job submission stamp: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find current job position in the queue, excluding jobs selected to run
	isFound := false
	n := 0
	for n = range rsc.queueKeys {
		isFound = rsc.queueKeys[n] == submitStamp
		if isFound {
			break
		}
	}
	if isFound {
		isSel := false
		for j := 0; !isSel && j < len(rsc.selectedKeys); j++ {
			isSel = rsc.selectedKeys[j] == submitStamp
		}
		isFound = !isSel
	}
	if !isFound {
		return false, [][2]string{} // job not found in the queue
	}

	// position must be between zero at the last position in the queue
	nPos := index
	fPos := jobPositionDefault

	isFirst := nPos <= 0
	if isFirst {
		nPos = 0
		rsc.jobFirstPosition--
		fPos = rsc.jobFirstPosition
	}

	isLast := !isFirst && nPos >= len(rsc.queueKeys)-1
	if isLast {
		nPos = len(rsc.queueKeys) - 1
		fPos = rsc.jobLastPosition + 1
		rsc.jobLastPosition = fPos + 1
	}
	if nPos == n {
		return true, [][2]string{} // job is already at this position
	}

	// list of files to rename
	moveLst := [][2]string{}

	fJ, isFrom := rsc.queueJobs[submitStamp]
	toJ, isTo := rsc.queueJobs[rsc.queueKeys[nPos]]

	if !isFirst && !isLast {

		if isFrom && isTo {
			moveLst = append(moveLst, [2]string{
				fJ.filePath,
				jobQueuePath(fJ.SubmitStamp, fJ.ModelName, fJ.ModelDigest, fJ.IsMpi, toJ.position, fJ.Res.Cpu, fJ.Res.Mem),
			})
		}
	} else { // move source file to the top (before the first position) or to the bottom (after last postion)

		moveLst = append(moveLst, [2]string{
			fJ.filePath,
			jobQueuePath(fJ.SubmitStamp, fJ.ModelName, fJ.ModelDigest, fJ.IsMpi, fPos, fJ.Res.Cpu, fJ.Res.Mem),
		})
	}

	// move down to the queue: shift items up
	if nPos > n {

		for k := n; k < nPos && k < len(rsc.queueKeys)-1; k++ {

			// skip files selected to run
			fromKey := rsc.queueKeys[k+1]

			isSel := false
			for j := 0; !isSel && j < len(rsc.selectedKeys); j++ {
				isSel = rsc.selectedKeys[j] == fromKey
			}
			if isSel {
				continue
			}

			if !isFirst && !isLast {

				fJ, isFrom = rsc.queueJobs[fromKey]
				toJ, isTo = rsc.queueJobs[rsc.queueKeys[k]]
				if isFrom && isTo {
					moveLst = append(moveLst, [2]string{
						fJ.filePath,
						jobQueuePath(fJ.SubmitStamp, fJ.ModelName, fJ.ModelDigest, fJ.IsMpi, toJ.position, fJ.Res.Cpu, fJ.Res.Mem),
					})
				}
			}

			rsc.queueKeys[k] = fromKey
		}

	} else { // move up in the queue: shift items down

		for k := n - 1; k >= nPos && k >= 0; k-- {

			// skip files selected to run
			fromKey := rsc.queueKeys[k]

			isSel := false
			for j := 0; !isSel && j < len(rsc.selectedKeys); j++ {
				isSel = rsc.selectedKeys[j] == fromKey
			}
			if isSel {
				continue
			}

			if !isFirst && !isLast {

				fJ, isFrom = rsc.queueJobs[fromKey]
				toJ, isTo = rsc.queueJobs[rsc.queueKeys[k+1]]
				if isFrom && isTo {
					moveLst = append(moveLst, [2]string{
						fJ.filePath,
						jobQueuePath(fJ.SubmitStamp, fJ.ModelName, fJ.ModelDigest, fJ.IsMpi, toJ.position, fJ.Res.Cpu, fJ.Res.Mem),
					})
				}
			}

			rsc.queueKeys[k+1] = fromKey
		}
	}
	rsc.queueKeys[nPos] = submitStamp

	return true, moveLst
}

// select additional computational servers or clusters to start,
// return server names, startup timeout and for each server startup exe names and startup arguments
func (rsc *RunCatalog) selectToStartCompute() ([]string, int, []string, [][]string) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// do not start anything if:
	// this instance is not a leader oms instance or queue is paused or MPI queue is empty
	if !rsc.isLeader || rsc.IsQueuePaused || rsc.topQueueRes.Cpu <= 0 && rsc.topQueueRes.Mem <= 0 {
		return []string{}, 1, []string{}, [][]string{}
	}
	res := rsc.topQueueRes

	// remove from startup list servers which are no longer exist
	// substract from required resources servers which are starting now
	n := 0
	for _, name := range rsc.startupNames {
		if cs, ok := rsc.computeState[name]; ok {
			rsc.startupNames[n] = name // server stil exist
			n++

			res.Cpu -= cs.totalRes.Cpu // server is starting, reduce cpu and memory required to run the queue
			res.Mem -= cs.totalRes.Mem
		}
	}
	rsc.startupNames = rsc.startupNames[:n]

	if res.Cpu <= 0 && res.Mem <= 0 {
		return []string{}, 1, []string{}, [][]string{} // do not start any additional servers
	}

	// select servers to start
	// where state is power off and with min error count and longest unused time
	// until it is enough cpu and memory to run first MPI job in the queue
	srvLst := []string{}
	exeLst := []string{}
	argLst := [][]string{}

	for res.Cpu > 0 || res.Mem > 0 { // until not enough servers to run first MPI job in the queue

		name := ""
		minE := 2 * maxComputeErrors
		minTs := time.Now().UnixMilli() + 100

		for _, cs := range rsc.computeState {
			if cs.state != "" {
				continue // server not in power off state
			}

			isAct := false
			for k := 0; !isAct && k < len(rsc.startupNames); k++ {
				isAct = rsc.startupNames[k] == cs.name
			}
			for k := 0; !isAct && k < len(srvLst); k++ {
				isAct = srvLst[k] == cs.name
			}
			for k := 0; !isAct && k < len(rsc.shutdownNames); k++ {
				isAct = rsc.shutdownNames[k] == cs.name
			}
			if isAct {
				continue // this server already in startup or shutdown list
			}

			if name == "" || cs.errorCount < minE || cs.errorCount == minE && cs.lastUsedTs < minTs {
				name = cs.name
				minE = cs.errorCount
				minTs = cs.lastUsedTs
			}
		}
		if name == "" {
			break // no servers available
		}

		// add server to startup list
		srvLst = append(srvLst, name)
		exeLst = append(exeLst, rsc.computeState[name].startExe)

		args := make([]string, len(rsc.computeState[name].startArgs))
		copy(args, rsc.computeState[name].startArgs)
		argLst = append(argLst, args)

		res.Cpu -= rsc.computeState[name].totalRes.Cpu
		res.Mem -= rsc.computeState[name].totalRes.Mem
	}

	// check results: if not enough servers to run first MPI job in the queue then return empty result
	if res.Cpu > 0 || res.Mem > 0 {
		return []string{}, 1, []string{}, [][]string{}
	}

	// append to servers starup list and return list of servers to start
	rsc.startupNames = append(rsc.startupNames, srvLst...)

	return srvLst, rsc.maxStartTime, exeLst, argLst
}

// select computational servers or clusters to shutdown,
// return server names, shutdown timeout and for each server shutdown exe names and shutdown arguments
func (rsc *RunCatalog) selectToStopCompute() ([]string, int, []string, [][]string) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// do not stop anything: if idle time is unlimited or this instance is not a leader oms instance
	if rsc.maxIdleTime <= 0 || !rsc.isLeader {
		return []string{}, 1, []string{}, [][]string{}
	}
	nowTs := time.Now().UnixMilli() // current time in unix milliseconds

	// remove from shutdown list servers which are no longer exist
	n := 0
	for _, name := range rsc.shutdownNames {
		if _, ok := rsc.computeState[name]; ok {
			rsc.shutdownNames[n] = name // server stil exist
			n++
		}
	}
	rsc.shutdownNames = rsc.shutdownNames[:n]

	// find servers where there no model runs for more than idle time interval
	srvLst := []string{}
	exeLst := []string{}
	argLst := [][]string{}

	for _, cs := range rsc.computeState {

		isAct := false
		for k := 0; !isAct && k < len(rsc.shutdownNames); k++ {
			isAct = rsc.shutdownNames[k] == cs.name
		}
		for k := 0; !isAct && k < len(rsc.startupNames); k++ {
			isAct = rsc.startupNames[k] == cs.name
		}
		if isAct {
			continue // this server already in shutdown or startup list
		}

		// if no model runs for more than idle time in milliseconds and server started more than idle time in milliseconds
		if cs.state == "ready" && cs.lastUsedTs+int64(1000*rsc.maxIdleTime) < nowTs && cs.lastStartTs+int64(1000*rsc.maxIdleTime) < nowTs {

			srvLst = append(srvLst, cs.name)
			exeLst = append(exeLst, cs.stopExe)

			args := make([]string, len(cs.stopArgs))
			copy(args, cs.stopArgs)
			argLst = append(argLst, args)

			rsc.shutdownNames = append(rsc.shutdownNames, cs.name)
		}
	}

	return srvLst, rsc.maxStopTime, exeLst, argLst
}

// on sucess reset server error count or increase it on error and remove server name from startup list
func (rsc *RunCatalog) startupCompleted(isOkStart bool, name string) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// update server state
	if cs, ok := rsc.computeState[name]; ok {
		if isOkStart {
			cs.errorCount = 0
			cs.lastStartTs = time.Now().UnixMilli()
		} else {
			cs.errorCount++
			cs.lastErrorTs = time.Now().UnixMilli()
		}
		rsc.computeState[name] = cs
	}

	// remove from startup list
	n := 0
	for k := range rsc.startupNames {
		if rsc.startupNames[k] != name {
			rsc.startupNames[n] = rsc.startupNames[k]
			n++
		}
	}
	rsc.startupNames = rsc.startupNames[:n]
}

// on sucess reset server error count or increase it on error remove server name from shutdown list
func (rsc *RunCatalog) shutdownCompleted(isOkStop bool, name string) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// update server state
	if cs, ok := rsc.computeState[name]; ok {
		if isOkStop {
			cs.errorCount = 0
			cs.lastStopTs = time.Now().UnixMilli()
		} else {
			cs.errorCount++
			cs.lastErrorTs = time.Now().UnixMilli()
		}
		rsc.computeState[name] = cs
	}

	// remove from shutdown list
	n := 0
	for k := range rsc.shutdownNames {
		if rsc.shutdownNames[k] != name {
			rsc.shutdownNames[n] = rsc.shutdownNames[k]
			n++
		}
	}
	rsc.shutdownNames = rsc.shutdownNames[:n]
}

// update run catalog with current job control files
func (rsc *RunCatalog) updateRunJobs(
	jsState JobServiceState,
	computeState map[string]computeItem,
	queueJobs map[string]queueJobFile,
	activeJobs map[string]runJobFile,
	historyJobs map[string]historyJobFile,
) *jobControlState {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	jNextPos := rsc.jobLastPosition

	rsc.JobServiceState = jsState

	if rsc.jobLastPosition < jNextPos {
		rsc.jobLastPosition = jNextPos
	}

	// copy state of computational resources
	for name := range rsc.computeState {
		_, ok := computeState[name]
		if !ok {
			delete(rsc.computeState, name) // remove: server or cluster not does exist anymore
		}
	}
	for name, cs := range computeState {
		rsc.computeState[name] = cs
	}

	// update queue jobs and collect all new submission stamps
	for stamp := range rsc.queueJobs {
		jf, ok := queueJobs[stamp]
		if !ok || jf.isError {
			delete(rsc.queueJobs, stamp) // remove: job file not exists
		}
	}

	for stamp, jf := range queueJobs {
		if _, ok := rsc.models[jf.ModelDigest]; !ok {
			continue // skip: model digest is not the models list
		}
		if jf.isError {
			continue // skip: model job error
		}
		rsc.queueJobs[stamp] = jf
	}

	// remove queue submission stamps which are no longer exists in the queue
	n := 0
	for _, stamp := range rsc.queueKeys {
		if _, ok := queueJobs[stamp]; ok {
			rsc.queueKeys[n] = stamp
			n++
		}
	}
	rsc.queueKeys = rsc.queueKeys[:n]

	// find new submission stamps from the queue
	n = len(queueJobs) - n
	if n > 0 {

		qKeys := make([]string, n)
		k := 0
		for stamp := range queueJobs {

			isFound := false
			for j := 0; !isFound && j < len(rsc.queueKeys); j++ {
				isFound = rsc.queueKeys[j] == stamp
			}
			if !isFound {
				qKeys[k] = stamp
				k++
			}
		}

		// sort new jobs by time stamps: first come first served and append at the end of existing queue
		sort.Strings(qKeys)
		rsc.queueKeys = append(rsc.queueKeys, qKeys...)
	}

	// update active model run jobs
	for stamp := range rsc.activeJobs {
		jf, ok := activeJobs[stamp]
		if !ok || jf.isError {
			delete(rsc.activeJobs, stamp) // remove: job file not exists
		}
	}

	for stamp, jf := range activeJobs {
		if _, ok := rsc.models[jf.ModelDigest]; !ok {
			continue // skip: model digest is not the models list
		}
		if jf.isError {
			continue // skip: model job error or
		}
		rsc.activeJobs[stamp] = jf
	}

	// update model run job history
	for stamp := range rsc.historyJobs {
		jh, ok := historyJobs[stamp]
		if !ok || jh.isError {
			delete(rsc.historyJobs, stamp) // remove: job file not exist
		}
	}

	for stamp, jh := range historyJobs {
		if !jh.isError {
			rsc.historyJobs[stamp] = jh
		}
	}

	// cleanup selected to run jobs list: remove if submission stamp not exist in queue files list
	n = 0
	for _, stamp := range rsc.selectedKeys {
		if _, ok := queueJobs[stamp]; ok {
			rsc.selectedKeys[n] = stamp // job file still exist in the queue
			n++
		}
	}
	rsc.selectedKeys = rsc.selectedKeys[:n]

	// return job control state
	jsc := jobControlState{
		Queue: make([]string, len(rsc.queueKeys)),
	}
	copy(jsc.Queue, rsc.queueKeys)

	return &jsc
}
