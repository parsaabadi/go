// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"sort"
	"time"

	"github.com/openmpp/go/ompp/helper"
)

// get model run request from the queue
func (rsc *RunCatalog) getJobFromQueue() (string, *RunRequest, bool) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find first job request in the queue
	if rsc.isPaused || len(rsc.queueKeys) <= 0 {
		return "", nil, false // queue is paused or empty
	}

	stamp := ""
	for k := range rsc.queueKeys {
		jc, ok := rsc.queueJobs[rsc.queueKeys[k]]
		if !ok || jc.isError {
			continue
		}
		isSel := false
		for j := 0; !isSel && j < len(rsc.selectedKeys); j++ {
			isSel = rsc.selectedKeys[j] == rsc.queueKeys[k]
		}
		if !isSel {
			stamp = rsc.queueKeys[k] // first job in queue which not yet selected to run
			break
		}
	}
	if stamp == "" {
		return "", nil, false // queue is empty or all jobs already selected to run
	}

	// job found: copy run request from the queue
	rsc.selectedKeys = append(rsc.selectedKeys, stamp)
	qj := rsc.queueJobs[stamp]
	req := qj.RunRequest

	req.Opts = make(map[string]string, len(qj.Opts))
	for key, val := range qj.Opts {
		req.Opts[key] = val
	}

	req.Env = make(map[string]string, len(qj.Env))
	for key, val := range qj.Env {
		req.Env[key] = val
	}

	req.Tables = make([]string, len(qj.Tables))
	copy(req.Tables, qj.Tables)

	req.RunNotes = make(
		[]struct {
			LangCode string // model language code
			Note     string // run notes
		},
		len(qj.RunNotes))
	copy(req.RunNotes, qj.RunNotes)

	return qj.SubmitStamp, &req, true
}

// return copy of submission stamps and job control items for queue, active and history model run jobs
func (rsc *RunCatalog) getRunJobs() (string, bool, []string, []RunJob, RunRes, RunRes, []string, []RunJob, RunRes, RunRes, []string, []historyJobFile) {

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

	return rsc.jobsUpdateDt, rsc.isPaused, qKeys, qJobs, rsc.queueTotalRes, rsc.queueOwnRes, aKeys, aJobs, rsc.activeTotalRes, rsc.activeOwnRes, hKeys, hJobs
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
func (rsc *RunCatalog) getQueueJobItem(submitStamp string) (runJobFile, bool) {

	if submitStamp == "" {
		return runJobFile{}, false // empty job submission stamp: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if qj, ok := rsc.queueJobs[submitStamp]; ok {
		return qj, true
	}
	return runJobFile{}, false // not found
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

// move job into the specified queue position.
// Top of the queue position is zero, negative position treated as zero.
// If position number exceeds queue length then job moved to the bottom of the queue.
// Return false if job not found in the queue
func (rsc *RunCatalog) moveJobInQueue(submitStamp string, position int) bool {

	if submitStamp == "" {
		return false // empty job submission stamp: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find current job position in the queue
	isFound := false
	n := 0
	for n = range rsc.queueKeys {
		isFound = rsc.queueKeys[n] == submitStamp
		if isFound {
			break
		}
	}
	if !isFound {
		return false // job not found in the queue
	}

	// position must be between zero at the last postion in the queue
	nPos := position
	if nPos <= 0 {
		nPos = 0
	}
	if nPos >= len(rsc.queueKeys)-1 {
		nPos = len(rsc.queueKeys) - 1
	}
	if nPos == n {
		return true // job is already at this position
	}

	// move down to the queue: shift items up
	if nPos > n {

		for k := n; k < nPos && k < len(rsc.queueKeys)-1; k++ {
			rsc.queueKeys[k] = rsc.queueKeys[k+1]
		}
	} else { // move up in the queue: shift items down

		for k := n - 1; k >= nPos && k >= 0; k-- {
			rsc.queueKeys[k+1] = rsc.queueKeys[k]
		}
	}
	rsc.queueKeys[nPos] = submitStamp

	return true
}

// update run catalog with current job control files
func (rsc *RunCatalog) updateRunJobs(
	queueJobs map[string]runJobFile, isPaused bool, reqTotalRes, reqOwnRes RunRes, activeJobs map[string]runJobFile, runTotalRes, runOwnRes RunRes, historyJobs map[string]historyJobFile,
) *jobControlState {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rsc.isPaused = isPaused
	rsc.activeTotalRes = runTotalRes
	rsc.activeOwnRes = runOwnRes
	rsc.queueTotalRes = reqTotalRes
	rsc.queueOwnRes = reqOwnRes
	rsc.jobsUpdateDt = helper.MakeDateTime(time.Now())

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

		// // sort new jobs by time stamps: first come forst served and append at the end of existing queue
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
