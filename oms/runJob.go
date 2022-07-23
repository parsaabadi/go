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

	jKey := ""
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
			jKey = rsc.queueKeys[k] // first job in queue which not yet selected to run
			break
		}
	}
	if jKey == "" {
		return "", nil, false // queue is empty or all jobs already selected to run
	}

	// job found: copy run request from the queue
	rsc.selectedKeys = append(rsc.selectedKeys, jKey)
	qj := rsc.queueJobs[jKey]
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

// update run catalog with current job control files
func (rsc *RunCatalog) updateRunJobs(
	queueJobs map[string]runJobFile, isPaused bool, activeJobs map[string]runJobFile, historyJobs map[string]historyJobFile,
) *jobControlState {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rsc.isPaused = isPaused
	rsc.jobsUpdateDt = helper.MakeDateTime(time.Now())

	// update queue with current list of job control files
	n := len(queueJobs)
	if n < cap(rsc.queueKeys) {
		n = cap(rsc.queueKeys)
	}
	qKeys := make([]string, 0, n)
	rsc.queueJobs = make(map[string]runJobFile, n)

	// copy existing queue job keys which still in the queue
	if len(queueJobs) > 0 {
		for _, jobKey := range rsc.queueKeys {

			jf, ok := queueJobs[jobKey]
			if !ok {
				continue // skip: job is no longer in the queue
			}
			if jf.isError || jf.omsName != theCfg.omsName {
				continue // skip: model job error or it is a different oms instance
			}
			if _, ok = rsc.models[jf.ModelDigest]; !ok {
				continue // skip: model digest is not the models list
			}

			// check if job already exists in job list
			isFound := false
			for k := 0; !isFound && k < len(qKeys); k++ {
				isFound = qKeys[k] == jobKey
			}
			if !isFound {
				qKeys = append(qKeys, jobKey)
			}
		}
	}
	rsc.queueKeys = qKeys

	// update queue jobs and collect all new job keys
	qKeys = make([]string, 0, n)

	for jobKey, jf := range queueJobs {

		if _, ok := rsc.models[jf.ModelDigest]; !ok {
			continue // skip: model digest is not the models list
		}
		if jf.isError || jf.omsName != theCfg.omsName {
			continue // skip: model job error or it is a different oms instance
		}

		rsc.queueJobs[jobKey] = jf

		// check if job already exists in job list
		isFound := false
		for k := 0; !isFound && k < len(rsc.queueKeys); k++ {
			isFound = rsc.queueKeys[k] == jobKey
		}
		if !isFound {
			qKeys = append(qKeys, jobKey)
		}
	}

	// append new job keys at the end of existing queue
	sort.Strings(qKeys)
	rsc.queueKeys = append(rsc.queueKeys, qKeys...)

	// update active model run jobs
	for jobKey := range rsc.activeJobs {
		jf, ok := activeJobs[jobKey]
		if !ok || jf.isError {
			delete(rsc.activeJobs, jobKey) // remove: job file not exists
		}
	}

	for jobKey, jf := range activeJobs {
		if _, ok := rsc.models[jf.ModelDigest]; !ok {
			continue // skip: model digest is not the models list
		}
		if jf.isError || jf.omsName != theCfg.omsName {
			continue // skip: model job error or it is a different oms instance
		}
		rsc.activeJobs[jobKey] = jf
	}

	// update model run job history
	for jobKey := range rsc.historyJobs {
		jh, ok := historyJobs[jobKey]
		if !ok || jh.isError {
			delete(rsc.historyJobs, jobKey) // remove: job file not exist
		}
	}

	for jobKey, jh := range historyJobs {
		if !jh.isError && jh.omsName == theCfg.omsName {
			rsc.historyJobs[jobKey] = jh
		}
	}

	// cleanup selected to run jobs list: remove if job key not exist in queue files list
	n = 0
	for _, jKey := range rsc.selectedKeys {
		if _, ok := queueJobs[jKey]; ok {
			rsc.selectedKeys[n] = jKey // job file still exist in the queue
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

// return copy of job keys and job control items for queue, active and history model run jobs
func (rsc *RunCatalog) getRunJobs() (string, bool, []string, []RunJob, []string, []RunJob, []string, []historyJobFile) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// jobs queue: sort in order of keys, which user may change through UI
	qKeys := make([]string, len(rsc.queueKeys))
	qJobs := make([]RunJob, len(rsc.queueKeys))
	for k, jKey := range rsc.queueKeys {
		qKeys[k] = jKey
		qJobs[k] = rsc.queueJobs[jKey].RunJob
	}

	// active jobs: sort by submission time
	aKeys := make([]string, len(rsc.activeJobs))
	n := 0
	for jKey := range rsc.activeJobs {
		aKeys[n] = jKey
		n++
	}
	sort.Strings(aKeys)

	aJobs := make([]RunJob, len(aKeys))
	for k, jKey := range aKeys {
		aJobs[k] = rsc.activeJobs[jKey].RunJob
	}

	// history jobs: sort by submission time
	hKeys := make([]string, len(rsc.historyJobs))
	n = 0
	for jKey := range rsc.historyJobs {
		hKeys[n] = jKey
		n++
	}
	sort.Strings(hKeys)

	hJobs := make([]historyJobFile, len(hKeys))
	for k, jKey := range hKeys {
		hJobs[k] = rsc.historyJobs[jKey]
	}

	return rsc.jobsUpdateDt, rsc.isPaused, qKeys, qJobs, aKeys, aJobs, hKeys, hJobs
}

// return active job control item and is found boolean flag
func (rsc *RunCatalog) getActiveJobItem(jobKey string) (runJobFile, bool) {

	if jobKey == "" {
		return runJobFile{}, false // empty job key: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if aj, ok := rsc.activeJobs[jobKey]; ok {
		return aj, true
	}
	return runJobFile{}, false // not found
}

// return queue job control item and is found boolean flag
func (rsc *RunCatalog) getQueueJobItem(jobKey string) (runJobFile, bool) {

	if jobKey == "" {
		return runJobFile{}, false // empty job key: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if qj, ok := rsc.queueJobs[jobKey]; ok {
		return qj, true
	}
	return runJobFile{}, false // not found
}

// return history job control item and is found boolean flag
func (rsc *RunCatalog) getHistoryJobItem(jobKey string) (historyJobFile, bool) {

	if jobKey == "" {
		return historyJobFile{}, false // empty job key: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	if hj, ok := rsc.historyJobs[jobKey]; ok {
		return hj, true
	}
	return historyJobFile{}, false // not found
}

// move job into the specified queue position.
// Top of the queue position is zero, negative position treated as zero.
// If position number exceeds queue length then job moved to the bottom of the queue.
// Return false if job not found in the queue
func (rsc *RunCatalog) moveJobInQueue(jobKey string, position int) bool {

	if jobKey == "" {
		return false // empty job key: return empty result
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find current job position in the queue
	isFound := false
	n := 0
	for n = range rsc.queueKeys {
		isFound = rsc.queueKeys[n] == jobKey
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
	rsc.queueKeys[nPos] = jobKey

	return true
}
