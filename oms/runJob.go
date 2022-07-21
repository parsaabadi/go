// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"sort"
	"time"

	"github.com/openmpp/go/ompp/helper"
)

// get model run request and remove it from the queue
func (rsc *RunCatalog) pullJobFromQueue() (string, *RunRequest, bool) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find first job request in the queue
	if len(rsc.queueKeys) <= 0 {
		return "", nil, false // queue is empty
	}
	jKey := ""
	for k := range rsc.queueKeys {
		jc, ok := rsc.queueJobs[rsc.queueKeys[k]]
		if ok && !jc.isError {
			jKey = rsc.queueKeys[k]
			rsc.queueKeys = append(rsc.queueKeys[:k], rsc.queueKeys[k+1:]...)
			break
		}
	}
	if jKey == "" {
		return "", nil, false // queue is empty
	}
	qj := rsc.queueJobs[jKey]
	delete(rsc.queueJobs, jKey)

	// copy run request from the queue
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

// add new model run request to the queue
func (rsc *RunCatalog) appendJobToQueue(stamp string, req *RunRequest) error {

	rjf, err := addJobToQueue(stamp, req)
	if err != nil {
		return err
	}
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// check if job is not already in queue, active or history
	jKey := jobKeyFromStamp(stamp)

	if _, ok := rsc.queueJobs[jKey]; ok {
		return nil // job already in the queue
	}
	if _, ok := rsc.activeJobs[jKey]; ok {
		return nil // job already active: model is running
	}
	if _, ok := rsc.historyJobs[jKey]; ok {
		return nil // job already in the history: run completed or failed
	}

	// append job into the queue
	rsc.queueJobs[jKey] = *rjf

	for _, qKey := range rsc.queueKeys {
		if qKey == jKey {
			return nil // job key already in the queue
		}
	}
	rsc.queueKeys = append(rsc.queueKeys, jKey)

	rsc.jobsUpdateDt = helper.MakeDateTime(time.Now())
	return nil
}

// update run catalog with current job control files
func (rsc *RunCatalog) updateRunJobs(queueJobs map[string]runJobFile, activeJobs map[string]runJobFile, historyJobs map[string]historyJobFile) *jobControlState {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// update queue with current list of job control files
	rsc.jobsUpdateDt = helper.MakeDateTime(time.Now())

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
	n = len(activeJobs)
	if n < cap(rsc.activeKeys) {
		n = cap(rsc.activeKeys)
	}
	rsc.activeKeys = make([]string, 0, n)
	rsc.activeJobs = make(map[string]runJobFile, n)

	for jobKey, jf := range activeJobs {
		if _, ok := rsc.models[jf.ModelDigest]; !ok {
			continue // skip: model digest is not the models list
		}
		if jf.isError || jf.omsName != theCfg.omsName {
			continue // skip: model job error or it is a different oms instance
		}
		rsc.activeJobs[jobKey] = jf
		rsc.activeKeys = append(rsc.activeKeys, jobKey)
	}
	sort.Strings(rsc.activeKeys)

	// update model run job history
	n = len(historyJobs)
	if n < cap(rsc.historyKeys) {
		n = cap(rsc.historyKeys)
	}
	rsc.historyKeys = make([]string, 0, n)
	rsc.historyJobs = make(map[string]historyJobFile, n)

	for jobKey, jh := range historyJobs {
		rsc.historyJobs[jobKey] = jh
		if !jh.isError && jh.omsName == theCfg.omsName {
			rsc.historyKeys = append(rsc.historyKeys, jobKey)
		}
	}
	sort.Strings(rsc.historyKeys)

	// retrun job control state
	jsc := jobControlState{
		Paused: rsc.isPaused,
		Queue:  make([]string, len(rsc.queueKeys)),
	}
	copy(jsc.Queue, rsc.queueKeys)

	return &jsc
}

// return copy of job keys and job control items for queue, active and history model run jobs
func (rsc *RunCatalog) getRunJobs() (string, []string, []RunJob, []string, []RunJob, []string, []historyJobFile) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	qKeys := make([]string, len(rsc.queueKeys))
	qJobs := make([]RunJob, len(rsc.queueKeys))
	for k, jobKey := range rsc.queueKeys {
		qKeys[k] = jobKey
		qJobs[k] = rsc.queueJobs[jobKey].RunJob
	}

	aKeys := make([]string, len(rsc.activeKeys))
	aJobs := make([]RunJob, len(rsc.activeKeys))
	for k, jobKey := range rsc.activeKeys {
		aKeys[k] = jobKey
		aJobs[k] = rsc.activeJobs[jobKey].RunJob
	}

	hKeys := make([]string, len(rsc.historyKeys))
	hJobs := make([]historyJobFile, len(rsc.historyKeys))
	for k, jobKey := range rsc.historyKeys {
		hKeys[k] = jobKey
		hJobs[k] = rsc.historyJobs[jobKey]
	}

	return rsc.jobsUpdateDt, qKeys, qJobs, aKeys, aJobs, hKeys, hJobs
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
