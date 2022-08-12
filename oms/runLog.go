// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// timeout in msec, sleep interval between scanning log directory
const logScanInterval = 4021

// get current run status and page of log file lines
func (rsc *RunCatalog) readModelRunLog(digest, stamp string, start, count int) (*RunStateLogPage, error) {

	// search for run status and log text by model digest and run stamp or submit stamp
	lrp, isFound, runStamp, isNewLog := rsc.getRunStateLogPage(digest, stamp, start, count)
	if !isFound {
		return lrp, nil // model run not found in catalog
	}
	if !lrp.isHistory {
		return lrp, nil // found model run in the oms run list
	}
	// else found model run in run history

	// check run status: if not completed then read most recent status from database
	isUpdated := false

	if runStamp != "" {
		rl, isOk := theCatalog.RunRowList(digest, runStamp)
		if isOk && len(rl) > 0 {

			// use most recent run with that run stamp
			r := rl[len(rl)-1]

			isUpdated = lrp.RunState.UpdateDateTime != r.UpdateDateTime || lrp.RunState.IsFinal != db.IsRunCompleted(r.Status)
			if isUpdated {
				lrp.RunState.UpdateDateTime = r.UpdateDateTime
				lrp.RunState.IsFinal = db.IsRunCompleted(r.Status)
			}
		}
	}

	// if run updated or if run found in history
	if isNewLog || isUpdated {

		// read log file and select log page to return
		lines := []string{}
		if lrp.RunState.IsLog {

			if lines, isOk := readLogFile(lrp.logPath); isOk && len(lines) > 0 {
				lrp.Offset, lrp.Size, lrp.Lines = getLinesPage(start, count, lines) // make log page to return
				lrp.TotalSize = len(lines)
			}
		}
		// update model run catalog
		rsc.postRunStateLog(digest, lrp.RunState, lines)
	}
	return lrp, nil
}

// update model run catalog with new run state and log file lines.
func (rsc *RunCatalog) postRunStateLog(digest string, runState RunState, logLines []string) {

	if digest == "" || runState.RunStamp == "" {
		return // model run undefined
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	// update model run state and append log message
	var rs *runStateLog
	var ok bool
	for re := rsc.runLst.Front(); re != nil; re = re.Next() {

		rs, ok = re.Value.(*runStateLog)
		if !ok || rs == nil {
			continue
		}
		ok = rs.ModelDigest == digest && rs.RunStamp == runState.RunStamp
		if ok {
			break
		}
	}

	// update existing run status or append new element to run list
	if ok && rs != nil {
		rs.RunState = runState
		if len(logLines) > len(rs.logLineLst) {
			rs.logLineLst = logLines
		}
	} else {
		rsc.runLst.PushFront(
			&runStateLog{
				RunState:   runState,
				logLineLst: logLines,
			})
	}
}

// get current run status and page of log lines and return true if run status found in catalog.
// if run found in log history it is neccessary to check run status and may be read log file content.
// return run state and two flags: first is true if model run found, second is true if model run found in log history.
func (rsc *RunCatalog) getRunStateLogPage(digest, stamp string, start, count int) (*RunStateLogPage, bool, string, bool) {

	lrp := &RunStateLogPage{
		Lines: []string{},
	}
	if digest == "" || stamp == "" {
		return lrp, false, "", false // empty model digest or stamp (run-or-submit stamp): exit
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run-or-submit stamp
	// return model run state and page from model log
	var rsl *runStateLog
	isFound := false
	runStamp := stamp

	for re := rsc.runLst.Front(); !isFound && re != nil; re = re.Next() {

		rsl, isFound = re.Value.(*runStateLog)
		if !isFound || rsl == nil {
			continue
		}
		isFound = rsl.ModelDigest == digest && rsl.RunStamp == stamp

		if !isFound && rsl.ModelDigest == digest && rsl.SubmitStamp == stamp { // check if it is a submit stamp, not a run stamp
			isFound = true
			runStamp = rsl.RunStamp
			break
		}
	}
	// if not found then try to search log history
	isNewLog := false
	if !isFound {
		if ml, ok := rsc.modelRuns[digest]; ok {
			if r, isLog := ml[runStamp]; isLog {
				rsl = &runStateLog{RunState: r, logLineLst: []string{}}
				isFound = true
			}
		}
		isNewLog = isFound
	}
	if !isFound {
		return lrp, false, "", false // not found: return empty result
	}
	// run state found
	lrp.RunState = rsl.RunState

	// if run log lines are empty then it is necceassry to read log file outside of lock
	if isNewLog || len(rsl.logLineLst) <= 0 {
		return lrp, true, runStamp, isNewLog
	}

	// copy log lines
	lrp.TotalSize = len(rsl.logLineLst)
	lrp.Offset, lrp.Size, lrp.Lines = getLinesPage(start, count, rsl.logLineLst)

	return lrp, true, runStamp, isNewLog
}

// read all non-empty text lines from log file.
func readLogFile(logPath string) ([]string, bool) {

	if logPath == "" {
		return []string{}, false // empty log file path: exit
	}

	f, err := os.Open(logPath)
	if err != nil {
		return []string{}, false // cannot open log file for reading
	}
	defer f.Close()
	rd := bufio.NewReader(f)

	// read log file, trim lines and skip empty lines
	lines := []string{}

	for {
		ln, e := rd.ReadString('\n')
		if e != nil {
			if e != io.EOF {
				omppLog.Log("Error at reading log file: ", logPath)
			}
			break
		}
		ln = strings.TrimRightFunc(ln, func(r rune) bool { return unicode.IsSpace(r) || !unicode.IsPrint(r) })
		if ln != "" {
			lines = append(lines, ln)
		}
	}

	return lines, true
}

// getLinesPage return count pages from logLines starting at offset.
// if count <= 0 then return all lines starting at offset
func getLinesPage(offset, count int, logLines []string) (int, int, []string) {

	nTotal := len(logLines)
	if nTotal <= 0 {
		return 0, 0, []string{}
	}

	if offset < 0 {
		offset = 0
	}
	if offset >= nTotal {
		return offset, 0, []string{} // log offset (first line to read) past last log line
	}
	if count <= 0 || offset+count > nTotal {
		count = nTotal - offset
	}

	// copy log lines into result
	lines := make([]string, count)
	copy(lines, logLines[offset:offset+count])

	return offset, count, lines
}

// scan model log directories to collect list log files for each model run history
func scanModelLogDirs(doneC <-chan bool) {

	// path to model run log file found by log directory scan
	type logItem struct {
		runId       int    // model run id
		runStamp    string // model run stamp
		runName     string // model run name
		isCompleted bool   // if true then run completed
		updateDt    string // last update date-time
		submitStamp string // approximate submission stamp
	}

	for {
		// get current models from run catalog and main catalog
		rbs := theRunCatalog.allModels()
		dLst := theCatalog.allModelDigests()
		sort.Strings(dLst)

		// find new model runs since last scan
		for dgst, it := range rbs {

			// skip model if digest does not exist in main catalog, ie: catalog closed
			if i := sort.SearchStrings(dLst, dgst); i < 0 || i >= len(dLst) || dLst[i] != dgst {
				continue
			}

			// model log directory path must be not empty to search for log files
			if !it.isLogDir {
				continue
			}
			if it.logDir == "" {
				it.logDir = "." // assume current directory if log directory not specified but eanbled by isLogDir
			}
			it.logDir = filepath.ToSlash(it.logDir) // use / path separator

			// get list of model runs
			rl, ok := theCatalog.RunRowListByModelDigest(dgst)
			if !ok || len(rl) <= 0 {
				continue // no model runs (or ignore get runs error)
			}

			// append new runs to model run list
			logLst := make([]logItem, len(rl))

			for k := range rl {
				logLst[k] = logItem{
					runId:       rl[k].RunId,
					runStamp:    rl[k].RunStamp,
					runName:     rl[k].Name,
					isCompleted: db.IsRunCompleted(rl[k].Status),
					updateDt:    rl[k].UpdateDateTime,
				}
				if helper.IsUnderscoreTimeStamp(rl[k].RunStamp) {
					logLst[k].submitStamp = rl[k].RunStamp
				} else {
					logLst[k].submitStamp = helper.ToUnderscoreTimeStamp(rl[k].CreateDateTime)
				}
			}

			// get list of model run log files
			ptrn := it.logDir + "/" + it.name + "." + "*.log"
			fLst := filesByPattern(ptrn, "Error at log files search")

			// replace path separators by / and sort file paths list
			for k := range fLst {
				fLst[k] = filepath.ToSlash(fLst[k])
			}
			sort.Strings(fLst) // sort by file path

			// search new model runs to find log file path
			// append run state to run state list ordered by run id
			rsLst := []RunState{}
			for k := 0; k < len(logLst); k++ {

				// search for modelName.runStamp.console.log
				fn := it.logDir + "/" + it.name + "." + logLst[k].runStamp + ".console.log"
				n := sort.SearchStrings(fLst, fn)
				isFound := n < len(fLst) && fLst[n] == fn

				// search for modelName.runStamp.log
				if !isFound {
					fn = it.logDir + "/" + it.name + "." + logLst[k].runStamp + ".log"
					n = sort.SearchStrings(fLst, fn)
					isFound = n < len(fLst) && fLst[n] == fn
				}

				if isFound {
					rsLst = append(rsLst,
						RunState{
							ModelName:      it.name,
							ModelDigest:    dgst,
							RunStamp:       logLst[k].runStamp,
							SubmitStamp:    logLst[k].submitStamp,
							IsFinal:        logLst[k].isCompleted,
							UpdateDateTime: logLst[k].updateDt,
							RunName:        logLst[k].runName,
							IsLog:          true,
							LogFileName:    filepath.Base(fn),
							logPath:        fn,
							isHistory:      true,
						})
				}
			}

			// add new log file names to model log list and update most recent run id for that model
			theRunCatalog.addModelRuns(dgst, rsLst)
		}

		// clear old entries entries in run state list
		theRunCatalog.clearRunStateList()

		// wait for doneC or sleep
		if doExitSleep(logScanInterval, doneC) {
			return
		}
	}
}

// add new log file names to model log list
// if run stamp already exist in model run history then replace run state with more recent data
func (rsc *RunCatalog) addModelRuns(digest string, runStateLst []RunState) {
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// add new runs to model run history
	for _, r := range runStateLst {

		if ml, ok := rsc.modelRuns[digest]; ok { // skip model if not exist
			ml[r.RunStamp] = r
		}
	}
}

// remove least recent used entries from run state list
func (rsc *RunCatalog) clearRunStateList() {
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// remove old run state if status is final (if run completed)
	n := 0
	for re := rsc.runLst.Front(); re != nil && rsc.runLst.Len() > theCfg.runHistoryMaxSize; re = re.Next() {

		rs, ok := re.Value.(*runStateLog) // model run state expected

		if !ok || rs == nil || !rs.IsFinal { // skip if run is not completed
			continue
		}

		n++
		if n > theCfg.runHistoryMaxSize {
			rsc.runLst.Remove(re)
		}
	}
}

// wait for doneC or sleep, return true on doneC read or false at the end of sleep
func doExitSleep(ms time.Duration, doneC <-chan bool) bool {
	select {
	case <-doneC:
		return true
	case <-time.After(ms * time.Millisecond):
	}
	return false
}
