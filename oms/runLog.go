// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"bufio"
	"io"
	"os"
	"strings"
	"unicode"

	"github.com/openmpp/go/ompp/db"

	"github.com/openmpp/go/ompp/omppLog"
)

// get current run status and page of log file lines
func (rsc *RunCatalog) readModelRunLog(digest, runStamp string, start, count int) (*RunStateLogPage, error) {

	// search for run status and log text by model digest and run stamp
	lrp, isFound, isNewLog := rsc.getRunStateLogPage(digest, runStamp, start, count)
	if !isFound {
		return lrp, nil // model run not found in catalog
	}
	if !lrp.isHistory {
		return lrp, nil // found model run in the oms run list
	}
	// else found model run in run history

	// check run status: if not completed then read most recent status from database
	isUpdated := false

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

	// if run updated or if run found in history
	if isNewLog || isUpdated {

		// read log file and select log page to return
		lines := []string{}
		if lrp.RunState.IsLog {

			if lines, isOk = rsc.readLogFile(lrp.logPath); isOk && len(lines) > 0 {
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
	for re := rsc.runLst.Back(); re != nil; re = re.Prev() {

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
		rsc.runLst.PushBack(
			&runStateLog{
				RunState:   runState,
				logLineLst: logLines,
			})
	}
}

// get current run status and page of log lines and return true if run status found in catalog.
// if run found in log history it is neccessary to check run status and may be read log file content.
// return run state and two flags: first is true if model run found, second is true if model run found in log history.
func (rsc *RunCatalog) getRunStateLogPage(digest, runStamp string, start, count int) (*RunStateLogPage, bool, bool) {

	lrp := &RunStateLogPage{
		Lines: []string{},
	}
	if digest == "" || runStamp == "" {
		return lrp, false, false // empty model digest: exit
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	// return model run state and page from model log
	var rsl *runStateLog
	isFound := false
	for re := rsc.runLst.Front(); !isFound && re != nil; re = re.Next() {

		rsl, isFound = re.Value.(*runStateLog)
		if !isFound || rsl == nil {
			continue
		}
		isFound = rsl.ModelDigest == digest && rsl.RunStamp == runStamp
	}
	// if not found then try to search log history
	isNewLog := false
	if !isFound {
		if ml, ok := rsc.modelLogs[digest]; ok {
			if r, isLog := ml[runStamp]; isLog {
				rsl = &runStateLog{RunState: r, logLineLst: []string{}}
				isFound = true
			}
		}
		isNewLog = isFound
	}
	if !isFound {
		return lrp, false, false // not found: return empty result
	}
	// run state found
	lrp.RunState = rsl.RunState

	// if run log lines are empty then it is necceassry to read log file outside of lock
	if isNewLog || len(rsl.logLineLst) <= 0 {
		return lrp, true, isNewLog
	}

	// copy log lines
	lrp.TotalSize = len(rsl.logLineLst)
	lrp.Offset, lrp.Size, lrp.Lines = getLinesPage(start, count, rsl.logLineLst)

	return lrp, true, isNewLog
}

// read all non-empty text lines from log file.
func (rsc *RunCatalog) readLogFile(logPath string) ([]string, bool) {

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
