// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"bufio"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"
	"time"
	"unicode"

	"github.com/openmpp/go/ompp/db"

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// runModel starts new model run and return run stamp.
// if run stamp not specified as input parameter then use unique timestamp.
// Model run console output redirected to log file: models/log/modelName.runStamp.console.log
func (rsc *RunStateCatalog) runModel(req *RunRequest) (*RunState, error) {

	// make model process run stamp, if not specified then use timestamp by default
	ts, dtNow := rsc.getNewTimeStamp()
	rStamp := helper.CleanSpecialChars(req.RunStamp)
	if rStamp == "" {
		rStamp = ts
	}

	// new run state
	rs := &RunState{
		ModelName:      req.ModelName,
		ModelDigest:    req.ModelDigest,
		RunStamp:       rStamp,
		UpdateDateTime: helper.MakeDateTime(dtNow),
	}

	// set directories: work directory and bin model.exe directory
	// if bin directory is relative then it must be relative to oms root directory
	// re-base it to model work directory
	binRoot, _ := theCatalog.getModelDir()

	wDir := req.Dir
	if wDir == "" || wDir == "." || wDir == "./" {
		wDir = binRoot
	}

	mb, ok := theCatalog.modelBasicByDigest(req.ModelDigest)
	if !ok {
		err := errors.New("Model run error, model not found: " + req.ModelName + ": " + req.ModelDigest)
		rsc.updateRunState(req.ModelDigest, rs.RunStamp, true, err.Error())
		rs.IsFinal = true
		return rs, err // exit with error: model failed to start

	}

	binDir := mb.binDir
	if mb.binDir == "" || binDir == "." || binDir == "./" {
		binDir = binRoot
	}
	binDir, err := filepath.Rel(wDir, binDir)
	if err != nil {
		binDir = binRoot
	}

	// make file path for model console log output
	if mb.isLogDir {
		if mb.logDir == "" {
			mb.logDir, mb.isLogDir = theCatalog.getModelLogDir()
		}
	}
	if mb.isLogDir {
		if mb.logDir == "" {
			mb.logDir = binDir
		}
		rs.IsLog = mb.isLogDir
		rs.LogFileName = rs.ModelName + "." + rStamp + ".console.log"
		rs.logPath = filepath.Join(mb.logDir, rs.LogFileName)
	}

	// make model run command line arguments, starting from process run stamp and log options
	mArgs := []string{}
	mArgs = append(mArgs, "-OpenM.RunStamp", rStamp)
	mArgs = append(mArgs, "-OpenM.LogToConsole", "true")

	// append model run options from run request
	for krq, val := range req.Opts {

		if len(krq) < 1 { // skip empty run options
			continue
		}

		// command line argument key starts with "-" ex: "-OpenM.Threads"
		key := krq
		if krq[0] != '-' {
			key = "-" + krq
		}

		// save run name and task run name to return as part of run state
		if strings.EqualFold(key, "-OpenM.RunName") {
			rs.RunName = val
		}
		if strings.EqualFold(key, "-OpenM.TaskRunName") {
			rs.TaskRunName = val
		}
		if strings.EqualFold(key, "-OpenM.LogToConsole") {
			continue // skip log to console input run option
		}

		mArgs = append(mArgs, key, val) // append command line argument key and value
	}

	// make model exe or use mpi run exe, assume model exe name same as model name
	mExe := helper.CleanSpecialChars(req.ModelName)
	if binDir == "" || binDir == "." || binDir == "./" {
		mExe = "./" + mExe
	} else {
		mExe = filepath.Join(binDir, mExe)
	}

	cmd, err := rsc.makeCommand(binDir, wDir, mArgs, req)
	if err != nil {
		omppLog.Log("Error at applying run template to ", req.ModelName, ": ", err.Error())
		return rs, err
	}

	// connect console output to log line array
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		return rs, err
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		return rs, err
	}
	outDoneC := make(chan bool, 1)
	errDoneC := make(chan bool, 1)
	logTck := time.NewTicker(logTickTimeout * time.Millisecond)

	// append console output to log lines array
	doLog := func(digest, runStamp string, r io.Reader, done chan<- bool) {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			rsc.updateRunState(digest, runStamp, false, sc.Text())
		}
		done <- true
		close(done)
	}

	// run state initialized: save in run state list
	rsc.createProcRunState(rs)

	// start console output listners
	go doLog(req.ModelDigest, rs.RunStamp, outPipe, outDoneC)
	go doLog(req.ModelDigest, rs.RunStamp, errPipe, errDoneC)

	// start the model
	omppLog.Log("Run model: ", mExe, ", directory: ", wDir)
	omppLog.Log(strings.Join(cmd.Args, " "))

	err = cmd.Start()
	if err != nil {
		omppLog.Log("Model run error: ", err)
		rsc.updateRunState(req.ModelDigest, rs.RunStamp, true, err.Error())
		rs.IsFinal = true
		return rs, err // exit with error: model failed to start
	}
	// else model started: wait until run completed
	go func(digest, runStamp string, cmd *exec.Cmd) {

		// wait until stdout and stderr closed
		for outDoneC != nil || errDoneC != nil {
			select {
			case _, ok := <-outDoneC:
				if !ok {
					outDoneC = nil
				}
			case _, ok := <-errDoneC:
				if !ok {
					errDoneC = nil
				}
			case <-logTck.C:
			}
		}

		// wait for model run to be completed
		e := cmd.Wait()
		if e != nil {
			omppLog.Log(e)
			rsc.updateRunState(digest, runStamp, true, e.Error())
			return
		}
		// else: completed OK
		rsc.updateRunState(digest, runStamp, true, "")
	}(req.ModelDigest, rs.RunStamp, cmd)

	return rs, nil
}

// makeCommand return command to run the model.
// If template file name specified then template processing results used to create command line.
// If this is MPI model run then tempalate is requred and by default "mpi.ModelRun.template.txt" being used.
func (rsc *RunStateCatalog) makeCommand(binDir, workDir string, mArgs []string, req *RunRequest) (*exec.Cmd, error) {

	// check is it MPI or regular process model run, to run MPI model template is required
	isMpi := req.Mpi.Np != 0
	if isMpi && req.Template == "" {
		req.Template = defaultMpiTemplate // use default template to run MPI model
	}
	isTmpl := req.Template != ""

	// make path to model exe assuming exe name same as model name
	mExe := helper.CleanSpecialChars(req.ModelName)
	if binDir == "" || binDir == "." || binDir == "./" {
		mExe = "./" + mExe
	} else {
		mExe = filepath.Join(binDir, mExe)
	}

	// if this is regular non-MPI model.exe run and no template:
	//	 ./modelExe -OpenM.LogToFile true ...etc...
	var cmd *exec.Cmd

	if !isTmpl && !isMpi {
		cmd = exec.Command(mExe, mArgs...)
	}

	// if template specified then process template to get exe name and arguments
	if isTmpl {

		// parse template
		tmpl, err := template.ParseFiles(filepath.Join(rsc.etcDir, req.Template))
		if err != nil {
			return nil, err
		}

		// set template parameters
		d := struct {
			ModelName string            // model name
			ExePath   string            // path to model exe as: binDir/modelname
			Dir       string            // work directory to run the model
			BinDir    string            // bin directory where model.exe is located
			MpiNp     int               // number of MPI processes
			Args      []string          // model command line arguments
			Env       map[string]string // environment variables to run the model
		}{
			ModelName: req.ModelName,
			ExePath:   mExe,
			Dir:       workDir,
			BinDir:    binDir,
			MpiNp:     req.Mpi.Np,
			Args:      mArgs,
			Env:       req.Env,
		}

		// execute template and convert results in array of text lines
		var b strings.Builder

		err = tmpl.Execute(&b, d)
		if err != nil {
			return nil, err
		}
		tLines := strings.Split(strings.Replace(b.String(), "\r", "\n", -1), "\n")

		// from template processing results get:
		//   exe name as first non-empty line
		//   use all other non-empty lines as command line arguments
		cExe := ""
		cArgs := []string{}

		for k := range tLines {

			cl := strings.TrimSpace(tLines[k])
			if cl == "" {
				continue
			}
			if cExe == "" {
				cExe = cl
			} else {
				cArgs = append(cArgs, cl)
			}
		}
		if cExe == "" {
			return nil, errors.New("Error: empty template processing results, cannot run the model: " + req.ModelName)
		}

		// make command
		cmd = exec.Command(cExe, cArgs...)
	}

	// if this is not MPI then set work directory and append to environment variables
	if !isMpi {

		cmd.Dir = workDir

		if len(req.Env) > 0 {
			env := os.Environ()
			for key, val := range req.Env {
				if key != "" && val != "" {
					env = append(env, key+"="+val)
				}
			}
			cmd.Env = env
		}
	}

	return cmd, nil
}

// add new model run state into run history
func (rsc *RunStateCatalog) createProcRunState(rs *RunState) {
	if rs == nil {
		return
	}
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rsc.runLst.PushFront(
		&procRunState{
			RunState:   *rs,
			logLineLst: make([]string, 0, 128),
		})
	for rsc.runLst.Len() > theCfg.runHistoryMaxSize { // remove old run state from history
		rsc.runLst.Remove(rsc.runLst.Back())
	}

	// create log file or truncate existing
	if rs.IsLog {
		f, err := os.Create(rs.logPath)
		if err != nil {
			rs.IsLog = false
			return
		}
		defer f.Close()
	}
}

// updateRunState does model run state update and append to model log lines array
func (rsc *RunStateCatalog) updateRunState(digest, runStamp string, isFinal bool, msg string) {
	if digest == "" || runStamp == "" {
		return // model run undefined
	}
	dtNow := time.Now()

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	// update model run state and append log message
	var rs *procRunState
	var ok bool
	for re := rsc.runLst.Front(); re != nil; re = re.Next() {

		rs, ok = re.Value.(*procRunState)
		if !ok || rs == nil {
			continue
		}
		ok = rs.ModelDigest == digest && rs.RunStamp == runStamp
		if ok {
			break
		}
	}
	if !ok || rs == nil {
		return // model run state not found
	}
	// run state found

	// update run state and append new log line if not empty
	rs.IsFinal = isFinal
	rs.UpdateDateTime = helper.MakeDateTime(dtNow)
	if msg != "" {
		rs.logLineLst = append(rs.logLineLst, msg)
	}

	// write into model console log file
	if rs.IsLog {

		f, err := os.OpenFile(rs.logPath, os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			rs.IsLog = false
			return
		}
		defer f.Close()

		_, err = f.WriteString(msg)
		if err == nil {
			if runtime.GOOS == "windows" { // adjust newline for windows
				_, err = f.WriteString("\r\n")
			} else {
				_, err = f.WriteString("\n")
			}
		}
		if err != nil {
			rs.IsLog = false
		}
	}
}

// get current run status and page of log file lines
func (rsc *RunStateCatalog) readModelRunLog(digest, runStamp string, start, count int) (*RunStateLogPage, error) {

	// search for run status and log text by model digest and run stamp
	lrp, isFound, isNewLog := rsc.getRunStateLogPage(digest, runStamp, start, count)
	if !isFound {
		return lrp, nil // model run not found in catalog
	}
	if !lrp.isHistory {
		return lrp, nil // found model run
	}
	// else found model run in run history

	// check run status: if not completed then read most recent status from database
	isUpdated := false

	rl, isOk := theCatalog.RunRowList(digest, runStamp)
	if isOk && len(rl) > 0 {
		r := rl[len(rl)-1] // most recent run with that run stamp

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
				lrp.Offset, lrp.Size, lrp.Lines = getLinesPage(start, count, lines) // make log page to retrun
				lrp.TotalSize = len(lines)
			}
		}
		// update model run catalog
		rsc.postRunStateLog(digest, lrp.RunState, lines)
	}
	return lrp, nil
}

// update model run catalog with new run state and log file lines.
func (rsc *RunStateCatalog) postRunStateLog(digest string, runState RunState, logLines []string) {

	if digest == "" || runState.RunStamp == "" {
		return // model run undefined
	}

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	// update model run state and append log message
	var rs *procRunState
	var ok bool
	for re := rsc.runLst.Back(); re != nil; re = re.Prev() {

		rs, ok = re.Value.(*procRunState)
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
			&procRunState{
				RunState:   runState,
				logLineLst: logLines,
			})
	}
}

// get current run status and page of log lines and return true if run status found in catalog.
// if run found in log history it is neccessary to check run status and may be read log file content.
// return run state and two flags: first is true if model run found, second is true if model run found in log history.
func (rsc *RunStateCatalog) getRunStateLogPage(digest, runStamp string, start, count int) (*RunStateLogPage, bool, bool) {

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
	var rs *procRunState
	isFound := false
	for re := rsc.runLst.Front(); !isFound && re != nil; re = re.Next() {

		rs, isFound = re.Value.(*procRunState)
		if !isFound || rs == nil {
			continue
		}
		isFound = rs.ModelDigest == digest && rs.RunStamp == runStamp
	}
	// if not found then try to search log history
	isNewLog := false
	if !isFound {

		if rsLst, ok := rsc.modelLogs[digest]; ok {

			for k := 0; !isFound && k < len(rsLst); k++ {
				isFound = rsLst[k].RunStamp == runStamp
				if isFound {
					rs = &procRunState{RunState: rsLst[k], logLineLst: []string{}}
				}
			}
			isNewLog = isFound
		}
	}
	if !isFound {
		return lrp, false, false // not found: return empty result
	}
	// run state found
	lrp.RunState = rs.RunState

	// if run log lines are empty then it is necceassry to read log file outside of lock
	if isNewLog || len(rs.logLineLst) <= 0 {
		return lrp, true, isNewLog
	}

	// copy log lines
	lrp.TotalSize = len(rs.logLineLst)
	lrp.Offset, lrp.Size, lrp.Lines = getLinesPage(start, count, rs.logLineLst)

	return lrp, true, isNewLog
}

// read all non-empty text lines from log file.
func (rsc *RunStateCatalog) readLogFile(logPath string) ([]string, bool) {

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
