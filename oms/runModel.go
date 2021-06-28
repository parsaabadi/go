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

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// runModel starts new model run and return run stamp.
// if run stamp not specified as input parameter then use unique timestamp.
// Model run console output redirected to log file: models/log/modelName.runStamp.console.log
func (rsc *RunCatalog) runModel(req *RunRequest) (*RunState, error) {

	// make model process run stamp, if not specified then use timestamp by default
	ts, dtNow := theCatalog.getNewTimeStamp()
	rStamp := helper.CleanPath(req.RunStamp)
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

	mb, ok := theCatalog.modelBasicByDigest(req.ModelDigest)
	if !ok {
		err := errors.New("Model run error, model not found: " + req.ModelName + ": " + req.ModelDigest)
		rsc.updateRunState(req.ModelDigest, rs.RunStamp, true, err.Error())
		rs.IsFinal = true
		return rs, err // exit with error: model failed to start
	}
	binDir := mb.binDir

	wDir := filepath.Join(binRoot, req.Dir)

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

	importDbLc := strings.ToLower("-ImportDb.")

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
		if strings.EqualFold(key, "-OpenM.Database") {
			continue // database connection string not allowed as run option
		}
		if strings.HasPrefix(strings.ToLower(key), importDbLc) {
			continue // import database connection string not allowed as run option
		}

		mArgs = append(mArgs, key, val) // append command line argument key and value
	}

	// assume model exe name is the same as model name
	mExe := helper.CleanPath(req.ModelName)

	cmd, err := rsc.makeCommand(mExe, binDir, wDir, mArgs, req)
	if err != nil {
		omppLog.Log("Error at starting run model ", req.ModelName, ": ", err.Error())
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
	omppLog.Log("Run model: ", mExe, " in directory: ", wDir)
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
// If this is MPI model run then tempalate is requred
// MPI run template can be model specific: "mpi.ModelName.template.txt" or default: "mpi.ModelRun.template.txt".
func (rsc *RunCatalog) makeCommand(mExe, binDir, workDir string, mArgs []string, req *RunRequest) (*exec.Cmd, error) {

	// check is it MPI model run, to run MPI model template is required
	isMpi := req.Mpi.Np != 0
	if isMpi && req.Template == "" {

		// search for model-specific MPI template
		mtn := "mpi." + req.ModelName + ".template.txt"

		for _, tn := range theRunCatalog.mpiTemplates {
			if tn == mtn {
				req.Template = mtn
			}
		}
		// if model-specific MPI template not found then use default MPI template to run the model
		if req.Template == "" {
			req.Template = defaultMpiTemplate
		}
	}
	isTmpl := req.Template != ""

	// if this is regular non-MPI model.exe run and no template:
	//	 ./modelExe -OpenM.LogToFile true ...etc...
	var cmd *exec.Cmd

	if !isTmpl && !isMpi {
		if binDir == "" || binDir == "." || binDir == "./" {
			mExe = "./" + mExe
		} else {
			mExe = filepath.Join(binDir, mExe)
		}
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
			ExeStem   string            // base part of model exe name, usually modelName
			Dir       string            // work directory to run the model
			BinDir    string            // bin directory where model.exe is located
			MpiNp     int               // number of MPI processes
			Args      []string          // model command line arguments
			Env       map[string]string // environment variables to run the model
		}{
			ModelName: req.ModelName,
			ExeStem:   mExe,
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

	// if this is not MPI run then:
	// 	set work directory
	// 	append request environment variables to model environment
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
func (rsc *RunCatalog) createProcRunState(rs *RunState) {
	if rs == nil {
		return
	}
	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rsc.runLst.PushFront(
		&runStateLog{
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
func (rsc *RunCatalog) updateRunState(digest, runStamp string, isFinal bool, msg string) {
	if digest == "" || runStamp == "" {
		return // model run undefined
	}
	dtNow := time.Now()

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
