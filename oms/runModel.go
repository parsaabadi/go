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
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// runModel starts new model run and return run stamp.
// if run stamp not specified as input parameter then use unique timestamp.
// Model run console output redirected to log file: models/log/modelName.runStamp.console.log
func (rsc *RunCatalog) runModel(job *RunJob, queueJobPath string, hfCfg hostIni, compUse []computeUse) (*RunState, error) {

	// make model process run stamp, if not specified then use timestamp by default
	ts, tNow := theCatalog.getNewTimeStamp()
	rStamp := helper.CleanPath(job.RunStamp)
	if rStamp == "" {
		rStamp = ts
	}

	// new run state
	rs := &RunState{
		ModelName:      job.ModelName,
		ModelDigest:    job.ModelDigest,
		RunStamp:       rStamp,
		SubmitStamp:    job.SubmitStamp,
		UpdateDateTime: helper.MakeDateTime(tNow),
	}

	// set directories: work directory and bin model.exe directory
	// if bin directory is relative then it must be relative to oms root directory
	// re-base it to model work directory
	binRoot, _ := theCatalog.getModelDir()

	mb, ok := theCatalog.modelBasicByDigest(rs.ModelDigest)
	if !ok {
		err := errors.New("Model not found: " + rs.ModelName + ": " + rs.ModelDigest)
		omppLog.Log("Model run error: ", err)
		moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
		rs.IsFinal = true
		return rs, err // exit with error: model failed to start
	}
	binDir := mb.binDir

	wDir := binDir
	if job.Dir != "" {
		wDir = filepath.Join(binRoot, job.Dir)
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
	mArgs = append(mArgs, "-OpenM.LogToFile", "false")

	importDbLcDot := strings.ToLower("-ImportDb.")
	microdataLcDot := strings.ToLower("-microdata.")

	entAttrs := theCatalog.entityAttrsByDigest(rs.ModelDigest)

	// append model run options from run request
	for krq, val := range job.Opts {

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
		// thread count MUST be specified using request Threads
		if strings.EqualFold(key, "-OpenM.Threads") {
			continue // skip number of threads option: use request Threads value instead
		}
		// MPI "not on root" flag
		if strings.EqualFold(key, "-OpenM.NotOnRoot") {
			continue // skip  MPI "not on root" flag: use request Mpi.IsNotOnRoot boolean instead
		}
		if strings.EqualFold(key, "-OpenM.LogToConsole") {
			continue // skip log to console input run option: it is already on
		}
		if strings.EqualFold(key, "-OpenM.LogToFile") {
			continue // skip log to file input run option: replaced by console output
		}
		if strings.EqualFold(key, "-OpenM.LogFilePath") {
			continue // skip log file path input run option: replaced by console output
		}
		if strings.EqualFold(key, "-OpenM.Database") {
			continue // database connection string not allowed as run option
		}
		if strings.HasPrefix(strings.ToLower(key), importDbLcDot) {
			continue // import database connection string not allowed as run option
		}

		// if this is microdata run option then microdata must be enabled
		// do not allow microdata options which are part of Microdata run request:
		//   -Microdata.ToDb -Microdata.UseInternal
		//   -Microdata.All  -Microdata.anyEntityName
		if strings.HasPrefix(strings.ToLower(key), microdataLcDot) {

			if !theCfg.isMicrodata {
				err = errors.New("Model run error: microdata not allowed: " + rs.ModelName + ": " + rs.ModelDigest)
				omppLog.Log(err)
				moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
				rs.IsFinal = true
				return rs, err // exit with error: microdata not allowed
			}
			subKey := key[len(microdataLcDot):]

			if strings.EqualFold(subKey, "All") || strings.EqualFold(subKey, "ToDb") || strings.EqualFold(subKey, "UseInternal") {
				err = errors.New("Model run error: incorrect use of run option: " + key + ": " + rs.ModelName + ": " + rs.ModelDigest)
				omppLog.Log(err)
				moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
				rs.IsFinal = true
				return rs, err // exit with error: incorrect microdata option
			}

			for k := range entAttrs {

				if subKey == entAttrs[k].Name {
					err = errors.New("Model run error: incorrect use of run option: " + key + ": " + rs.ModelName + ": " + rs.ModelDigest)
					omppLog.Log(err)
					moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
					rs.IsFinal = true
					return rs, err // exit with error: incorrect microdata option
				}
			}
		}

		mArgs = append(mArgs, key, val) // append command line argument key and value
	}

	// use job control resources if not explicitly disabled
	// recalculate number of MPI processes, modelling threads and create hostfile
	hfPath := ""
	if job.IsMpi && !job.Mpi.IsNotByJob {

		hfPath, job.Threads, err = createHostFile(job, hfCfg, compUse)

		if err != nil {
			omppLog.Log("Model run error: ", err)
			moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
			rs.IsFinal = true
			return rs, err
		}
	}

	// append threads number if required
	if job.Threads > 1 {
		mArgs = append(mArgs, "-OpenM.Threads", strconv.Itoa(job.Threads))
	}
	if job.IsMpi && job.Mpi.IsNotOnRoot {
		mArgs = append(mArgs, "-OpenM.NotOnRoot")
	}

	// if list of tables to retain is not empty then put the list into ini-file:
	//
	//   [Tables]
	//   Retain   = ageSexIncome, AdditionalTables
	//
	// if list of tables to retain is not empty then put the list into ini-file:
	//
	//   [Microdata]
	//   ToDb        = true
	//   UseInternal = true
	//   Person      = age,income
	//   Other       = All
	//
	iniContent := ""

	// append tables to retain to ini file content
	if len(job.Tables) > 0 {
		iniContent += "[Tables]" + "\n" + "Retain = " + strings.Join(job.Tables, ", ") + "\n"
	}

	// append microdata run options to ini file content
	if theCfg.isMicrodata && len(entAttrs) > 0 && job.Microdata.IsToDb && len(job.Microdata.Entity) > 0 {

		iniContent += "[Microdata]" + "\n" + "ToDb = true\n"

		if job.Microdata.IsInternal {
			iniContent += "UseInternal = true\n"
		}

		// for each entity check if All attributes included or attributes must be specified as comma separated list
		for k := range job.Microdata.Entity {

			// find entity name in the list of model entities
			eIdx := -1
			for j := range entAttrs {
				if entAttrs[j].Name == job.Microdata.Entity[k].Name {
					eIdx = j
					break
				}
			}
			if eIdx < 0 || eIdx >= len(entAttrs) {
				err = errors.New("Model run error: invalid microdata entity: " + job.Microdata.Entity[k].Name + ": " + rs.ModelName + ": " + rs.ModelDigest)
				omppLog.Log(err)
				moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
				rs.IsFinal = true
				return rs, err // exit with error: microdata entity name not found
			}

			// check if all entity attributes included in run microdata
			na := len(job.Microdata.Entity[k].Attr)
			isAll := na == 1 && job.Microdata.Entity[k].Attr[0] == "All"

			if !isAll {

				attrs := make([]string, na)
				copy(attrs, job.Microdata.Entity[k].Attr)
				sort.Strings(attrs)

				for j := range entAttrs[eIdx].Attr {

					if !job.Microdata.IsInternal && entAttrs[eIdx].Attr[j].IsInternal {
						continue // skip: this model run does not using internal attributes
					}

					n := sort.SearchStrings(attrs, entAttrs[eIdx].Attr[j].Name)
					isAll = n >= 0 && n < na && attrs[n] == entAttrs[eIdx].Attr[j].Name
					if !isAll {
						break
					}
				}
			}

			// append entity attributes to ini file content: EntityName = All or EntityName = AttrA, AttrB
			if isAll {
				iniContent += job.Microdata.Entity[k].Name + " = All\n"
			} else {
				iniContent += job.Microdata.Entity[k].Name + " = " + strings.Join(job.Microdata.Entity[k].Attr, ",") + "\n"
			}
		}
	}

	// create ini file and append -ini fileName.ini to model run options
	if iniContent != "" {
		p, e := filepath.Abs(filepath.Join(mb.logDir, rStamp+"."+mb.name+".ini"))
		if e == nil {
			e = os.WriteFile(p, []byte(iniContent), 0644)
		}
		awd := ""
		if e == nil {
			if awd, e = filepath.Abs(wDir); e == nil {
				p, e = filepath.Rel(awd, p)
			}
		}
		if e != nil {
			omppLog.Log("Model run error: ", e)
			moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
			rs.IsFinal = true
			return rs, e
		}
		mArgs = append(mArgs, "-ini", p) // append ini file path to command line arguments
	}

	// save run notes into the file(s) and append file path(s) to the model run options
	for _, rn := range job.RunNotes {
		if rn.Note == "" {
			continue
		}
		if !rs.IsLog {
			e := errors.New("Unable to save run notes: " + rs.ModelName + ": " + rs.ModelDigest)
			omppLog.Log("Model run error: ", e)
			moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
			rs.IsFinal = true
			return rs, e
		}

		p, e := filepath.Abs(filepath.Join(mb.logDir, rStamp+".run_notes."+rn.LangCode+".md"))
		if e == nil {
			e = os.WriteFile(p, []byte(rn.Note), 0644)
		}
		awd := ""
		if e == nil {
			if awd, e = filepath.Abs(wDir); e == nil {
				p, e = filepath.Rel(awd, p)
			}
		}
		if e != nil {
			omppLog.Log("Model run error: ", e)
			moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
			rs.IsFinal = true
			return rs, e
		}

		mArgs = append(mArgs, "-"+rn.LangCode+".RunNotesPath", p) // append run notes file path to command line arguments
	}

	// cleanup helpers
	delComputeUse := func(cuLst []computeUse) {
		for _, cu := range cuLst {
			if cu.filePath != "" {
				fileDeleteAndLog(false, cu.filePath)
			}
		}
	}
	cleanAndReturn := func(e error, rState *RunState, qPath string, cuLst []computeUse) (*RunState, error) {
		omppLog.Log("Error at starting model: ", e)
		delComputeUse(cuLst)
		moveJobQueueToFailed(qPath, rState.SubmitStamp, rState.ModelName, rState.ModelDigest, rState.RunStamp)
		rState.IsFinal = true
		return rState, errors.New("Error at starting model " + rState.ModelName + ": " + e.Error())
	}

	// assume model exe name is the same as model name
	mExe := helper.CleanPath(rs.ModelName)

	cmd, err := rsc.makeCommand(mExe, binDir, wDir, mb.dbPath, mArgs, hfPath, job.RunRequest)
	if err != nil {
		omppLog.Log("Error at starting model: ", err)
		moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
		rs.IsFinal = true
		return rs, errors.New("Error at starting model " + rs.ModelName + ": " + err.Error())
	}

	// create job usage file for each computational server
	isErr := false
	for k := 0; !isErr && k < len(compUse); k++ {

		compUse[k].filePath = compUsedPath(compUse[k].name, rs.SubmitStamp, compUse[k].Cpu, compUse[k].Mem)
		isErr = !fileCreateEmpty(false, compUse[k].filePath)
	}
	if isErr {
		omppLog.Log("Error at starting model: ", rs.ModelName, " ", rs.ModelDigest, " ", rs.SubmitStamp)
		delComputeUse(compUse)
		moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
		rs.IsFinal = true
		return rs, errors.New("Error at starting model " + rs.ModelName + " " + rs.ModelDigest)
	}

	// connect console output to log line array
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		return cleanAndReturn(err, rs, queueJobPath, compUse)
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		return cleanAndReturn(err, rs, queueJobPath, compUse)
	}
	outDoneC := make(chan bool, 1)
	errDoneC := make(chan bool, 1)
	rs.killC = make(chan bool, 1)
	logTck := time.NewTicker(logTickTimeout * time.Millisecond)

	// append console output to log lines array
	doLog := func(rState *RunState, r io.Reader, done chan<- bool) {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			rsc.updateRunStateLog(rState, false, sc.Text())
		}
		done <- true
		close(done)
	}

	// run state initialized: append it to the run state list
	// create model run log file
	rsc.createRunStateLog(rs)

	// start console output listners
	go doLog(rs, outPipe, outDoneC)
	go doLog(rs, errPipe, errDoneC)

	// start the model
	omppLog.Log("Run model: ", mExe, " in directory: ", wDir)
	if rs.logPath != "" {
		omppLog.Log("Run model: ", mExe, " log: ", rs.logPath)
	}
	omppLog.Log(strings.Join(cmd.Args, " "))
	rs.cmdPath = cmd.Path
	rsc.updateRunStateProcess(rs, false)

	err = cmd.Start()
	if err != nil {
		omppLog.Log("Model run error: ", err)
		delComputeUse(compUse)
		moveJobQueueToFailed(queueJobPath, rs.SubmitStamp, rs.ModelName, rs.ModelDigest, rStamp)
		rsc.updateRunStateLog(rs, true, err.Error())
		rs.IsFinal = true
		return rs, err // exit with error: model failed to start
	}
	// else model started
	rs.pid = cmd.Process.Pid
	rsc.updateRunStateProcess(rs, false)
	activeJobPath, _ := moveJobToActive(queueJobPath, *rs, job.Res, rStamp)

	//  wait until run completed or terminated
	go func(rState *RunState, cmd *exec.Cmd, jobPath string, cuLst []computeUse) {

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
			case isKill, ok := <-rs.killC:
				if !ok {
					rs.killC = nil
				}
				if isKill && ok {
					omppLog.Log("Kill run: ", rState.ModelName, " ", rState.ModelDigest, " ", rState.RunName, " ", rState.RunStamp)
					if e := cmd.Process.Kill(); e != nil {
						omppLog.Log(e)
					}
				}
			case <-logTck.C:
			}
		}

		// wait for model run to be completed
		e := cmd.Wait()
		if e != nil {
			omppLog.Log("Model run error: ", e)
			delComputeUse(cuLst)
			rsc.updateRunStateLog(rState, true, e.Error())
			moveActiveJobToHistory(jobPath, db.ErrorRunStatus, rState.SubmitStamp, rState.ModelName, rState.ModelDigest, rState.RunStamp)
			_, e = theCatalog.UpdateRunStatus(rState.ModelDigest, rState.RunStamp, db.ErrorRunStatus)
			if e != nil {
				omppLog.Log(e)
			}
			return
		}
		// else: completed OK
		rsc.updateRunStateLog(rState, true, "")
		delComputeUse(cuLst)
		moveActiveJobToHistory(jobPath, db.DoneRunStatus, rState.SubmitStamp, rState.ModelName, rState.ModelDigest, rState.RunStamp)

	}(rs, cmd, activeJobPath, compUse)

	return rs, nil
}

// makeCommand return command to run the model.
// If template file name specified then template processing results used to create command line.
// If this is MPI model run then tempalate is requred
// MPI run template can be model specific: "mpi.ModelName.template.txt" or default: "mpi.ModelRun.template.txt".
func (rsc *RunCatalog) makeCommand(mExe, binDir, workDir, dbPath string, mArgs []string, hfPath string, req RunRequest) (*exec.Cmd, error) {

	// check is it MPI model run, to run MPI model template is required
	if req.IsMpi && req.Template == "" {

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

	if !isTmpl && !req.IsMpi {
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
		wd, err := filepath.Abs(workDir)
		if err != nil {
			return nil, err
		}
		d := struct {
			ModelName string            // model name
			ExeStem   string            // base part of model exe name, usually modelName
			Dir       string            // work directory to run the model
			BinDir    string            // bin directory where model.exe is located
			DbPath    string            // path to sqlite database file: models/bin/model.sqlite
			MpiNp     int               // number of MPI processes
			HostFile  string            // if not empty then absolute path to hostfile
			Args      []string          // model command line arguments
			Env       map[string]string // environment variables to run the model
		}{
			ModelName: req.ModelName,
			ExeStem:   mExe,
			Dir:       wd,
			BinDir:    binDir,
			DbPath:    dbPath,
			MpiNp:     req.Mpi.Np,
			HostFile:  hfPath,
			Args:      mArgs,
			Env:       req.Env,
		}

		// execute template and convert results in array of text lines
		var b strings.Builder

		err = tmpl.Execute(&b, d)
		if err != nil {
			return nil, err
		}
		tLines := strings.Split(strings.ReplaceAll(b.String(), "\r", "\n"), "\n")

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
	if !req.IsMpi {

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

// stopModelRun kill model run by run stamp
// or remove run request from the queue by submit stamp or by run stamp
// return submission stamp, job file path and two flags: if model run found and if model is runniing now
func (rsc *RunCatalog) stopModelRun(modelDigest string, stamp string) (bool, string, string, bool) {

	tNow := time.Now()

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	// find model run state by digest and run stamp
	rsl := rsc.findRunStateLog(modelDigest, stamp)

	if rsl == nil { // if model run stamp and submit stamp not found then check if there is a job file in the queue

		if qj, ok := rsc.queueJobs[stamp]; ok {
			return true, stamp, qj.filePath, false // job file found in the queue
		}
		return false, "", "", false // no model run stamp and no submit stamp found
	}

	// find model in the active job list or if not active then find it in job queue
	jobPath := ""
	if aj, ok := rsc.activeJobs[rsl.SubmitStamp]; ok {
		jobPath = aj.filePath
	} else {
		if qj, ok := rsc.queueJobs[rsl.SubmitStamp]; ok {
			jobPath = qj.filePath
		}
	}

	rsl.UpdateDateTime = helper.MakeDateTime(tNow)

	// kill model run if model is running
	if rsl.killC != nil {
		rsl.killC <- true
		return true, rsl.SubmitStamp, jobPath, true
	}
	// else remove request from the queue
	rsl.IsFinal = true

	return true, rsl.SubmitStamp, jobPath, false
}
