// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// jobDirValid checking job control configuration.
// if job control directory is empty then job control disabled.
// if job control directory not empty then it must have active, queue, history and state subdirectories.
// if state.json exists then it must be a valid configuration file.
func jobDirValid(jobDir string) error {

	if jobDir == "" {
		return nil // job control disabled
	}

	if err := dirExist(jobDir); err != nil {
		return err
	}
	if err := dirExist(filepath.Join(jobDir, "active")); err != nil {
		return err
	}
	if err := dirExist(filepath.Join(jobDir, "queue")); err != nil {
		return err
	}
	if err := dirExist(filepath.Join(jobDir, "history")); err != nil {
		return err
	}
	if err := dirExist(filepath.Join(jobDir, "state")); err != nil {
		return err
	}
	return nil
}

// return job control file path if model is running now.
// For example: 2022_07_08_23_03_27_555-#-_4040-#-RiskPaths-#-d90e1e9a-#-mpi-#-cpu-#-8-#-mem-#-4-#-8888.json
func jobActivePath(submitStamp, modelName, modelDigest string, isMpi bool, pid int, cpu int, mem int) string {

	ml := "local"
	if isMpi {
		ml = "mpi"
	}
	return filepath.Join(
		theCfg.jobDir,
		"active",
		submitStamp+"-#-"+theCfg.omsName+"-#-"+modelName+"-#-"+modelDigest+"-#-"+ml+"-#-cpu-#-"+strconv.Itoa(cpu)+"-#-mem-#-"+strconv.Itoa(mem)+"-#-"+strconv.Itoa(pid)+".json")
}

// return path job control file path if model run standing is queue
// For example: 2022_07_05_19_55_38_111-#-_4040-#-RiskPaths-#-d90e1e9a-#-mpi-cpu-#-8-#-mem-#-4.json
func jobQueuePath(submitStamp, modelName, modelDigest string, isMpi bool, position int, cpu int, mem int) string {

	ml := "local"
	if isMpi {
		ml = "mpi"
	}
	return filepath.Join(
		theCfg.jobDir,
		"queue",
		submitStamp+"-#-"+theCfg.omsName+"-#-"+modelName+"-#-"+modelDigest+"-#-"+ml+"-#-cpu-#-"+strconv.Itoa(cpu)+"-#-mem-#-"+strconv.Itoa(mem)+"-#-"+strconv.Itoa(position)+".json")
}

// return job control file path to completed model with run status suffix.
// For example: 2022_07_04_20_06_10_817-#-_4040-#-RiskPaths-#-d90e1e9a-#-2022_07_04_20_06_10_818-#-success.json
func jobHistoryPath(status, submitStamp, modelName, modelDigest, runStamp string) string {
	return filepath.Join(
		theCfg.jobDir,
		"history",
		submitStamp+"-#-"+theCfg.omsName+"-#-"+modelName+"-#-"+modelDigest+"-#-"+runStamp+"-#-"+db.NameOfRunStatus(status)+".json")
}

// return job state file path e.g.: job/state/_4040.json
func jobStatePath() string {
	return filepath.Join(theCfg.jobDir, "state", theCfg.omsName+".json")
}

// return job queue paused file path e.g.: job/state/jobs.queue.paused
func jobQueuePausedPath() string {
	return filepath.Join(theCfg.jobDir, "state", "jobs.queue.paused")
}

// return limit file paths, zero or negative value means unlimited e.g.: job/state/total-limit-cpu-#-64
func jobLimitPath(kind string, value int) string {
	return filepath.Join(theCfg.jobDir, "state", kind+"-#-"+strconv.Itoa(value))
}

// return compute server or cluster ready file path: job/state/comp-ready-#-name
func compReadyPath(name string) string {
	return filepath.Join(theCfg.jobDir, "state", "comp-ready-#-"+name)
}

// return server used by model run path prefix and file path.
// For example: job/state/comp-used-#-name-#-2022_07_08_23_03_27_555-#-_4040-#-cpu-#-4-#-mem-#-8
func compUsedPath(name, submitStamp string, cpu, mem int) string {
	return filepath.Join(
		theCfg.jobDir,
		"state",
		"comp-used-#-"+name+"-#-"+submitStamp+"-#-"+theCfg.omsName+"-#-cpu-#-"+strconv.Itoa(cpu)+"-#-mem-#-"+strconv.Itoa(mem))
}

// parse job file path or job file name:
// remove .json extension and directory prefix
// return submission stamp, oms instance name, model name, model digest and the rest of the file name
func parseJobPath(srcPath string) (string, string, string, string, string) {

	// remove job directory and extension, file extension must be .json
	if filepath.Ext(srcPath) != ".json" {
		return "", "", "", "", ""
	}
	p := filepath.Base(srcPath)
	p = p[:len(p)-len(".json")]

	// check result: it must be at least 4 non-empty parts and first must be a time stamp
	sp := strings.SplitN(p, "-#-", 5)
	if len(sp) < 4 || !helper.IsUnderscoreTimeStamp(sp[0]) || sp[1] == "" || sp[2] == "" || sp[3] == "" {
		return "", "", "", "", "" // source file path is not job file
	}

	if len(sp) == 4 {
		return sp[0], sp[1], sp[2], sp[3], "" // only 4 parts, the rest of source file name is empty
	}
	return sp[0], sp[1], sp[2], sp[3], sp[4]
}

// parse active job file path or queue file path (or file name)
// and return submission stamp, oms instance name, model name, digest, MPI or local, cpu count, memory size and active job pid or queue job position.
// For example: 2022_07_05_19_55_38_111-#-_4040-#-RiskPaths-#-d90e1e9a-#-mpi-#-cpu-#-8-#-mem-#-4-#-8888.json
func parseJobActPath(srcPath string) (string, string, string, string, bool, int, int, int) {

	// parse common job file part
	subStamp, oms, mn, dgst, p := parseJobPath(srcPath)

	if subStamp == "" || oms == "" || mn == "" || dgst == "" || p == "" {
		return subStamp, oms, "", "", false, 0, 0, 0 // source file path is not active or queue job file
	}

	// parse cpu count and memory size, 5 parts expected
	sp := strings.Split(p, "-#-")
	if len(sp) != 6 ||
		(sp[0] != "mpi" && sp[0] != "local") ||
		sp[1] != "cpu" || sp[2] == "" ||
		sp[3] != "mem" || sp[4] == "" ||
		sp[5] == "" {
		return subStamp, oms, "", "", false, 0, 0, 0 // source file path is not active or queue job file
	}
	isMpi := sp[0] == "mpi"

	// parse and convert cpu count and memory size
	cpu, err := strconv.Atoi(sp[2])
	if err != nil || cpu <= 0 {
		return subStamp, oms, "", "", false, 0, 0, 0 // cpu count must be positive integer
	}
	mem, err := strconv.Atoi(sp[4])
	if err != nil || mem < 0 {
		return subStamp, oms, "", "", false, 0, 0, 0 // memory size must be non-negative integer
	}
	pos, err := strconv.Atoi(sp[5])
	if err != nil || pos < 0 {
		return subStamp, oms, "", "", false, 0, 0, 0 // position must be non-negative integer
	}

	return subStamp, oms, mn, dgst, isMpi, cpu, mem, pos
}

// parse active file path or active file name
// and return submission stamp, oms instance name, model name, digest, MPI or local, cpu count, memory size and process id.
// For example: 2022_07_08_23_03_27_555-#-_4040-#-RiskPaths-#-d90e1e9a-#-mpi-#-cpu-#-8-#-mem-#-4-#-8888.json
func parseActivePath(srcPath string) (string, string, string, string, bool, int, int, int) {
	return parseJobActPath(srcPath)
}

// parse queue file path or queue file name
// and return submission stamp, oms instance name, model name, digest, MPI or local, cpu count, memory size and job position in queue.
// For example: 2022_07_05_19_55_38_111-#-_4040-#-RiskPaths-#-d90e1e9a-#-mpi-#-cpu-#-8-#-mem-#-4-#-20220817.json
func parseQueuePath(srcPath string) (string, string, string, string, bool, int, int, int) {
	return parseJobActPath(srcPath)
}

// parse history file path or history file name and
// return submission stamp, oms instance name, model name, digest, run stamp and run status.
// For example: 2022_07_04_20_06_10_817-#-_4040-#-RiskPaths-#-d90e1e9a-#-2022_07_04_20_06_10_818-#-success.json
func parseHistoryPath(srcPath string) (string, string, string, string, string, string) {

	// parse common job file part
	subStamp, oms, mn, dgst, p := parseJobPath(srcPath)

	if subStamp == "" || oms == "" || mn == "" || dgst == "" || len(p) < len("r-#-s") {
		return subStamp, oms, "", "", "", "" // source file path is not history job file
	}

	// get run stamp and status
	sp := strings.Split(p, "-#-")
	if len(sp) != 2 || sp[0] == "" || sp[1] == "" {
		return subStamp, oms, "", "", "", "" // source file path is not history job file
	}

	return subStamp, oms, mn, dgst, sp[0], sp[1]
}

// parse oms heart beat tick file path: job/state/oms-#-_4040-#-2022_07_08_23_45_12_123-#-1257894000000
// return oms instance name time stamp and clock ticks.
func parseOmsTickPath(srcPath string) (string, string, int64) {

	p := filepath.Base(srcPath) // remove job directory

	// split file name and check result: it must be 4 non-empty parts with time stamp
	sp := strings.Split(p, "-#-")
	if len(sp) != 4 || sp[0] != "oms" || sp[1] == "" || sp[3] == "" || !helper.IsUnderscoreTimeStamp(sp[2]) {
		return "", "", 0 // source file path is not job file
	}

	// convert clock ticks
	tickMs, err := strconv.ParseInt(sp[3], 10, 64)
	if err != nil || tickMs <= minJobTickMs {
		return "", "", 0 // clock ticks must after 2020-08-17 23:45:59
	}

	return sp[1], sp[2], tickMs
}

// parse limit file path or file name and return a limit, zero or negative value means unlimited.
// For example this is 64 cores total limit: job/state/total-limit-cpu-#-64
func parseLimitPath(srcPath string, kind string) int {

	if srcPath == "" || kind == "" {
		return 0 // source file path is not a total limit file or invalid (empty) limit kind specified
	}

	p := filepath.Base(srcPath) // remove job directory

	// split file name and check result: it must be 2 non-empty parts and first must be a limit kind
	sp := strings.Split(p, "-#-")
	if len(sp) != 2 || kind == "" || sp[0] != kind || sp[1] == "" {
		return 0 // source file path is not a total limit file
	}

	// convert limit value
	n, err := strconv.Atoi(sp[1])
	if err != nil || n <= 0 {
		return 0 // limit value invalid (not an integer) or unlimited (zero or negative)
	}
	return n
}

// parse compute server or cluster ready file path and return server name, e.g.: job/state/comp-ready-#-name
func parseCompReadyPath(srcPath string) string {

	p := filepath.Base(srcPath) // remove job directory

	// split file name and check result: it must be 2 non-empty parts
	sp := strings.Split(p, "-#-")
	if len(sp) != 2 || sp[0] != "comp-ready" || sp[1] == "" {
		return "" // source file path is not compute server ready path
	}

	return sp[1]
}

// parse compute server or cluster state file path and return server name, time stamp and clock ticks.
// State must be one of: start, stop, error.
// For example: job/state/comp-start-#-name-#-2022_07_08_23_45_12_123-#-1257894000000
func parseCompStatePath(srcPath, state string) (string, string, int64) {

	p := filepath.Base(srcPath) // remove job directory

	// split file name and check result: it must be 4 non-empty parts and with time stamp
	sp := strings.Split(p, "-#-")
	if len(sp) != 4 || sp[0] != "comp-"+state || sp[1] == "" || !helper.IsUnderscoreTimeStamp(sp[2]) || sp[3] == "" {
		return "", "", 0 // source file path is not compute server state path
	}

	// convert clock ticks
	tickMs, err := strconv.ParseInt(sp[3], 10, 64)
	if err != nil || tickMs <= minJobTickMs {
		return "", "", 0 // clock ticks must after 2020-08-17 23:45:59
	}

	return sp[1], sp[2], tickMs
}

// parse compute server used by model run file path
// and return server name, submission stamp, oms instance name, cpu count and memory size.
// For example: job/state/comp-used-#-name-#-2022_07_08_23_03_27_555-#-_4040-#-cpu-#-4-#-mem-#-8
func parseCompUsedPath(srcPath string) (string, string, string, int, int) {

	p := filepath.Base(srcPath) // remove job directory

	// split file name and check result: it must be 8 non-empty parts
	// and include submission stamp, cpu count and memory size
	sp := strings.Split(p, "-#-")
	if len(sp) != 8 || sp[0] != "comp-used" ||
		sp[1] == "" || !helper.IsUnderscoreTimeStamp(sp[2]) || sp[3] == "" ||
		sp[4] != "cpu" || sp[5] == "" || sp[6] != "mem" || sp[7] == "" {
		return "", "", "", 0, 0 // source file path is not compute server used by model run path
	}

	// parse and convert cpu count and memory size
	cpu, err := strconv.Atoi(sp[5])
	if err != nil || cpu <= 0 {
		return "", "", "", 0, 0 // cpu count must be positive integer
	}
	mem, err := strconv.Atoi(sp[7])
	if err != nil || mem < 0 {
		return "", "", "", 0, 0 // memory size must be non-negative integer
	}

	return sp[1], sp[2], sp[3], cpu, mem
}

// move run job to active state from queue
func moveJobToActive(queueJobPath string, rState RunState, res RunRes, runStamp string) (string, bool) {
	if !theCfg.isJobControl {
		return "", true // job control disabled
	}

	// read run request from job queue
	var jc RunJob
	isOk, err := helper.FromJsonFile(queueJobPath, &jc)
	if err != nil {
		omppLog.Log(err)
	}
	if !isOk || err != nil {
		fileDeleteAndLog(true, queueJobPath) // invalid file content: remove job control file from queue
		return "", false
	}

	// add run stamp, process info, actual run resources, log file and move job control file into active
	jc.RunStamp = runStamp
	jc.Pid = rState.pid
	jc.CmdPath = rState.cmdPath
	jc.Res = res
	jc.LogFileName = rState.LogFileName
	jc.LogPath = rState.logPath

	dst := jobActivePath(rState.SubmitStamp, rState.ModelName, rState.ModelDigest, jc.IsMpi, rState.pid, jc.Res.Cpu, jc.Res.Mem)

	fileDeleteAndLog(false, queueJobPath) // remove job control file from queue

	err = helper.ToJsonIndentFile(dst, &jc)
	if err != nil {
		omppLog.Log(err)
		fileDeleteAndLog(true, dst) // on error remove file, if any file created
		return "", false
	}

	return dst, true
}

// move active model run job control file to history
func moveActiveJobToHistory(activePath, status string, submitStamp, modelName, modelDigest, runStamp string) bool {
	if !theCfg.isJobControl {
		return true // job control disabled
	}

	// move active job file to history
	dst := jobHistoryPath(status, submitStamp, modelName, modelDigest, runStamp)

	isOk := fileMoveAndLog(false, activePath, dst)
	if !isOk {
		fileDeleteAndLog(true, activePath) // if move failed then delete job control file from active list
	}

	// remove all compute server usage files
	// for example: job/state/comp-used-#-name-#-2022_07_08_23_03_27_555-#-_4040-#-cpu-#-4-#-mem-#-8
	ptrn := filepath.Join(theCfg.jobDir, "state") + string(filepath.Separator) + "comp-used-#-*-#-" + submitStamp + "-#-" + theCfg.omsName + "-#-cpu-#-*-#-mem-#-*"

	if fLst, err := filepath.Glob(ptrn); err == nil {
		for _, f := range fLst {
			fileDeleteAndLog(false, f)
		}
	}
	return isOk
}

// move model run request from queue to error if model run fail to start
func moveJobQueueToFailed(queuePath string, submitStamp, modelName, modelDigest string) bool {
	if !theCfg.isJobControl || queuePath == "" {
		return true // job control disabled
	}

	dst := jobHistoryPath(db.ErrorRunStatus, submitStamp, modelName, modelDigest, "no-run-time-stamp")

	if !fileMoveAndLog(true, queuePath, dst) {
		fileDeleteAndLog(true, queuePath) // if move failed then delete job control file from queue
		return false
	}
	return true
}

// read run title from job json file: return run name or task run name or workset name
func getJobRunTitle(filePath string) string {
	if !theCfg.isJobControl {
		return "" // job control disabled
	}

	// read run request from job queue
	var jc RunJob
	isOk, err := helper.FromJsonFile(filePath, &jc)
	if err != nil {
		omppLog.Log(err)
	}
	if !isOk || err != nil {
		return ""
	}

	// find run name or task run or workset name in model run options
	runName := ""
	taskRunName := ""
	wsName := ""
	for krq, val := range jc.Opts {

		// remove "-" from command line argument key ex: "-OpenM.Threads"
		key := krq
		if krq[0] == '-' {
			key = krq[1:]
		}

		if strings.EqualFold(key, "OpenM.RunName") {
			runName = val
		}
		if strings.EqualFold(key, "OpenM.TaskRunName") {
			taskRunName = val
		}
		if strings.EqualFold(key, "OpenM.SetName") {
			wsName = val
		}
	}

	if runName != "" {
		return runName
	}
	if taskRunName != "" {
		return taskRunName
	}
	return wsName
}

// remove all existing oms heart beat tick files and create new oms heart beat tick file with current timestamp.
// return oms heart beat file path and true is file created successfully.
// For example: job/state/oms-#-_4040-#-2022_07_08_23_45_12_123-#-1257894000000
func makeOmsTick() (string, bool) {

	// create new oms heart beat tick file
	p := filepath.Join(theCfg.jobDir, "state", "oms-#-"+theCfg.omsName)
	ts := time.Now()
	fnow := p + "-#-" + helper.MakeTimeStamp(ts) + "-#-" + strconv.FormatInt(ts.UnixMilli(), 10)

	isOk := fileCreateEmpty(false, fnow)

	// delete existing heart beat files for our oms instance
	omsFiles := filesByPattern(p+"-#-*-#-*", "Error at oms heart beat files search")
	for _, f := range omsFiles {
		if f != fnow {
			fileDeleteAndLog(false, f)
		}
	}
	return fnow, isOk
}

// create new compute server file with current timestamp.
// For example: job/state/comp-start-#-name-#-2022_07_08_23_45_12_123-#-1257894000000
func createCompStateFile(name, state string) string {

	ts := time.Now()
	fp := filepath.Join(
		theCfg.jobDir,
		"state",
		"comp-"+state+"-#-"+name+"-#-"+helper.MakeTimeStamp(ts)+"-#-"+strconv.FormatInt(ts.UnixMilli(), 10))

	if !fileCreateEmpty(false, fp) {
		fp = ""
	}
	return fp
}

// remove all existing compute server state files, log delete errors, return true on success or false or errors.
// Create error state file if any delete failed.
// For example: job/state/comp-error-#-name-#-2022_07_08_23_45_12_123-#-1257894000000
func deleteCompStateFiles(name, state string) bool {

	p := filepath.Join(theCfg.jobDir, "state", "comp-"+state+"-#-"+name)

	isNoError := true

	fl := filesByPattern(p+"-#-*-#-*", "Error at server state files search")
	for _, f := range fl {
		isOk := fileDeleteAndLog(false, f)
		isNoError = isNoError && isOk
		if !isOk {
			createCompStateFile(name, "error")
			omppLog.Log("FAILED to delete state file: ", state, " ", name, " ", p)
		}
	}

	return isNoError
}

// read job control state from the file, return empty state on error or if state file not exist
func jobStateRead() (*jobControlState, bool) {

	var jcs jobControlState
	isOk, err := helper.FromJsonFile(jobStatePath(), &jcs)
	if err != nil {
		omppLog.Log(err)
	}
	if !isOk || err != nil {
		return &jobControlState{Queue: []string{}}, false
	}
	return &jcs, true
}

// save job control state into the file, return false on error
func jobStateWrite(jsc jobControlState) bool {

	err := helper.ToJsonIndentFile(jobStatePath(), jsc)
	if err != nil {
		omppLog.Log(err)
		return false
	}
	return true
}

// return true if jobs queue processing is paused
func isPausedJobQueue() bool {
	return fileExist(jobQueuePausedPath()) == nil
}

// create MPI job hostfile, e.g.: models/log/host-2022_07_08_23_03_27_555-_4040.ini
// retun path to host file, number of modelling threads per process and error
func createHostFile(job *RunJob, hfCfg hostIni, compUse []computeUse) (string, int, error) {

	if !job.IsMpi || !hfCfg.isUse || len(compUse) <= 0 { // hostfile is not required
		return "", job.Threads, nil
	}

	// number of modelling threads:
	// it must be <= max threads from run request, e.g. if user request max threads == 1 then model is single threaded
	// number of threads can be limited by job.ini to avoid excessive threads in single model process
	// number of threads is greatest common divisor to between all avaliable CPU cores on each server
	nTh := job.Threads
	if nTh <= 0 {
		nTh = 1
	}
	if !job.Mpi.IsNotByJob {

		if hfCfg.maxThreads > 0 && nTh > hfCfg.maxThreads { // number of threads is limited in job.ini
			nTh = hfCfg.maxThreads
		}
		for k := range compUse {
			nTh = helper.Gcd2(nTh, compUse[k].Cpu)
		}
	}

	/*
	   ; MS-MPI hostfile
	   ;
	   ; cpm:1
	   ; cpc-1:2
	   ; cpc-3:4
	   ;
	   [hostfile]
	   HostFileDir = models\log
	   HostName = @-HOST-@
	   CpuCores = @-CORES-@
	   RootLine = cpm:1
	   HostLine = @-HOST-@:@-CORES-@

	   ; OpenMPI hostfile
	   ;
	   ; cpm   slots=1 max_slots=1
	   ; cpc-1 slots=2
	   ; cpc-3 slots=4
	   ;
	   [hostfile]
	   HostFileDir = models/log
	   HostName = @-HOST-@
	   CpuCores = @-CORES-@
	   RootLine = cpm slots=1 max_slots=1
	   HostLine = @-HOST-@ slots=@-CORES-@
	*/
	// first line is root process host
	ls := []string{}

	if hfCfg.rootLine != "" {
		ls = append(ls, hfCfg.rootLine)
	}

	// for each server substitute host name and cores count
	if hfCfg.hostLine != "" {
		for _, cu := range compUse {

			ln := hfCfg.hostLine

			if hfCfg.hostName != "" {
				ln = strings.ReplaceAll(ln, hfCfg.hostName, cu.name)
			}
			if hfCfg.cpuCores != "" {
				if !job.Mpi.IsNotByJob {
					ln = strings.ReplaceAll(ln, hfCfg.cpuCores, strconv.Itoa(cu.Cpu/nTh))
				} else {
					ln = strings.ReplaceAll(ln, hfCfg.cpuCores, strconv.Itoa(cu.Cpu))
				}
			}
			ls = append(ls, ln)
		}
	}

	// write all lines into hostfile: /ompp/models/log/host-2022_07_08_23_03_27_555-_4040.ini
	hfPath := ""
	var err error
	if len(ls) > 0 {

		fn := "host-" + job.SubmitStamp + "-" + theCfg.omsName + ".ini"
		hfPath, err = filepath.Abs(filepath.Join(hfCfg.dir, fn))
		if err == nil {
			err = os.WriteFile(hfPath, []byte(strings.Join(ls, "\n")+"\n"), 0644)
		}
		if err != nil {
			omppLog.Log("Error at write into ", fn, ": ", err)
			return "", job.Threads, err
		}

		omppLog.Log("Run job: ", job.SubmitStamp, " ", job.ModelName, " hostfile: ", hfPath)
		for _, ln := range ls {
			omppLog.Log(ln)
		}
	}

	return hfPath, nTh, nil
}
