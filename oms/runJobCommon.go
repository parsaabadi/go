// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
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
// For example: 2022_07_08_23_03_27_555-#-_4040-#-RiskPaths-#-d90e1e9a-#-cpu-#-8-#-mem-#-4-#-8888.json
func jobActivePath(submitStamp, modelName, modelDigest string, pid int, cpu int, mem int) string {
	return filepath.Join(
		theCfg.jobDir,
		"active",
		submitStamp+"-#-"+theCfg.omsName+"-#-"+modelName+"-#-"+modelDigest+"-#-cpu-#-"+strconv.Itoa(cpu)+"-#-mem-#-"+strconv.Itoa(mem)+"-#-"+strconv.Itoa(pid)+".json")
}

// return path job control file path if model run standing is queue
// For example: 2022_07_05_19_55_38_111-#-_4040-#-RiskPaths-#-d90e1e9a-cpu-#-8-#-mem-#-4.json
func jobQueuePath(submitStamp, modelName, modelDigest string, position int, cpu int, mem int) string {
	return filepath.Join(
		theCfg.jobDir,
		"queue",
		submitStamp+"-#-"+theCfg.omsName+"-#-"+modelName+"-#-"+modelDigest+"-#-cpu-#-"+strconv.Itoa(cpu)+"-#-mem-#-"+strconv.Itoa(mem)+"-#-"+strconv.Itoa(position)+".json")
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

// return compute server or cluster resources file path: job/state/comp-#-name-#-cpu-#-8-#-mem-#-16
func compPath(name string, cpu, mem int) string {
	return filepath.Join(theCfg.jobDir, "state", "comp#-"+name+"-#-cpu-#-"+strconv.Itoa(cpu)+"-#-mem-#-"+strconv.Itoa(mem))
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
		"comp-used-#-"+name+"-#-"+submitStamp+"-#-"+theCfg.omsName+"-#-"+"-#-cpu-#-"+strconv.Itoa(cpu)+"-#-mem-#-"+strconv.Itoa(mem))
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
// and return submission stamp, oms instance name, model name, digest, cpu count, memory size and active job pid or queue job position.
// For example: 2022_07_05_19_55_38_111-#-_4040-#-RiskPaths-#-d90e1e9a-#-cpu-#-8-#-mem-#-4-#-8888.json
func parseJobActPath(srcPath string) (string, string, string, string, int, int, int) {

	// parse common job file part
	subStamp, oms, mn, dgst, p := parseJobPath(srcPath)

	if subStamp == "" || oms == "" || mn == "" || dgst == "" || p == "" {
		return subStamp, oms, "", "", 0, 0, 0 // source file path is not active or queue job file
	}

	// parse cpu count and memory size, 5 parts expected
	sp := strings.Split(p, "-#-")
	if len(sp) != 5 || sp[0] != "cpu" || sp[1] == "" || sp[2] != "mem" || sp[3] == "" || sp[4] == "" {
		return subStamp, oms, "", "", 0, 0, 0 // source file path is not active or queue job file
	}

	// parse and convert cpu count and memory size
	cpu, err := strconv.Atoi(sp[1])
	if err != nil || cpu <= 0 {
		return subStamp, oms, "", "", 0, 0, 0 // cpu count must be positive integer
	}
	mem, err := strconv.Atoi(sp[3])
	if err != nil || mem < 0 {
		return subStamp, oms, "", "", 0, 0, 0 // memory size must be non-negative integer
	}
	pos, err := strconv.Atoi(sp[4])
	if err != nil || pos < 0 {
		return subStamp, oms, "", "", 0, 0, 0 // position must be non-negative integer
	}

	return subStamp, oms, mn, dgst, cpu, mem, pos
}

// parse active file path or active file name
// and return submission stamp, oms instance name, model name, digest, cpu count, memory size and process id.
// For example: 2022_07_08_23_03_27_555-#-_4040-#-RiskPaths-#-d90e1e9a-#-cpu-#-8-#-mem-#-4-#-8888.json
func parseActivePath(srcPath string) (string, string, string, string, int, int, int) {
	return parseJobActPath(srcPath)
}

// parse queue file path or queue file name
// and return submission stamp, oms instance name, model name, digest, cpu count, memory size and job position in queue.
// For example: 2022_07_05_19_55_38_111-#-_4040-#-RiskPaths-#-d90e1e9a-#-cpu-#-8-#-mem-#-4-#-20220817.json
func parseQueuePath(srcPath string) (string, string, string, string, int, int, int) {
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

// parse compute server or cluster resources file path and return server name, cpu count, memory size.
// For example: job/state/comp-#-name-#-cpu-#-8-#-mem-#-16
func parseCompPath(srcPath string) (string, int, int) {

	p := filepath.Base(srcPath) // remove job directory

	// split file name and check result: it must be 6 non-empty parts with cpu count and memory size
	sp := strings.Split(p, "-#-")
	if len(sp) != 6 || sp[0] != "comp" || sp[1] == "" || sp[2] != "cpu" || sp[3] == "" || sp[4] != "mem" || sp[5] == "" {
		return "", 0, 0 // source file path is not compute resources path
	}

	// parse and convert cpu count and memory size
	cpu, err := strconv.Atoi(sp[3])
	if err != nil || cpu <= 0 {
		return "", 0, 0 // cpu count must be positive integer
	}
	mem, err := strconv.Atoi(sp[5])
	if err != nil || mem < 0 {
		return "", 0, 0 // memory size must be non-negative integer
	}

	return sp[1], cpu, mem
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
	if len(sp) != 3 || sp[0] != "comp-"+state || sp[1] == "" || sp[3] == "" || !helper.IsUnderscoreTimeStamp(sp[2]) {
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
	if len(sp) != 9 || sp[0] != "comp-used" ||
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

	dst := jobActivePath(rState.SubmitStamp, rState.ModelName, rState.ModelDigest, rState.pid, jc.Res.Cpu, jc.Res.Mem)

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
func moveJobToHistory(activePath, status string, submitStamp, modelName, modelDigest, runStamp string) bool {
	if !theCfg.isJobControl {
		return true // job control disabled
	}

	dst := jobHistoryPath(status, submitStamp, modelName, modelDigest, runStamp)

	if !fileMoveAndLog(false, activePath, dst) {
		fileDeleteAndLog(true, activePath) // if move failed then delete job control file from active list
		return false
	}
	return true
}

// move outer model run job control file to history
func moveOuterJobToHistory(srcPath, status string, submitStamp, modelName, modelDigest, runStamp string) bool {
	if !theCfg.isJobControl {
		return true // job control disabled
	}

	dst := jobHistoryPath(status, submitStamp, modelName, modelDigest, runStamp)

	if !fileMoveAndLog(true, srcPath, dst) {
		fileDeleteAndLog(true, srcPath) // if move failed then delete job control file from active list
		return false
	}
	return true
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

// remove all existing oms heart beat tick files and create new oms heart beat tick file with current timestamp.
// For example: job/state/oms-#-_4040-#-2022_07_08_23_45_12_123-#-1257894000000
func createOmsTick() (string, string) {

	p := filepath.Join(theCfg.jobDir, "state", "oms-#-"+theCfg.omsName)

	// delete existing heart beat files for our oms instance
	omsFiles := filesByPattern(p+"-#-*-#-*", "Error at oms heart beat files search")
	for _, f := range omsFiles {
		fileDeleteAndLog(false, f)
	}

	// create new oms heart beat tick file
	ts := time.Now()
	fp := p + "-#-" + helper.MakeTimeStamp(ts) + "-#-" + strconv.FormatInt(ts.UnixMilli(), 10)

	if !fileCreateEmpty(false, fp) {
		fp = ""
		p = ""
	}
	return fp, p
}

// update oms heart beat tick file path with current timestamp: oms instance is alive
func moveToNextOmsTick(srcPath, stem string) (string, bool) {
	if !theCfg.isJobControl || srcPath == "" {
		return "", false // job control disabled or job run state file error
	}

	// rename oms heart beat tick file
	ts := time.Now()
	dst := stem + "-#-" + helper.MakeTimeStamp(ts) + "-#-" + strconv.FormatInt(ts.UnixMilli(), 10)

	if !fileMoveAndLog(false, srcPath, dst) {
		fileDeleteAndLog(true, srcPath) // if move failed then delete active run job state file
		return "", false
	}
	return dst, true
}

// remove all existing compute server state files and create new compute server file with current timestamp.
// For example: job/state/comp-start-#-name-#-2022_07_08_23_45_12_123-#-1257894000000
func createCompState(name, state string) string {

	p := filepath.Join(theCfg.jobDir, "state", "comp-"+state+"-#-"+name)

	// delete existing state files for this server state
	fl := filesByPattern(p+"-#-*-#-*", "Error at server state files search")
	for _, f := range fl {
		fileDeleteAndLog(false, f)
	}

	// create new server state file
	ts := time.Now()
	fp := p + "-#-" + helper.MakeTimeStamp(ts) + "-#-" + strconv.FormatInt(ts.UnixMilli(), 10)

	if !fileCreateEmpty(false, fp) {
		fp = ""
	}
	return fp
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
