// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"path/filepath"
	"strconv"

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// isJobDirValid checking job control configuration.
// if job control directory is empty then job control disabled.
// if job control directory not empty then it must have active, queue and history subdirectories.
// if state.json exists then it must be a valid configuration file.
func isJobDirValid(jobDir string) error {

	if jobDir == "" {
		return nil // job control disabled
	}

	if err := isDirExist(jobDir); err != nil {
		return err
	}
	if err := isDirExist(filepath.Join(jobDir, "active")); err != nil {
		return err
	}
	if err := isDirExist(filepath.Join(jobDir, "queue")); err != nil {
		return err
	}
	if err := isDirExist(filepath.Join(jobDir, "history")); err != nil {
		return err
	}
	return nil
}

// retrun path job control file path if model run standing is queue
func jobQueuePath(stamp string, modelName string) string {
	return filepath.Join(theCfg.jobDir, "queue", stamp+"."+theCfg.omsName+"."+modelName+".json")
}

// retrun path job control file path if model is running now
func jobActivePath(stamp string, modelName string, pid int) string {
	return filepath.Join(theCfg.jobDir, "active", stamp+"."+theCfg.omsName+"."+modelName+"."+strconv.Itoa(pid)+".json")
}

// retrun path job control file path if model run completed successfully
func jobDonePath(stamp string, modelName string) string {
	return filepath.Join(theCfg.jobDir, "history", stamp+"."+theCfg.omsName+"."+modelName+".done.json")
}

// retrun path job control file path if model run failed
func jobErrorPath(stamp string, modelName string) string {
	return filepath.Join(theCfg.jobDir, "history", stamp+"."+theCfg.omsName+"."+modelName+".error.json")
}

// add new run request to job queue
func addJobToQueue(req *RunRequest) error {
	if !theCfg.isJobControl {
		return nil // job control is not required
	}

	fn := jobQueuePath(req.SubmitStamp, req.ModelName)
	err := helper.ToJsonFile(fn, req)
	if err != nil {
		omppLog.Log(err)
		fileDleteAndLog(true, fn)
		return err
	}
	return nil
}

// move model run request from queue to active model runs list when run started successfully
func moveJobToActive(stamp string, modelName string, pid int) bool {
	if !theCfg.isJobControl {
		return true // job control is not required
	}

	src := jobQueuePath(stamp, modelName)
	dst := jobActivePath(stamp, modelName, pid)

	if !fileMoveAndLog(true, src, dst) {
		fileDleteAndLog(true, src) // if move failed then delete job control file from queue
		return false
	}
	return true
}

// move active model run job control file to history
func moveJobToHistory(isOk bool, stamp string, modelName string, pid int) bool {
	if !theCfg.isJobControl {
		return true // job control is not required
	}

	src := jobActivePath(stamp, modelName, pid)
	var dst string
	if isOk {
		dst = jobDonePath(stamp, modelName)
	} else {
		dst = jobErrorPath(stamp, modelName)
	}

	if !fileMoveAndLog(true, src, dst) {
		fileDleteAndLog(true, src) // if move failed then delete job control file from active list
		return false
	}
	return true
}

// move model run request from queue to error if model run fail to start
func moveJobQueueToFailed(stamp string, modelName string) bool {
	if !theCfg.isJobControl {
		return true // job control is not required
	}

	src := jobQueuePath(stamp, modelName)
	dst := jobErrorPath(stamp, modelName)

	if !fileMoveAndLog(true, src, dst) {
		fileDleteAndLog(true, src) // if move failed then delete job control file from queue
		return false
	}
	return true
}
