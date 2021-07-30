// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// make dbcopy command to prepare full model download
func makeModelDownloadCommand(baseName string, mb modelBasic, logPath string) (*exec.Cmd, string) {

	// make dbcopy message for user log
	cmdMsg := "dbcopy -m " + mb.name + " -dbcopy.Zip -dbcopy.OutputDir " + theCfg.downloadDir

	// make absolute path to download directory: dbcopy work directory is a model bin directory
	absDownDir, err := filepath.Abs(theCfg.downloadDir)
	if err != nil {
		renameToDownloadErrorLog(logPath, "Error at starting "+cmdMsg)
		return nil, cmdMsg
	}

	// make dbcopy command
	cmd := exec.Command(
		theCfg.dbcopyPath, "-m", mb.name, "-dbcopy.Zip", "-dbcopy.OutputDir", absDownDir,
	)
	cmd.Dir = mb.binDir // dbcopy work directory is a model bin directory

	return cmd, cmdMsg
}

// make dbcopy command to prepare model run download
func makeRunDownloadCommand(baseName string, mb modelBasic, runId int, logPath string) (*exec.Cmd, string) {

	// make dbcopy message for user log
	cmdMsg := "dbcopy -m " + mb.name + " -dbcopy.RunId " + strconv.Itoa(runId) + " -dbcopy.Zip -dbcopy.OutputDir " + theCfg.downloadDir

	// make absolute path to download directory: dbcopy work directory is a model bin directory
	absDownDir, err := filepath.Abs(theCfg.downloadDir)
	if err != nil {
		renameToDownloadErrorLog(logPath, "Error at starting "+cmdMsg)
		return nil, cmdMsg
	}

	// make dbcopy command
	cmd := exec.Command(
		theCfg.dbcopyPath, "-m", mb.name, "-dbcopy.RunId", strconv.Itoa(runId), "-dbcopy.Zip", "-dbcopy.OutputDir", absDownDir,
	)
	cmd.Dir = mb.binDir // dbcopy work directory is a model bin directory

	return cmd, cmdMsg
}

// make dbcopy command to prepare model workset download
func makeWorksetDownloadCommand(baseName string, mb modelBasic, setName string, logPath string) (*exec.Cmd, string) {

	// make dbcopy message for user log
	cmdMsg := "dbcopy -m " + mb.name + " -dbcopy.SetName " + setName + " -dbcopy.Zip -dbcopy.OutputDir " + theCfg.downloadDir

	// make absolute path to download directory: dbcopy work directory is a model bin directory
	absDownDir, err := filepath.Abs(theCfg.downloadDir)
	if err != nil {
		renameToDownloadErrorLog(logPath, "Error at starting "+cmdMsg)
		return nil, cmdMsg
	}

	// make dbcopy command
	cmd := exec.Command(
		theCfg.dbcopyPath, "-m", mb.name, "-dbcopy.SetName", setName, "-dbcopy.Zip", "-dbcopy.OutputDir", absDownDir,
	)
	cmd.Dir = mb.binDir // dbcopy work directory is a model bin directory

	return cmd, cmdMsg
}

// makeDownload invoke dbcopy to create model download directory and .zip file:
// 1. delete existing: previous download log file, model.xyz.zip, model.xyz directory.
// 2. start dbcopy to do actual download.
// 3. if dbcopy done OK then rename log file into model......download.ready.log else into model......download.error.log
func makeDownload(baseName string, cmd *exec.Cmd, cmdMsg string, logPath string) {

	// delete existing (previous copy) of download data
	basePath := filepath.Join(theCfg.downloadDir, baseName)

	if !removeDownloadFile(basePath+".download.ready.log", logPath, "delete: "+baseName+".download.ready.log") {
		return
	}
	if !removeDownloadFile(basePath+".download.error.log", logPath, "delete: "+baseName+".download.error.log") {
		return
	}
	if !removeDownloadFile(basePath+".zip", logPath, "delete: "+baseName+".zip") {
		return
	}
	if !removeDownloadDir(basePath, logPath, "delete: "+baseName) {
		return
	}

	// connect console output to outout log
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		renameToDownloadErrorLog(logPath, "Error at starting "+cmdMsg)
		return
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		renameToDownloadErrorLog(logPath, "Error at starting "+cmdMsg)
		return
	}
	outDoneC := make(chan bool, 1)
	errDoneC := make(chan bool, 1)
	logTck := time.NewTicker(logTickTimeout * time.Millisecond)

	// append console output to log
	isLogOk := true
	doLog := func(path string, r io.Reader, done chan<- bool) {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			if isLogOk {
				isLogOk = appendToDownloadLog(path, false, sc.Text())
			}
		}
		done <- true
		close(done)
	}

	// start console output listners
	absLogPath, err := filepath.Abs(logPath)
	if err != nil {
		renameToDownloadErrorLog(logPath, "Error at starting "+cmdMsg)
		return
	}

	go doLog(absLogPath, outPipe, outDoneC)
	go doLog(absLogPath, errPipe, errDoneC)

	// start dbcopy
	omppLog.Log("Run dbcopy at: ", cmd.Dir)
	omppLog.Log(strings.Join(cmd.Args, " "))

	err = cmd.Start()
	if err != nil {
		omppLog.Log("Error: ", err)
		renameToDownloadErrorLog(logPath, err.Error())
		return
	}
	// else dbcopy started: wait until completed

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

	// wait for dbcopy to be completed
	e := cmd.Wait()
	if e != nil {
		omppLog.Log(e)
		appendToDownloadLog(logPath, true, e.Error())
		renameToDownloadErrorLog(logPath, "Error at: "+cmdMsg)
		return
	}
	// else: completed OK
	if !isLogOk {
		omppLog.Log("Warning: dbcopy log output may incomplete")
	}

	// all done, rename log file on success: model......download.progress.log into model......download.ready.log
	os.Rename(logPath, strings.TrimSuffix(logPath, ".progress.log")+".ready.log")
}

// remove download file and append log message, on error do rename log file into model......download.error.log and return false
func removeDownloadFile(path string, logPath string, msg string) bool {

	if !appendToDownloadLog(logPath, true, msg) {
		renameToDownloadErrorLog(logPath, "")
		return false
	}
	if e := os.Remove(path); e != nil && !os.IsNotExist(e) {
		omppLog.Log(e)
		renameToDownloadErrorLog(logPath, "Error at "+msg)
		return false
	}
	return true
}

// remove download directory and append log message, on error do rename log file into model......download.error.log and return false
func removeDownloadDir(path string, logPath string, msg string) bool {

	if !appendToDownloadLog(logPath, true, msg) {
		renameToDownloadErrorLog(logPath, "")
		return false
	}
	if e := os.RemoveAll(path); e != nil && !os.IsNotExist(e) {
		omppLog.Log(e)
		renameToDownloadErrorLog(logPath, "Error at "+msg)
		return false
	}
	return true
}

// create new download log file or truncate existing
func createDownloadLog(logPath string) (string, bool) {

	f, err := os.Create(logPath)
	if err != nil {
		return "", false
	}
	defer f.Close()
	return logPath, true
}

// rename log file on error: model......download.progress.log into model......download.error.log
func renameToDownloadErrorLog(logPath string, errMsg string) {
	if errMsg != "" {
		appendToDownloadLog(logPath, true, errMsg)
	}
	os.Rename(logPath, strings.TrimSuffix(logPath, ".progress.log")+".error.log")
}

// appendToDownloadLog append to mesaeg into download log file
func appendToDownloadLog(logPath string, isDoTimestamp bool, msg ...string) bool {

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return false // disable log on error
	}
	defer f.Close()

	tsPrefix := helper.MakeDateTime(time.Now()) + " "

	for _, m := range msg {
		if isDoTimestamp {
			if _, err = f.WriteString(tsPrefix); err != nil {
				return false // disable log on error
			}
		}
		if _, err = f.WriteString(m); err != nil {
			return false // disable log on error
		}
		if runtime.GOOS == "windows" { // adjust newline for windows
			_, err = f.WriteString("\r\n")
		} else {
			_, err = f.WriteString("\n")
		}
		if err != nil {
			return false
		}
	}
	return err == nil // disable log on error
}
