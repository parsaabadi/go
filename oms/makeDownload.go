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

// DownloadStatusLog contains download status info and content of log file
type DownloadStatusLog struct {
	Status      string   // if not empty then one of: progress ready error
	Kind        string   // if not empty then one of: model run workset
	ModelDigest string   // content of Model Digest:
	RunDigest   string   // content of Run  Digest:
	WorksetName string   // contenet of Scenario Name:
	Folder      string   // content of Folder:
	IsFolder    bool     // if true then download folder exist
	ZipFileName string   // zip file name
	IsZip       bool     // if true then download zip exist
	LogFileName string   // log file name
	LogModTime  int64    // log file modification time in nanseconds since epoch
	Lines       []string // file content
}

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
// 3. if dbcopy done OK then rename log file into model......ready.download.log else into model......error.download.log
func makeDownload(baseName string, cmd *exec.Cmd, cmdMsg string, logPath string) {

	// delete existing (previous copy) of download data
	basePath := filepath.Join(theCfg.downloadDir, baseName)

	if !removeDownloadFile(basePath+".ready.download.log", logPath, "delete: "+baseName+".ready.download.log") {
		return
	}
	if !removeDownloadFile(basePath+".error.download.log", logPath, "delete: "+baseName+".error.download.log") {
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
		omppLog.Log("Error at: ", cmd.Args)
		appendToDownloadLog(logPath, true, e.Error())
		renameToDownloadErrorLog(logPath, "Error at: "+cmdMsg)
		return
	}
	// else: completed OK
	if !isLogOk {
		omppLog.Log("Warning: dbcopy log output may incomplete")
	}

	// all done, rename log file on success: model......progress.download.log into model......ready.download.log
	os.Rename(logPath, strings.TrimSuffix(logPath, ".progress.download.log")+".ready.download.log")
}

// remove download file and append log message, on error do rename log file into model......error.download.log and return false
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

// remove download directory and append log message, on error do rename log file into model......error.download.log and return false
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

// rename log file on error: model......progress.download.log into model......error.download.log
func renameToDownloadErrorLog(logPath string, errMsg string) {
	if errMsg != "" {
		appendToDownloadLog(logPath, true, errMsg)
	}
	os.Rename(logPath, strings.TrimSuffix(logPath, ".progress.download.log")+".error.download.log")
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

// update file status of download files:
// check if zip exist, if folder exist and set log file modification time in nanoseconds since epoch
func updateStatDownloadLog(logPath string, dl *DownloadStatusLog) {

	// check if download zip or download folder exist
	if dl.Folder != "" {
		dl.IsFolder = isDirExist(filepath.Join(theCfg.downloadDir, dl.Folder)) == nil
		dl.ZipFileName = dl.Folder + ".zip"
		dl.IsZip = dl.Status == "ready" && isFileExist(filepath.Join(theCfg.downloadDir, dl.ZipFileName)) == nil
	}

	// retrive log file modification time
	fi, err := os.Stat(filepath.Join(theCfg.downloadDir, logPath))
	if err == nil {
		dl.LogModTime = fi.ModTime().Sub(time.Unix(0, 0)).Nanoseconds()
	}
}

// parse log file content to get folder name, log file kind and keys
// kind and keys are:
//   model:   model digest
//   run:     model digest, run digest
//   workset: model digest, workset name
func parseDownloadLog(fileName, fileContent string) DownloadStatusLog {

	dl := DownloadStatusLog{LogFileName: fileName}

	// set download status by .download.log file extension
	if dl.Status == "" && strings.HasSuffix(fileName, ".ready.download.log") {
		dl.Status = "ready"
	}
	if dl.Status == "" && strings.HasSuffix(fileName, ".progress.download.log") {
		dl.Status = "progress"
	}
	if dl.Status == "" && strings.HasSuffix(fileName, ".error.download.log") {
		dl.Status = "error"
	}

	// split log lines
	dl.Lines = strings.Split(strings.ReplaceAll(fileContent, "\r", "\x20"), "\n")
	if len(dl.Lines) <= 0 {
		return dl // empty log file
	}

	// header is between -------- lines, at least 8 dashes expected
	firstHdr := 0
	endHdr := 0
	for k := 0; k < len(dl.Lines); k++ {
		if strings.HasPrefix(dl.Lines[k], "--------") {
			if firstHdr <= 0 {
				firstHdr = k + 1
			} else {
				endHdr = k
				break
			}
		}
	}
	// header must have at least two lines: model digest and folder
	if firstHdr <= 1 || endHdr < firstHdr+2 || endHdr >= len(dl.Lines) {
		return dl
	}

	// parse header lines to find keys and folder
	for _, h := range dl.Lines[firstHdr:endHdr] {

		if strings.HasPrefix(h, "Folder") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				dl.Folder = strings.TrimSpace(h[n+1:])
			}
			continue
		}
		if strings.HasPrefix(h, "Model Digest") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				dl.ModelDigest = strings.TrimSpace(h[n+1:])
			}
			continue
		}
		if strings.HasPrefix(h, "Run Digest") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				dl.RunDigest = strings.TrimSpace(h[n+1:])
			}
			continue
		}
		if strings.HasPrefix(h, "Scenario Name") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				dl.WorksetName = strings.TrimSpace(h[n+1:])
			}
			continue
		}
	}

	// check kind of the log: model, model run or workset
	if dl.Kind == "" && dl.ModelDigest != "" && dl.RunDigest != "" {
		dl.Kind = "run"
	}
	if dl.Kind == "" && dl.ModelDigest != "" && dl.WorksetName != "" {
		dl.Kind = "workset"
	}
	if dl.Kind == "" && dl.ModelDigest != "" {
		dl.Kind = "model"
	}

	return dl
}
