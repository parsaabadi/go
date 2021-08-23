// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"bufio"
	"io"
	"io/fs"
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
	Status        string   // if not empty then one of: progress ready error
	Kind          string   // if not empty then one of: model, run, workset or delete
	ModelDigest   string   // content of "Model Digest:"
	RunDigest     string   // content of "Run  Digest:"
	WorksetName   string   // content of "Scenario Name:"
	IsFolder      bool     // if true then download folder exist
	Folder        string   // content of "Folder:"
	FolderModTime int64    // folder modification time in milliseconds since epoch
	IsZip         bool     // if true then download zip exist
	ZipFileName   string   // zip file name
	ZipModTime    int64    // zip modification time in milliseconds since epoch
	ZipSize       int64    // zip file size
	LogFileName   string   // log file name
	LogModTime    int64    // log file modification time in milliseconds since epoch
	Lines         []string // file content
}

// PathItem contain basic file info after tree walk: relative path, size and modification time
type PathItem struct {
	Path    string // file path in / slash form
	IsDir   bool   // if true then it is a directory
	Size    int64  // file size (may be zero for directories)
	ModTime int64  // file modification time in milliseconds since epoch
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
		theCfg.dbcopyPath, "-m", mb.name, "-dbcopy.Zip", "-dbcopy.OutputDir", absDownDir, "-dbcopy.FromSqlite", mb.dbPath,
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
		theCfg.dbcopyPath, "-m", mb.name, "-dbcopy.RunId", strconv.Itoa(runId), "-dbcopy.Zip", "-dbcopy.OutputDir", absDownDir, "-dbcopy.FromSqlite", mb.dbPath,
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
		theCfg.dbcopyPath, "-m", mb.name, "-dbcopy.SetName", setName, "-dbcopy.Zip", "-dbcopy.OutputDir", absDownDir, "-dbcopy.FromSqlite", mb.dbPath,
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
// check if zip exist, if folder exist and retirve file modification time in milliseconds since epoch
func updateStatDownloadLog(logName string, dl *DownloadStatusLog) {

	// check if download zip or download folder exist
	if dl.Folder != "" {

		fi, e := dirStat(filepath.Join(theCfg.downloadDir, dl.Folder))
		dl.IsFolder = e == nil
		if dl.IsFolder {
			dl.FolderModTime = fi.ModTime().UnixNano() / int64(time.Millisecond)
		}

		if dl.Status == "ready" {
			fi, e = fileStat(filepath.Join(theCfg.downloadDir, dl.Folder+".zip"))
			dl.IsZip = e == nil
			if dl.IsZip {
				dl.ZipFileName = dl.Folder + ".zip"
				dl.ZipSize = fi.Size()
				dl.ZipModTime = fi.ModTime().UnixNano() / int64(time.Millisecond)
			}
		}
	}

	// retrive log file modification time
	if fi, err := os.Stat(filepath.Join(theCfg.downloadDir, logName)); err == nil {
		dl.LogModTime = fi.ModTime().UnixNano() / int64(time.Millisecond)
	}
}

// parseDownloadLogFileList for each download directory entry check is it a .download.log file and parse content
func parseDownloadLogFileList(preffix string, dirEntryLst []fs.DirEntry) []DownloadStatusLog {

	dlLst := []DownloadStatusLog{}

	for _, f := range dirEntryLst {

		// skip if this is not a .download.log file or does not have modelName. prefix
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".download.log") {
			continue
		}
		if preffix != "" && !strings.HasPrefix(f.Name(), preffix) {
			continue
		}

		// read log file, skip on error
		bt, err := os.ReadFile(filepath.Join(theCfg.downloadDir, f.Name()))
		if err != nil {
			omppLog.Log("Failed to read log file: ", f.Name())
			continue // skip log file on read error
		}
		fc := string(bt)

		// parse log file content to get folder name, log file kind and keys
		dl := parseDownloadLog(f.Name(), fc)
		updateStatDownloadLog(f.Name(), &dl)

		dlLst = append(dlLst, dl)
	}

	return dlLst
}

// parse log file content to get folder name, log file kind and keys
// kind and keys are:
//   model:   model digest
//   run:     model digest, run digest
//   workset: model digest, workset name
//   delete:  folder
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
	// header must have at least two lines: folder and model digest or delete
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
		if strings.HasPrefix(h, "Delete") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				dl.Kind = "delete"
				dl.Folder = strings.TrimSpace(h[n+1:])
			}
			continue
		}
	}

	// check kind of the log: model, model run, workset or delete
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
