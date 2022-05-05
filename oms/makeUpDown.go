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

// UpDownStatusLog contains download, upload or delete  status info and content of log file
type UpDownStatusLog struct {
	Status        string   // if not empty then one of: progress ready error
	Kind          string   // if not empty then one of: model, run, workset, delete, upload
	ModelDigest   string   // content of "Model Digest:"
	RunDigest     string   // content of "Run  Digest:"
	WorksetName   string   // content of "Scenario Name:"
	IsFolder      bool     // if true then download (or upload) folder exist
	Folder        string   // content of "Folder:"
	FolderModTime int64    // folder modification time in milliseconds since epoch
	IsZip         bool     // if true then download (or upload) zip exist
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
func makeModelDownloadCommand(mb modelBasic, logPath string, isNoAcc bool, isCsvBom bool) (*exec.Cmd, string) {

	// make dbcopy message for user log
	cmdMsg := "dbcopy -m " + mb.name + " -dbcopy.Zip -dbcopy.OutputDir " + theCfg.downloadDir
	if isNoAcc {
		cmdMsg += " -dbcopy.NoAccumulatorsCsv"
	}
	if isCsvBom {
		cmdMsg += " -dbcopy.Utf8BomIntoCsv"
	}

	// make absolute path to download directory: dbcopy work directory is a model bin directory
	absDownDir, err := filepath.Abs(theCfg.downloadDir)
	if err != nil {
		renameToDownloadErrorLog(logPath, "Error at starting "+cmdMsg)
		return nil, cmdMsg
	}

	// make dbcopy command
	cArgs := []string{"-m", mb.name, "-dbcopy.Zip", "-dbcopy.OutputDir", absDownDir, "-dbcopy.FromSqlite", mb.dbPath}
	if isNoAcc {
		cArgs = append(cArgs, "-dbcopy.NoAccumulatorsCsv")
	}
	if isCsvBom {
		cArgs = append(cArgs, "-dbcopy.Utf8BomIntoCsv")
	}

	cmd := exec.Command(theCfg.dbcopyPath, cArgs...)
	cmd.Dir = mb.binDir // dbcopy work directory is a model bin directory

	return cmd, cmdMsg
}

// make dbcopy command to prepare model run download
func makeRunDownloadCommand(mb modelBasic, runId int, logPath string, isNoAcc bool, isCsvBom bool) (*exec.Cmd, string) {

	// make dbcopy message for user log
	cmdMsg := "dbcopy -m " + mb.name +
		" -dbcopy.IdOutputNames=false" +
		" -dbcopy.RunId " + strconv.Itoa(runId) +
		" -dbcopy.Zip" +
		" -dbcopy.OutputDir " + theCfg.downloadDir
	if isNoAcc {
		cmdMsg += " -dbcopy.NoAccumulatorsCsv"
	}
	if isCsvBom {
		cmdMsg += " -dbcopy.Utf8BomIntoCsv"
	}

	// make absolute path to download directory: dbcopy work directory is a model bin directory
	absDownDir, err := filepath.Abs(theCfg.downloadDir)
	if err != nil {
		renameToDownloadErrorLog(logPath, "Error at starting "+cmdMsg)
		return nil, cmdMsg
	}

	// make dbcopy command
	cArgs := []string{
		"-m", mb.name,
		"-dbcopy.IdOutputNames=false",
		"-dbcopy.RunId", strconv.Itoa(runId),
		"-dbcopy.Zip",
		"-dbcopy.OutputDir", absDownDir,
		"-dbcopy.FromSqlite", mb.dbPath,
	}
	if isNoAcc {
		cArgs = append(cArgs, "-dbcopy.NoAccumulatorsCsv")
	}
	if isCsvBom {
		cArgs = append(cArgs, "-dbcopy.Utf8BomIntoCsv")
	}

	cmd := exec.Command(theCfg.dbcopyPath, cArgs...)
	cmd.Dir = mb.binDir // dbcopy work directory is a model bin directory

	return cmd, cmdMsg
}

// make dbcopy command to prepare model workset download
func makeWorksetDownloadCommand(mb modelBasic, setName string, logPath string, isCsvBom bool) (*exec.Cmd, string) {

	// make dbcopy message for user log
	cmdMsg := "dbcopy -m " + mb.name +
		" -dbcopy.IdOutputNames=false" +
		" -dbcopy.SetName " + setName +
		" -dbcopy.Zip" +
		" -dbcopy.OutputDir " + theCfg.downloadDir
	if isCsvBom {
		cmdMsg += " -dbcopy.Utf8BomIntoCsv "
	}

	// make absolute path to download directory: dbcopy work directory is a model bin directory
	absDownDir, err := filepath.Abs(theCfg.downloadDir)
	if err != nil {
		renameToDownloadErrorLog(logPath, "Error at starting "+cmdMsg)
		return nil, cmdMsg
	}

	// make dbcopy command
	cArgs := []string{
		"-m", mb.name,
		"-dbcopy.IdOutputNames=false",
		"-dbcopy.SetName", setName,
		"-dbcopy.Zip",
		"-dbcopy.OutputDir", absDownDir,
		"-dbcopy.FromSqlite", mb.dbPath,
	}
	if isCsvBom {
		cArgs = append(cArgs, "-dbcopy.Utf8BomIntoCsv")
	}

	cmd := exec.Command(theCfg.dbcopyPath, cArgs...)
	cmd.Dir = mb.binDir // dbcopy work directory is a model bin directory

	return cmd, cmdMsg
}

// make dbcopy command to prepare model run import into database after upload
func makeRunUploadCommand(mb modelBasic, runName string, logPath string) (*exec.Cmd, string) {

	// make dbcopy message for user log
	cmdMsg := "dbcopy -m " + mb.name +
		" -dbcopy.IdOutputNames=false" +
		" -dbcopy.RunName " + runName +
		" -dbcopy.To db" +
		" -dbcopy.Zip" +
		" -dbcopy.InputDir " + theCfg.uploadDir

	// make absolute path to upload directory: dbcopy work directory is a model bin directory
	absUpDir, err := filepath.Abs(theCfg.uploadDir)
	if err != nil {
		renameToUploadErrorLog(logPath, "Error at starting "+cmdMsg)
		return nil, cmdMsg
	}

	// make dbcopy command
	cArgs := []string{
		"-m", mb.name,
		"-dbcopy.IdOutputNames=false",
		"-dbcopy.RunName", runName,
		"-dbcopy.To", "db",
		"-dbcopy.Zip",
		"-dbcopy.InputDir", absUpDir,
		"-dbcopy.ToSqlite", mb.dbPath,
	}

	cmd := exec.Command(theCfg.dbcopyPath, cArgs...)
	cmd.Dir = mb.binDir // dbcopy work directory is a model bin directory

	return cmd, cmdMsg
}

// make dbcopy command to prepare model workset import into database after upload
func makeWorksetUploadCommand(mb modelBasic, setName string, logPath string, isNoDigestCheck bool) (*exec.Cmd, string) {

	// make dbcopy message for user log
	cmdMsg := "dbcopy -m " + mb.name +
		" -dbcopy.IdOutputNames=false" +
		" -dbcopy.SetName " + setName +
		" -dbcopy.To db" +
		" -dbcopy.Zip" +
		" -dbcopy.InputDir " + theCfg.uploadDir
	if isNoDigestCheck {
		cmdMsg += " -dbcopy.NoDigestCheck"
	}

	// make absolute path to upload directory: dbcopy work directory is a model bin directory
	absUpDir, err := filepath.Abs(theCfg.uploadDir)
	if err != nil {
		renameToUploadErrorLog(logPath, "Error at starting "+cmdMsg)
		return nil, cmdMsg
	}

	// make dbcopy command
	cArgs := []string{
		"-m", mb.name,
		"-dbcopy.IdOutputNames=false",
		"-dbcopy.SetName", setName,
		"-dbcopy.To", "db",
		"-dbcopy.Zip",
		"-dbcopy.InputDir", absUpDir,
		"-dbcopy.ToSqlite", mb.dbPath,
	}
	if isNoDigestCheck {
		cArgs = append(cArgs, "-dbcopy.NoDigestCheck")
	}

	cmd := exec.Command(theCfg.dbcopyPath, cArgs...)
	cmd.Dir = mb.binDir // dbcopy work directory is a model bin directory

	return cmd, cmdMsg
}

// makeDownload invoke dbcopy to create model download directory and .zip file:
// 1. delete existing: previous download log file, model.xyz.zip, model.xyz directory.
// 2. start dbcopy to export model data into pack it into .zip file.
// 3. if dbcopy done OK then rename log file into model......ready.download.log else into model......error.download.log
func makeDownload(baseName string, cmd *exec.Cmd, cmdMsg string, logPath string) {
	runUpDownDbcopy("download", theCfg.downloadDir, baseName, cmd, cmdMsg, logPath)
}

// makeUpload invoke dbcopy to create model upload directory and .zip file:
// 1. delete existing: previous upload log file and model.xyz directory.
// 2. start dbcopy to unzip uploaded file and import into it model database.
// 3. if dbcopy done OK then rename log file into model......ready.upload.log else into model......error.upload.log
func makeUpload(baseName string, cmd *exec.Cmd, cmdMsg string, logPath string) {
	runUpDownDbcopy("upload", theCfg.uploadDir, baseName, cmd, cmdMsg, logPath)
}

// runUpDownDbcopy invoke dbcopy to export from dbd into download .zip or import from uploaded .zip into model database.
// 1. delete existing: previous log file and model.xyz directory.
// 2. if download then delete existing model.xyz.zip
// 3. if download: start dbcopy to export model data into .zip file.
// 3. if upload: stsrt dbopy to unzip uploaded file and import into it model database.
// 4. if dbcopy done OK then rename log file into model......ready.up-or-down.log else into model......error.up-or-down.log
func runUpDownDbcopy(upDown string, upDownDir string, baseName string, cmd *exec.Cmd, cmdMsg string, logPath string) {

	// delete existing (previous copy) of download or upload data
	basePath := filepath.Join(upDownDir, baseName)

	if !removeUpDownFile(upDown, basePath+".ready."+upDown+".log", logPath, baseName+".ready."+upDown+".log") {
		return
	}
	if !removeUpDownFile(upDown, basePath+".error."+upDown+".log", logPath, baseName+".error."+upDown+".log") {
		return
	}
	if upDown == "download" && !removeUpDownFile(upDown, basePath+".zip", logPath, baseName+".zip") {
		return
	}
	if !removeUpDownDir(upDown, basePath, logPath, baseName) {
		return
	}
	appendToUpDownLog(logPath, true, cmdMsg)

	// connect console output to output log file
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		renameToUpDownErrorLog(upDown, logPath, "Error at starting "+cmdMsg)
		return
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		renameToUpDownErrorLog(upDown, logPath, "Error at starting "+cmdMsg)
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
				isLogOk = appendToUpDownLog(path, false, sc.Text())
			}
		}
		done <- true
		close(done)
	}

	// start console output listners
	absLogPath, err := filepath.Abs(logPath)
	if err != nil {
		renameToUpDownErrorLog(upDown, logPath, "Error at starting "+cmdMsg)
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
		renameToUpDownErrorLog(upDown, logPath, err.Error())
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
		appendToUpDownLog(logPath, true, e.Error())
		renameToUpDownErrorLog(upDown, logPath, "Error at: "+cmdMsg)
		return
	}
	// else: completed OK
	if !isLogOk {
		omppLog.Log("Warning: dbcopy log output may incomplete")
	}

	// all done, rename log file on success: model......progress.up-or-down.log into model......ready.up-or-down.log
	os.Rename(logPath, strings.TrimSuffix(logPath, ".progress."+upDown+".log")+".ready."+upDown+".log")
}

// remove download file and append log message
// on error do rename log file into model......error.download.log and return false
func removeDownloadFile(path string, logPath string, fileName string) bool {
	return removeUpDownFile("download", path, logPath, fileName)
}

// remove upload file and append log message
// on error do rename log file into model......error.upload.log and return false
func removeUploadFile(path string, logPath string, fileName string) bool {
	return removeUpDownFile("upload", path, logPath, fileName)
}

// remove download or upload file and append log message
// on error do rename log file into model......error.up-or-down.log and return false
func removeUpDownFile(upDown string, path string, logPath string, fileName string) bool {

	if !appendToUpDownLog(logPath, true, "delete: "+fileName) {
		renameToUpDownErrorLog(upDown, logPath, "")
		return false
	}
	if e := os.Remove(path); e != nil && !os.IsNotExist(e) {
		omppLog.Log(e)
		renameToUpDownErrorLog(upDown, logPath, "Error at delete "+fileName)
		return false
	}
	return true
}

// remove download directory and append log message
// on error do rename log file into model......error.download.log and return false
func removeDownloadDir(path string, logPath string, dirName string) bool {
	return removeUpDownDir("download", path, logPath, dirName)
}

// remove upload directory and append log message
// on error do rename log file into model......error.upload.log and return false
func removeUploadDir(path string, logPath string, dirName string) bool {
	return removeUpDownDir("upload", path, logPath, dirName)
}

// remove upload or download directory and append log message
// on error do rename log file into model......error.up-or-down.log and return false
func removeUpDownDir(upDown string, path string, logPath string, dirName string) bool {

	if !appendToUpDownLog(logPath, true, "delete: "+dirName) {
		renameToUpDownErrorLog(upDown, logPath, "")
		return false
	}
	if e := os.RemoveAll(path); e != nil && !os.IsNotExist(e) {
		omppLog.Log(e)
		renameToUpDownErrorLog(upDown, logPath, "Error at delete "+dirName)
		return false
	}
	return true
}

// create new upload or download log file or truncate existing
func createUpDownLog(logPath string) (string, bool) {

	f, err := os.Create(logPath)
	if err != nil {
		return "", false
	}
	defer f.Close()
	return logPath, true
}

// rename upload or download log file on error: model......progress.download.log into model......error.download.log
func renameToDownloadErrorLog(logPath string, errMsg string) {
	renameToUpDownErrorLog("download", logPath, errMsg)
}

// rename upload log file on error: model......progress.upload.log into model......error.upload.log
func renameToUploadErrorLog(logPath string, errMsg string) {
	renameToUpDownErrorLog("upload", logPath, errMsg)
}

// rename upload or download log file on error: model......progress.up-or-down.log into model......error.up-or-down.log
func renameToUpDownErrorLog(upDown string, logPath string, errMsg string) {
	if errMsg != "" {
		appendToUpDownLog(logPath, true, errMsg)
	}
	os.Rename(logPath, strings.TrimSuffix(logPath, ".progress."+upDown+".log")+".error."+upDown+".log")
}

// append to message into upload or download log file
func appendToUpDownLog(logPath string, isDoTimestamp bool, msg ...string) bool {

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
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

// update file status of download files
func updateStatDownloadLog(logName string, uds *UpDownStatusLog) {
	updateStatUpDownLog(logName, uds, theCfg.downloadDir)
}

// update file status of upload files
func updateStatUploadLog(logName string, uds *UpDownStatusLog) {
	updateStatUpDownLog(logName, uds, theCfg.uploadDir)
}

// update file status of download or upload files:
// check if zip exist, if folder exist and retirve file modification time in milliseconds since epoch
func updateStatUpDownLog(logName string, uds *UpDownStatusLog, upDownDir string) {

	// check if download zip or download folder exist
	if uds.Folder != "" {

		fi, e := dirStat(filepath.Join(upDownDir, uds.Folder))
		uds.IsFolder = e == nil
		if uds.IsFolder {
			uds.FolderModTime = fi.ModTime().UnixNano() / int64(time.Millisecond)
		}

		if uds.Status == "ready" {
			fi, e = fileStat(filepath.Join(upDownDir, uds.Folder+".zip"))
			uds.IsZip = e == nil
			if uds.IsZip {
				uds.ZipFileName = uds.Folder + ".zip"
				uds.ZipSize = fi.Size()
				uds.ZipModTime = fi.ModTime().UnixNano() / int64(time.Millisecond)
			}
		}
	}

	// retrive log file modification time
	if fi, err := os.Stat(filepath.Join(upDownDir, logName)); err == nil {
		uds.LogModTime = fi.ModTime().UnixNano() / int64(time.Millisecond)
	}
}

// parseDownloadLogFileList for each download directory entry check is it a .download.log file and parse content
func parseDownloadLogFileList(preffix string, dirEntryLst []fs.DirEntry) []UpDownStatusLog {
	return parseUpDownLogFileList("download", preffix, dirEntryLst, theCfg.downloadDir)
}

// parseUploadLogFileList for each upload directory entry check is it a .upload.log file and parse content
func parseUploadLogFileList(preffix string, dirEntryLst []fs.DirEntry) []UpDownStatusLog {
	return parseUpDownLogFileList("upload", preffix, dirEntryLst, theCfg.uploadDir)
}

// parseUpDownLogFileList for each download or upload directory entry check is it a .up-or-down.log file and parse content
func parseUpDownLogFileList(upDown string, preffix string, dirEntryLst []fs.DirEntry, upDownDir string) []UpDownStatusLog {

	udsLst := []UpDownStatusLog{}

	for _, f := range dirEntryLst {

		// skip if this is not a .up-or-down.log file or does not have modelName. prefix
		if f.IsDir() || !strings.HasSuffix(f.Name(), "."+upDown+".log") {
			continue
		}
		if preffix != "" && !strings.HasPrefix(f.Name(), preffix) {
			continue
		}

		// read log file, skip on error
		bt, err := os.ReadFile(filepath.Join(upDownDir, f.Name()))
		if err != nil {
			omppLog.Log("Failed to read log file: ", f.Name())
			continue // skip log file on read error
		}
		fc := string(bt)

		// parse log file content to get folder name, log file kind and keys
		uds := parseUpDownLog(upDown, f.Name(), fc)
		updateStatUpDownLog(f.Name(), &uds, upDownDir)

		udsLst = append(udsLst, uds)
	}

	return udsLst
}

// parse log file content to get folder name, log file kind and keys
// kind and keys are:
//   model:   model digest
//   run:     model digest, run digest
//   workset: model digest, workset name
//   delete:  folder
//   upload:  zip file name
func parseDownloadLog(fileName, fileContent string) UpDownStatusLog {
	return parseUpDownLog("download", fileName, fileContent)
}

// parse log file content to get folder name, log file kind and keys
// kind and keys are:
//   model:   model digest
//   run:     model digest, run digest
//   workset: model digest, workset name
//   delete:  folder
//   upload:  zip file name
func parseUpDownLog(upDown, fileName, fileContent string) UpDownStatusLog {

	uds := UpDownStatusLog{LogFileName: fileName}

	// set status by .up-or-down.log file extension
	if uds.Status == "" && strings.HasSuffix(fileName, ".ready."+upDown+".log") {
		uds.Status = "ready"
	}
	if uds.Status == "" && strings.HasSuffix(fileName, ".progress."+upDown+".log") {
		uds.Status = "progress"
	}
	if uds.Status == "" && strings.HasSuffix(fileName, ".error."+upDown+".log") {
		uds.Status = "error"
	}

	// split log lines
	uds.Lines = strings.Split(strings.ReplaceAll(fileContent, "\r", "\x20"), "\n")
	if len(uds.Lines) <= 0 {
		return uds // empty log file
	}

	// header is between -------- lines, at least 8 dashes expected
	firstHdr := 0
	endHdr := 0
	for k := 0; k < len(uds.Lines); k++ {
		if strings.HasPrefix(uds.Lines[k], "--------") {
			if firstHdr <= 0 {
				firstHdr = k + 1
			} else {
				endHdr = k
				break
			}
		}
	}
	// header must have at least two lines: folder and model digest or delete
	if firstHdr <= 1 || endHdr < firstHdr+2 || endHdr >= len(uds.Lines) {
		return uds
	}

	// parse header lines to find keys and folder
	for _, h := range uds.Lines[firstHdr:endHdr] {

		if strings.HasPrefix(h, "Folder") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				uds.Folder = strings.TrimSpace(h[n+1:])
			}
			continue
		}
		if strings.HasPrefix(h, "Model Digest") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				uds.ModelDigest = strings.TrimSpace(h[n+1:])
			}
			continue
		}
		if strings.HasPrefix(h, "Run Digest") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				uds.RunDigest = strings.TrimSpace(h[n+1:])
			}
			continue
		}
		if strings.HasPrefix(h, "Scenario Name") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				uds.WorksetName = strings.TrimSpace(h[n+1:])
			}
			continue
		}
		if strings.HasPrefix(h, "Delete") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				uds.Kind = "delete"
				uds.Folder = strings.TrimSpace(h[n+1:])
			}
			continue
		}
		if strings.HasPrefix(h, "Upload") {
			if n := strings.IndexByte(h, ':'); n > 1 && n+1 < len(h) {
				uds.Kind = "upload"
				uds.ZipFileName = strings.TrimSpace(h[n+1:])
			}
			continue
		}
	}

	// check kind of the log: model, model run, workset or delete
	if uds.Kind == "" && uds.ModelDigest != "" && uds.RunDigest != "" {
		uds.Kind = "run"
	}
	if uds.Kind == "" && uds.ModelDigest != "" && uds.WorksetName != "" {
		uds.Kind = "workset"
	}
	if uds.Kind == "" && uds.ModelDigest != "" {
		uds.Kind = "model"
	}

	return uds
}
