// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// fileLogDownloadGetHandler return .download.log file by name and download status.
// GET /api/download/log/file/:name
// Download status is one of: progress ready error or "" if unknown
func fileLogDownloadGetHandler(w http.ResponseWriter, r *http.Request) {
	fileLogUpDownGet("download", theCfg.downloadDir, w, r)
}

// fileLogUploadGetHandler return .upload.log file by name and upload status.
// GET /api/upload/log/file/:name
// Upload status is one of: progress ready error or "" if unknown
func fileLogUploadGetHandler(w http.ResponseWriter, r *http.Request) {
	fileLogUpDownGet("upload", theCfg.uploadDir, w, r)
}

// fileLogUpDownGet return .up-or-down.log file by name and status.
// Status is one of: progress ready error or "" if unknown
func fileLogUpDownGet(upDown string, upDownDir string, w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	fileName := getRequestParam(r, "name")

	// validate name: it must be a file name only, it cannot be any directory
	if fileName == "" || !strings.HasSuffix(fileName, "."+upDown+".log") || fileName != filepath.Base(helper.CleanPath(fileName)) {
		http.Error(w, "Log file name invalid (or empty): "+fileName, http.StatusBadRequest)
		return
	}

	// read file content
	filePath := filepath.Join(upDownDir, fileName)

	if fileExist(filePath) != nil {
		http.Error(w, "Log file not found: "+fileName, http.StatusBadRequest)
		return
	}

	bt, err := os.ReadFile(filePath)
	if err != nil {
		http.Error(w, "Failed to read log file: "+fileName, http.StatusBadRequest)
		return
	}
	fc := string(bt)

	// parse log file content to get folder name, log file kind and keys
	uds := parseUpDownLog(upDown, fileName, fc)
	updateStatUpDownLog(fileName, &uds, upDownDir)

	jsonResponse(w, r, uds) // return log file content and status
}

// allDownloadLogGetHandler return all .download.log files and download status.
// GET /api/download/log/all
// Download status is one of: progress ready error or "" if unknown
func allLogDownloadGetHandler(w http.ResponseWriter, r *http.Request) {
	allLogUpDownGet("download", theCfg.downloadDir, w, r)
}

// allUploadLogGetHandler return all .upload.log files and download status.
// GET /api/upload/log/all
// Upload status is one of: progress ready error or "" if unknown
func allLogUploadGetHandler(w http.ResponseWriter, r *http.Request) {
	allLogUpDownGet("upload", theCfg.uploadDir, w, r)
}

// allDownloadLogGetHandler return all .up-or-down.log files and status.
// Status is one of: progress ready error or "" if unknown
func allLogUpDownGet(upDown string, upDownDir string, w http.ResponseWriter, r *http.Request) {

	// find all .up-or-down.log files
	fLst, err := os.ReadDir(upDownDir)
	if err != nil {
		http.Error(w, "Error at reading log directory", http.StatusBadRequest)
		return
	}

	// parse all files
	udsLst := parseUpDownLogFileList(upDown, "", fLst, upDownDir)

	jsonResponse(w, r, udsLst)
}

// modelDownloadLogGetHandler return model .download.log files with download status.
// GET /api/download/log/model/:model
// Download status is one of: progress ready error or "" if unknown
func modelLogDownloadGetHandler(w http.ResponseWriter, r *http.Request) {
	modelLogUpDownGet("download", theCfg.downloadDir, w, r)
}

// modelUploadLogGetHandler return model .upload.log files with upload status.
// GET /api/upload/log/model/:model
// Upload status is one of: progress ready error or "" if unknown
func modelLogUploadGetHandler(w http.ResponseWriter, r *http.Request) {
	modelLogUpDownGet("upload", theCfg.uploadDir, w, r)
}

// modelUpDownLogGet return model .up-or-down.log files and status.
// Status is one of: progress ready error or "" if unknown
func modelLogUpDownGet(upDown string, upDownDir string, w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model") // model digest-or-name
	mName := dn

	// if this model digest then try to find model name in catalog
	mb, ok := theCatalog.modelBasicByDigestOrName(dn)
	if ok {
		mName = mb.name
	}

	// find all .up-or-down.log files
	fLst, err := os.ReadDir(upDownDir)
	if err != nil {
		http.Error(w, "Error at reading log directory", http.StatusBadRequest)
		return
	}

	// parse all files
	udsLst := parseUpDownLogFileList(upDown, mName+".", fLst, upDownDir)

	jsonResponse(w, r, udsLst)
}

// fileTreeDownloadGetHandler return file tree (file path, size, modification time) by folder name.
// GET /api/download/file-tree/:folder
func fileTreeDownloadGetHandler(w http.ResponseWriter, r *http.Request) {
	fileTreeUpDownGet(theCfg.downloadDir, w, r)
}

// fileTreeUploadGetHandler return file tree (file path, size, modification time) by folder name.
// GET /api/upload/file-tree/:folder
func fileTreeUploadGetHandler(w http.ResponseWriter, r *http.Request) {
	fileTreeUpDownGet(theCfg.uploadDir, w, r)
}

// fileTreeUpDownGet return file tree (file path, size, modification time) by folder name.
func fileTreeUpDownGet(upDownDir string, w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	folder := getRequestParam(r, "folder")
	if folder == "" || folder != filepath.Base(helper.CleanPath(folder)) {
		http.Error(w, "Folder name invalid (or empty): "+folder, http.StatusBadRequest)
		return
	}

	folderPath := filepath.Join(upDownDir, folder)
	if dirExist(folderPath) != nil {
		http.Error(w, "Folder not found: "+folder, http.StatusBadRequest)
		return
	}
	dp := filepath.ToSlash(upDownDir) + "/"

	// get list of files under up-or-down/folder
	treeLst := []PathItem{}
	err := filepath.WalkDir(folderPath, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			omppLog.Log("Error at directory walk: ", path, " : ", err.Error())
			return err
		}
		fi, e := de.Info()
		if e != nil {
			return nil // ignore directory entry where file removed after readdir()
		}
		p := strings.TrimPrefix(filepath.ToSlash(path), dp)
		treeLst = append(treeLst, PathItem{
			Path:    filepath.ToSlash(p),
			IsDir:   de.IsDir(),
			Size:    fi.Size(),
			ModTime: fi.ModTime().UnixNano() / int64(time.Millisecond),
		})
		return nil
	})
	if err != nil {
		omppLog.Log("Error at directory walk: ", err.Error())
		http.Error(w, "Error at folder scan: "+folder, http.StatusBadRequest)
		return
	}

	jsonResponse(w, r, treeLst)
}

// downloadDeleteHandler delete download files by folder name.
// DELETE /api/download/delete/:folder
// Delete folder, .zip file and .download.log files
func downloadDeleteHandler(w http.ResponseWriter, r *http.Request) {
	upDownDelete("download", theCfg.downloadDir, false, w, r)

}

// downloadAsyncDeleteHandler starts deleting of download files by folder name.
// DELETE /api/download/start/delete/:folder
// Delete started on separate thread and does delete of folder, .zip file and .download.log files
func downloadAsyncDeleteHandler(w http.ResponseWriter, r *http.Request) {
	upDownDelete("download", theCfg.downloadDir, true, w, r)
}

// uploadDeleteHandler delete upload files by folder name.
// DELETE /api/upload/delete/:folder
// Delete folder, .zip file and .upload.log files
func uploadDeleteHandler(w http.ResponseWriter, r *http.Request) {
	upDownDelete("upload", theCfg.uploadDir, false, w, r)

}

// uploadAsyncDeleteHandler starts deleting of upload files by folder name.
// DELETE /api/upload/start/delete/:folder
// Delete started on separate thread and does delete of folder, .zip file and .upload.log files
func uploadAsyncDeleteHandler(w http.ResponseWriter, r *http.Request) {
	upDownDelete("upload", theCfg.uploadDir, true, w, r)
}

// upDownDelete delete download or upload files by folder name.
// Delete folder, .zip file and .up-or-down.log files
func upDownDelete(upDown string, upDownDir string, isAsync bool, w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	folder := getRequestParam(r, "folder")

	// delete files
	err := doDeleteUpDown(upDown, upDownDir, isAsync, folder)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// report to the client results location
	w.Header().Set("Content-Location", "/api/"+upDown+"/delete/"+folder)
}

// delete download or upload files by folder name.
// if isAsync is true then start delete on separate thread
func doDeleteUpDown(upDown string, upDownDir string, isAsync bool, folder string) error {

	if folder == "" || folder != filepath.Base(helper.CleanPath(folder)) {
		return errors.New("Folder name invalid (or empty): " + folder)
	}
	omppLog.Log("Delete: ", upDown, " ", folder)

	// create new up-or-down.progress.log file and write delete header
	logPath := filepath.Join(upDownDir, folder+".progress."+upDown+".log")

	logPath, isLog := createUpDownLog(logPath)
	if !isLog {
		omppLog.Log("Failed to create log file: " + folder + ".progress." + upDown + ".log")
		return errors.New("Delete failed: " + folder)
	}
	hdrMsg := []string{
		"---------------",
		"Delete        : " + folder,
		"Folder        : " + folder,
		"---------------",
	}
	if !appendToUpDownLog(logPath, true, "Start deleting: "+folder) {
		renameToUpDownErrorLog(upDown, logPath, "")
		omppLog.Log("Failed to write into log file: " + folder + ".progress." + upDown + ".log")
		return errors.New("Delete failed: " + folder)
	}
	if !appendToUpDownLog(logPath, false, hdrMsg...) {
		renameToUpDownErrorLog(upDown, logPath, "")
		omppLog.Log("Failed to write into log file: " + folder + ".progress." + upDown + ".log")
		return errors.New("Delete failed: " + folder)
	}

	// delete files on separate thread
	doDelete := func(baseName, logPath string) {

		// remove files
		basePath := filepath.Join(upDownDir, baseName)

		if !removeUpDownFile(upDown, basePath+".ready."+upDown+".log", logPath, baseName+".ready."+upDown+".log") {
			return
		}
		if !removeUpDownFile(upDown, basePath+".error."+upDown+".log", logPath, baseName+".error."+upDown+".log") {
			return
		}
		if !removeUpDownFile(upDown, basePath+".zip", logPath, baseName+".zip") {
			return
		}
		if !removeUpDownDir(upDown, basePath, logPath, baseName) {
			return
		}
		// last step: remove delete progress log file
		if e := os.Remove(basePath + ".progress." + upDown + ".log"); e != nil && !os.IsNotExist(e) {
			omppLog.Log(e)
		}
	}

	if isAsync {
		go doDelete(folder, logPath)
	} else {
		doDelete(folder, logPath)
	}
	return nil
}
