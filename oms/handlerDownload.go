// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// modelDownloadLogGetHandler return .download.log file by name with download status.
// GET /api/download/log/file/:name
// Download status is one of: progress ready error or "" if unknown
func fileLogDownloadGetHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	fileName := getRequestParam(r, "name")

	// validate name: it must be a file name only, it cannot be any directory
	if fileName == "" || !strings.HasSuffix(fileName, ".download.log") || fileName != filepath.Base(helper.CleanPath(fileName)) {
		http.Error(w, "Log file name invalid (or empty): "+fileName, http.StatusBadRequest)
		return
	}

	// read file content
	filePath := filepath.Join(theCfg.downloadDir, fileName)

	if isFileExist(filePath) != nil {
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
	dl := parseDownloadLog(fileName, fc)
	updateStatDownloadLog(filePath, &dl)

	jsonResponse(w, r, dl) // return log file content and status
}

// allDownloadLogGetHandler return all .download.log files with download status.
// GET /api/download/log/all
// Download status is one of: progress ready error or "" if unknown
func allLogDownloadGetHandler(w http.ResponseWriter, r *http.Request) {

	// find all .download.log files
	fLst, err := os.ReadDir(theCfg.downloadDir)
	if err != nil {
		http.Error(w, "Error at reading download directory", http.StatusBadRequest)
		return
	}

	// parse all files
	dlLst := parseDownloadLogFileList("", fLst)

	jsonResponse(w, r, dlLst)
}

// modelDownloadLogGetHandler return model .download.log files with download status.
// GET /api/download/log/model/:model
// Download status is one of: progress ready error or "" if unknown
func modelLogDownloadGetHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model") // model digest-or-name
	mName := dn

	// if this model digest then try to find model name in catalog
	mb, ok := theCatalog.modelBasicByDigestOrName(dn)
	if ok {
		mName = mb.name
	}

	// find all .download.log files
	fLst, err := os.ReadDir(theCfg.downloadDir)
	if err != nil {
		http.Error(w, "Error at reading download directory", http.StatusBadRequest)
		return
	}

	// parse all files
	dlLst := parseDownloadLogFileList(mName, fLst)

	jsonResponse(w, r, dlLst)
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

// fileTreeDownloadGetHandler return file tree (file path, size, modification time) by folder name.
// GET /api/download/file-tree/:folder
func fileTreeDownloadGetHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	folder := getRequestParam(r, "folder")
	if folder == "" || folder != filepath.Base(helper.CleanPath(folder)) {
		http.Error(w, "Folder name invalid (or empty): "+folder, http.StatusBadRequest)
		return
	}

	folderPath := filepath.Join(theCfg.downloadDir, folder)
	if isDirExist(folderPath) != nil {
		http.Error(w, "Folder not found: "+folder, http.StatusBadRequest)
		return
	}
	dp := filepath.ToSlash(theCfg.downloadDir) + "/"

	// get list of files under download/folder
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
			ModTime: fi.ModTime().UnixNano() / 1000000,
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

// modelDownloadPostHandler initate creation of model zip archive in home/out/download folder.
// POST /api/download/model/:model
// Zip archive is the same as created by dbcopy command line utilty.
// Dimension(s) and enum-based parameters returned as enum codes, not enum id's.
func modelDownloadPostHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model") // model digest-or-name

	// find model metadata by digest or name
	mb, ok := theCatalog.modelBasicByDigestOrName(dn)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}
	m, ok := theCatalog.ModelDicByDigest(mb.digest)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}

	// base part of: output directory name, .zip file name and log file name
	baseName := mb.name
	omppLog.Log("Download of: ", baseName)

	// if download.progress.log file exist the retun error: download in progress
	logPath := filepath.Join(theCfg.downloadDir, baseName+".progress.download.log")
	if e := isFileExist(logPath); e == nil {
		omppLog.Log("Error: download already in progress: ", logPath)
		http.Error(w, "Model download already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new download.progress.log file and write model decsription
	logPath, isLog := createDownloadLog(logPath)
	if !isLog {
		omppLog.Log("Failed to create download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model download failed: "+baseName, http.StatusBadRequest)
		return
	}
	hdrMsg := []string{
		"---------------",
		"Model Name    : " + m.Name,
		"Model Version : " + m.Version + " " + m.CreateDateTime,
		"Model Digest  : " + m.Digest,
		"Folder        : " + baseName,
		"---------------",
	}
	if !appendToDownloadLog(logPath, true, "Download of: "+baseName) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model download failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToDownloadLog(logPath, false, hdrMsg...) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model download failed: "+baseName, http.StatusBadRequest)
		return
	}

	// create model download files on separate thread
	cmd, cmdMsg := makeModelDownloadCommand(baseName, mb, logPath)

	go makeDownload(baseName, cmd, cmdMsg, logPath)

	// report to the client results location
	w.Header().Set("Content-Location", "/api/model/"+dn+"/download/"+baseName)
}

// runDownloadPostHandler initate creation of model run zip archive in home/out/download folder.
// POST /api/download/model/:model/run/:run
// Zip archive is the same as created by dbcopy command line utilty.
// Dimension(s) and enum-based parameters returned as enum codes, not enum id's.
func runDownloadPostHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model") // model digest-or-name
	rdsn := getRequestParam(r, "run") // run digest-or-stamp-or-name

	// find model metadata by digest or name
	mb, ok := theCatalog.modelBasicByDigestOrName(dn)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}
	m, ok := theCatalog.ModelDicByDigest(mb.digest)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}

	// find all model runs by run digest, run stamp or name, check run status: it must be success
	rst, ok := theCatalog.RunRowList(mb.digest, rdsn)
	if !ok || len(rst) <= 0 {
		http.Error(w, "Model run not found: "+mb.name+" "+dn+" "+rdsn, http.StatusBadRequest)
		return // empty result: model run not found
	}
	if len(rst) > 1 {
		omppLog.Log("Warning: multiple model runs found, using first one of: ", mb.name+" "+dn+" "+rdsn)
	}
	r0 := rst[0] // first run, if there are multiple with the same stamp or name

	if r0.Status != db.DoneRunStatus {
		http.Error(w, "Model run is not completed successfully: "+mb.name+" "+dn+" "+rdsn, http.StatusBadRequest)
		return // empty result: run status must be success
	}

	// base part of: output directory name, .zip file name and log file name
	baseName := mb.name + ".run." + strconv.Itoa(r0.RunId)
	omppLog.Log("Download of: ", baseName)

	// if download.progress.log file exist the retun error: download in progress
	logPath := filepath.Join(theCfg.downloadDir, baseName+".progress.download.log")
	if e := isFileExist(logPath); e == nil {
		omppLog.Log("Error: download already in progress: ", logPath)
		http.Error(w, "Model run download already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new download.progress.log file and write model run decsription
	logPath, isLog := createDownloadLog(logPath)
	if !isLog {
		omppLog.Log("Failed to create download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model run download failed: "+baseName, http.StatusBadRequest)
		return
	}
	hdrMsg := []string{
		"---------------",
		"Model Name    : " + m.Name,
		"Model Version : " + m.Version + " " + m.CreateDateTime,
		"Model Digest  : " + m.Digest,
		"Run Name      : " + r0.Name,
		"Run Version   : " + strconv.Itoa(r0.RunId) + " " + r0.CreateDateTime,
		"Run Digest    : " + r0.RunDigest,
		"Folder        : " + baseName,
		"---------------",
	}
	if !appendToDownloadLog(logPath, true, "Download of: "+baseName) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model run download failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToDownloadLog(logPath, false, hdrMsg...) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model run download failed: "+baseName, http.StatusBadRequest)
		return
	}

	// create model run download files on separate thread
	cmd, cmdMsg := makeRunDownloadCommand(baseName, mb, r0.RunId, logPath)

	go makeDownload(baseName, cmd, cmdMsg, logPath)

	// report to the client results location
	w.Header().Set("Content-Location", "/api/model/"+dn+"/download/run/"+rdsn+"/"+baseName)
}

// worksetDownloadPostHandler initate creation of model workset zip archive in home/out/download folder.
// POST /api/download/model/:model/workset/:set
// Zip archive is the same as created by dbcopy command line utilty.
// Dimension(s) and enum-based parameters returned as enum codes, not enum id's.
func worksetDownloadPostHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model") // model digest-or-name
	wsn := getRequestParam(r, "set")  // workset name

	// find model metadata by digest or name
	mb, ok := theCatalog.modelBasicByDigestOrName(dn)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}
	m, ok := theCatalog.ModelDicByDigest(mb.digest)
	if !ok {
		http.Error(w, "Model not found: "+dn, http.StatusBadRequest)
		return // empty result: model digest not found
	}

	// find workset by name and status: it must be read-only
	wst, ok, notFound := theCatalog.WorksetStatus(dn, wsn)
	if !ok || notFound {
		http.Error(w, "Model scenario not found: "+mb.name+" "+dn+" "+wsn, http.StatusBadRequest)
		return // empty result: workset not found
	}
	if !wst.IsReadonly {
		http.Error(w, "Model scenario must be read-only: "+mb.name+" "+dn+" "+wst.Name, http.StatusBadRequest)
		return // empty result: workset must be read-only
	}

	// base part of: output directory name, .zip file name and log file name
	baseName := mb.name + ".set." + helper.CleanPath(wst.Name)
	omppLog.Log("Download of: ", baseName)

	// if download.progress.log file exist the retun error: download in progress
	logPath := filepath.Join(theCfg.downloadDir, baseName+".progress.download.log")
	if e := isFileExist(logPath); e == nil {
		omppLog.Log("Error: download already in progress: ", logPath)
		http.Error(w, "Model scenario download already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new download.progress.log file and write model scenario decsription
	logPath, isLog := createDownloadLog(logPath)
	if !isLog {
		omppLog.Log("Failed to create download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model scenario download failed: "+baseName, http.StatusBadRequest)
		return
	}
	hdrMsg := []string{
		"------------------",
		"Model Name       : " + m.Name,
		"Model Version    : " + m.Version + " " + m.CreateDateTime,
		"Model Digest     : " + m.Digest,
		"Scenario Name    : " + wst.Name,
		"Scenario Version : " + wst.UpdateDateTime,
		"Folder           : " + baseName,
		"------------------",
	}
	if !appendToDownloadLog(logPath, true, "Download of: "+baseName) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model scenario download failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToDownloadLog(logPath, false, hdrMsg...) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model scenario download failed: "+baseName, http.StatusBadRequest)
		return
	}

	// create model scenario download files on separate thread
	cmd, cmdMsg := makeWorksetDownloadCommand(baseName, mb, wst.Name, logPath)

	go makeDownload(baseName, cmd, cmdMsg, logPath)

	// report to the client results location
	w.Header().Set("Content-Location", "/api/model/"+dn+"/download/workset/"+wsn+"/"+baseName)
}
