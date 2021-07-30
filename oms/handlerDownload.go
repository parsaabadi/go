// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"
	"path/filepath"
	"strconv"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// modelDownloadPostHandler initate creation of model zip archive in home/out/download folder.
// POST /api/model/:model/download
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
	logPath := filepath.Join(theCfg.downloadDir, baseName+".download.progress.log")
	if e := isFileExist(logPath); e == nil {
		omppLog.Log("Error: download already in progress: ", logPath)
		http.Error(w, "Model download already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new download.progress.log file and write model decsription
	logPath, isLog := createDownloadLog(logPath)
	if !isLog {
		omppLog.Log("Failed to create download log file: " + baseName + ".download.progress.log")
		http.Error(w, "Model download failed: "+baseName, http.StatusBadRequest)
		return
	}
	hdrMsg := []string{
		"------",
		"Model: " + m.Name + " " + m.Version + " " + m.CreateDateTime + " " + m.Digest,
		"------",
	}
	if !appendToDownloadLog(logPath, true, "Download of: "+baseName) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".download.progress.log")
		http.Error(w, "Model download failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToDownloadLog(logPath, false, hdrMsg...) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".download.progress.log")
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
// POST /api/model/:model/download/run/:run
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
	logPath := filepath.Join(theCfg.downloadDir, baseName+".download.progress.log")
	if e := isFileExist(logPath); e == nil {
		omppLog.Log("Error: download already in progress: ", logPath)
		http.Error(w, "Model run download already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new download.progress.log file and write model run decsription
	logPath, isLog := createDownloadLog(logPath)
	if !isLog {
		omppLog.Log("Failed to create download log file: " + baseName + ".download.progress.log")
		http.Error(w, "Model run download failed: "+baseName, http.StatusBadRequest)
		return
	}
	hdrMsg := []string{
		"------",
		"Model: " + m.Name + " " + m.Version + " " + m.CreateDateTime + " " + m.Digest,
		"Run  : " + strconv.Itoa(r0.RunId) + " " + r0.CreateDateTime + " " + r0.RunDigest + " " + r0.Name,
		"------",
	}
	if !appendToDownloadLog(logPath, true, "Download of: "+baseName) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".download.progress.log")
		http.Error(w, "Model run download failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToDownloadLog(logPath, false, hdrMsg...) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".download.progress.log")
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
// POST /api/model/:model/download/workset/:set
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
	logPath := filepath.Join(theCfg.downloadDir, baseName+".download.progress.log")
	if e := isFileExist(logPath); e == nil {
		omppLog.Log("Error: download already in progress: ", logPath)
		http.Error(w, "Model scenario download already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new download.progress.log file and write model scenario decsription
	logPath, isLog := createDownloadLog(logPath)
	if !isLog {
		omppLog.Log("Failed to create download log file: " + baseName + ".download.progress.log")
		http.Error(w, "Model scenario download failed: "+baseName, http.StatusBadRequest)
		return
	}
	hdrMsg := []string{
		"----------",
		"Model:    " + m.Name + " " + m.Version + " " + m.CreateDateTime + " " + m.Digest,
		"Scenario: " + wst.Name + " " + wst.UpdateDateTime,
		"----------",
	}
	if !appendToDownloadLog(logPath, true, "Download of: "+baseName) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".download.progress.log")
		http.Error(w, "Model scenario download failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToDownloadLog(logPath, false, hdrMsg...) {
		renameToDownloadErrorLog(logPath, "")
		omppLog.Log("Failed to write into download log file: " + baseName + ".download.progress.log")
		http.Error(w, "Model scenario download failed: "+baseName, http.StatusBadRequest)
		return
	}

	// create model scenario download files on separate thread
	cmd, cmdMsg := makeWorksetDownloadCommand(baseName, mb, wst.Name, logPath)

	go makeDownload(baseName, cmd, cmdMsg, logPath)

	// report to the client results location
	w.Header().Set("Content-Location", "/api/model/"+dn+"/download/workset/"+wsn+"/"+baseName)
}
