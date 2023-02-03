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

// modelDownloadPostHandler initate creation of model zip archive in home/io/download folder.
// POST /api/download/model/:model
// Zip archive is the same as created by dbcopy command line utilty.
// Dimension(s) and enum-based parameters returned as enum codes, not enum id's.
// Json is posted to specify download options.
// If NoAccumulatorsCsv is true then accumulators CSV files are not included in result.
// It is significantly faster to porduce the result archive, we but cannot import it back into the model database,
// it is only to analyze model output values CSV data using some other tools
// If NoMicrodata is true then microdata not included in result.
// If Utf8BomIntoCsv is true then add utf-8 byte order mark into csv files
func modelDownloadPostHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model") // model digest-or-name

	// decode json download options
	opts := struct {
		NoAccumulatorsCsv bool
		NoMicrodata       bool
		Utf8BomIntoCsv    bool
	}{}
	if !jsonRequestDecode(w, r, false, &opts) {
		return // error at json decode, response done with http error
	}
	if !theCfg.isMicrodata {
		opts.NoMicrodata = true // microdata output disabled
	}

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
	if fileExist(logPath) {
		omppLog.Log("Error: download already in progress: ", logPath)
		http.Error(w, "Model download already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new download.progress.log file and write model decsription
	logPath, isLog := createUpDownLog(logPath)
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
	if !appendToUpDownLog(logPath, true, "Download of: "+baseName) {
		renameToDownloadErrorLog(logPath, "", nil)
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model download failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToUpDownLog(logPath, false, hdrMsg...) {
		renameToDownloadErrorLog(logPath, "", nil)
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model download failed: "+baseName, http.StatusBadRequest)
		return
	}

	// create model download files on separate thread
	cmd, cmdMsg := makeModelDownloadCommand(mb, logPath, opts.NoAccumulatorsCsv, opts.NoMicrodata, opts.Utf8BomIntoCsv)

	go makeDownload(baseName, cmd, cmdMsg, logPath)

	// report to the client results location
	w.Header().Set("Content-Location", "/api/download/model/"+dn+"/"+baseName)
}

// runDownloadPostHandler initate creation of model run zip archive in home/io/download folder.
// POST /api/download/model/:model/run/:run
// Zip archive is the same as created by dbcopy command line utilty.
// Dimension(s) and enum-based parameters returned as enum codes, not enum id's.
// Json is posted to specify download options.
// If NoAccumulatorsCsv is true then accumulators CSV files are not included in result.
// It is significantly faster to porduce the result archive, we but cannot import it back into the model database,
// it is only to analyze model output values CSV data using some other tools
// If NoMicrodata is true then microdata not included in result.
// If Utf8BomIntoCsv is true then add utf-8 byte order mark into csv files
func runDownloadPostHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model") // model digest-or-name
	rdsn := getRequestParam(r, "run") // run digest-or-stamp-or-name

	// decode json download options
	opts := struct {
		NoAccumulatorsCsv bool
		NoMicrodata       bool
		Utf8BomIntoCsv    bool
	}{}
	if !jsonRequestDecode(w, r, false, &opts) {
		return // error at json decode, response done with http error
	}
	if !theCfg.isMicrodata {
		opts.NoMicrodata = true // microdata output disabled
	}

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
	baseName := mb.name + ".run." + helper.CleanPath(r0.Name)
	omppLog.Log("Download of: ", baseName)

	// if download.progress.log file exist the retun error: download in progress
	logPath := filepath.Join(theCfg.downloadDir, baseName+".progress.download.log")
	if fileExist(logPath) {
		omppLog.Log("Error: download already in progress: ", logPath)
		http.Error(w, "Model run download already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new download.progress.log file and write model run decsription
	logPath, isLog := createUpDownLog(logPath)
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
	if !appendToUpDownLog(logPath, true, "Download of: "+baseName) {
		renameToDownloadErrorLog(logPath, "", nil)
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model run download failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToUpDownLog(logPath, false, hdrMsg...) {
		renameToDownloadErrorLog(logPath, "", nil)
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model run download failed: "+baseName, http.StatusBadRequest)
		return
	}

	// create model run download files on separate thread
	cmd, cmdMsg := makeRunDownloadCommand(mb, r0.RunId, logPath, opts.NoAccumulatorsCsv, opts.NoMicrodata, opts.Utf8BomIntoCsv)

	go makeDownload(baseName, cmd, cmdMsg, logPath)

	// report to the client results location
	w.Header().Set("Content-Location", "/api/download/model/"+dn+"/run/"+rdsn+"/"+baseName)
}

// worksetDownloadPostHandler initate creation of model workset zip archive in home/io/download folder.
// POST /api/download/model/:model/workset/:set
// Zip archive is the same as created by dbcopy command line utilty.
// Dimension(s) and enum-based parameters returned as enum codes, not enum id's.
// Json is posted to specify download options.
// If Utf8BomIntoCsv is true then add utf-8 byte order mark into csv files
func worksetDownloadPostHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model") // model digest-or-name
	wsn := getRequestParam(r, "set")  // workset name

	// decode json download options
	opts := struct{ Utf8BomIntoCsv bool }{}

	if !jsonRequestDecode(w, r, false, &opts) {
		return // error at json decode, response done with http error
	}

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
	if fileExist(logPath) {
		omppLog.Log("Error: download already in progress: ", logPath)
		http.Error(w, "Model scenario download already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new download.progress.log file and write model scenario decsription
	logPath, isLog := createUpDownLog(logPath)
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
	if !appendToUpDownLog(logPath, true, "Download of: "+baseName) {
		renameToDownloadErrorLog(logPath, "", nil)
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model scenario download failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToUpDownLog(logPath, false, hdrMsg...) {
		renameToDownloadErrorLog(logPath, "", nil)
		omppLog.Log("Failed to write into download log file: " + baseName + ".progress.download.log")
		http.Error(w, "Model scenario download failed: "+baseName, http.StatusBadRequest)
		return
	}

	// create model scenario download files on separate thread
	cmd, cmdMsg := makeWorksetDownloadCommand(mb, wst.Name, logPath, opts.Utf8BomIntoCsv)

	go makeDownload(baseName, cmd, cmdMsg, logPath)

	// report to the client results location
	w.Header().Set("Content-Location", "/api/download/model/"+dn+"/workset/"+wsn+"/"+baseName)
}
