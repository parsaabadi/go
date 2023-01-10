// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strings"

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// runUploadPostHandler post of model run zip archive in home/io/upload folder.
// POST /api/upload/model/:model/run
// POST /api/upload/model/:model/run/:run
// Zip archive is the same as created by dbcopy command line utilty.
// Dimension(s) and enum-based parameters returned as enum codes, not enum id's.
func runUploadPostHandler(w http.ResponseWriter, r *http.Request) {
	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	rName := getRequestParam(r, "run") // run name

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

	// parse multipart form: only single part expected with run.zip file attached
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Error at multipart form open ", http.StatusBadRequest)
		return
	}

	// open next part
	part, err := mr.NextPart()
	if err == io.EOF {
		http.Error(w, "Invalid (empty) next part of multipart form", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, "Failed to get next part of multipart form: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer part.Close()

	// check file name: it should be modelName.run.RunName.zip
	// if run name not specified in URL the get it from file name
	fName := part.FileName()
	ext := path.Ext(fName)
	baseName := strings.TrimSuffix(fName, ext)
	mpn := m.Name + ".run."
	runName := strings.TrimPrefix(baseName, mpn)

	if baseName == "" || baseName == "." || baseName == ".." ||
		runName == "" || runName == "." || runName == ".." ||
		fName != helper.CleanPath(fName) {
		http.Error(w, "Error: invalid (or empty) file name: "+fName, http.StatusBadRequest)
		return
	}
	if ext != ".zip" || !strings.HasPrefix(baseName, mpn) {
		http.Error(w, "Error: file name must be: "+mpn+"Name.zip", http.StatusBadRequest)
		return
	}
	if rName != "" && runName != rName {
		http.Error(w, "Error: invalid file name, expected: "+mpn+rName+".zip", http.StatusBadRequest)
		return
	}

	// if upload.progress.log file exist the retun error: upload in progress
	omppLog.Log("Upload of: ", fName)

	logPath := filepath.Join(theCfg.uploadDir, baseName+".progress.upload.log")
	if fileExist(logPath) {
		omppLog.Log("Error: upload already in progress: ", logPath)
		http.Error(w, "Model run upload already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new upload.progress.log file and write model run decsription
	logPath, isLog := createUpDownLog(logPath)
	if !isLog {
		omppLog.Log("Failed to create upload log file: " + baseName + ".progress.upload.log")
		http.Error(w, "Model run upload failed: "+baseName, http.StatusBadRequest)
		return
	}
	hdrMsg := []string{
		"------------------",
		"Upload           : " + fName,
		"Model Name       : " + m.Name,
		"Model Version    : " + m.Version + " " + m.CreateDateTime,
		"Model Digest     : " + m.Digest,
		"Run Name         : " + runName,
		"Folder           : " + baseName,
		"------------------",
	}
	if !appendToUpDownLog(logPath, true, "Upload of: "+baseName) {
		renameToUploadErrorLog(logPath, "")
		omppLog.Log("Failed to write into upload log file: " + baseName + ".progress.upload.log")
		http.Error(w, "Model run upload failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToUpDownLog(logPath, false, hdrMsg...) {
		renameToUploadErrorLog(logPath, "")
		omppLog.Log("Failed to write into upload log file: " + baseName + ".progress.upload.log")
		http.Error(w, "Model run upload failed: "+baseName, http.StatusBadRequest)
		return
	}

	// save run.zip into upload directory
	saveToPath := filepath.Join(theCfg.uploadDir, fName)

	helper.SaveTo(saveToPath, part)
	if err != nil {
		omppLog.Log("Error: unable to write into ", saveToPath, err)
		http.Error(w, "Error: unable to write into "+fName, http.StatusInternalServerError)
		return
	}

	// create model run upload files on separate thread
	cmd, cmdMsg := makeRunUploadCommand(mb, runName, logPath)

	go makeUpload(baseName, cmd, cmdMsg, logPath)

	// report to the client results location
	w.Header().Set("Content-Location", "/api/upload/model/"+dn+"/run/"+runName+"/"+baseName)
}

// worksetUploadPostHandler post of model workset zip archive in home/io/upload folder.
// POST /api/upload/model/:model/workset
// POST /api/upload/model/:model/workset/:set
// Zip archive is the same as created by dbcopy command line utilty.
// Dimension(s) and enum-based parameters returned as enum codes, not enum id's.
// Posted multi-part form can have optional "workset-upload-options" part with json upload options
// Upload option NoDigestCheck=true do suppress model digest verification:
// model digest in source zip is ignored, only model name is used and that allows to upload worksets into different model version.
func worksetUploadPostHandler(w http.ResponseWriter, r *http.Request) {
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

	// parse multipart form: only single part expected with set.zip file attached
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Error at multipart form open ", http.StatusBadRequest)
		return
	}

	// open first part: it can be workset-upload-options part or workset.zip file
	part, err := mr.NextPart()
	if err == io.EOF {
		http.Error(w, "Invalid (empty) next part of multipart form", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, "Failed to get next part of multipart form: "+err.Error(), http.StatusBadRequest)
		return
	}

	// if this is workset-upload-options then decode json
	isNoDigestCheck := false
	if part.FormName() == "workset-upload-options" {

		opts := struct{ NoDigestCheck bool }{}

		err = json.NewDecoder(part).Decode(&opts)
		if err != nil && err != io.EOF {
			http.Error(w, "Json decode error at 'workset-upload-options' part of multipart form", http.StatusBadRequest)
			part.Close()
			return
		}
		isNoDigestCheck = opts.NoDigestCheck

		// open next part: workset.zip file
		part.Close()

		part, err = mr.NextPart()
		if err == io.EOF {
			http.Error(w, "Invalid (empty) next part of multipart form", http.StatusBadRequest)
			return
		}
		if err != nil {
			http.Error(w, "Failed to get next part of multipart form: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	defer part.Close()

	// check file name: it should be modelName.set.WorksetName.zip
	// if workset name not specified in URL the get it from file name
	fName := part.FileName()
	ext := path.Ext(fName)
	baseName := strings.TrimSuffix(fName, ext)
	mpn := m.Name + ".set."
	setName := strings.TrimPrefix(baseName, mpn)

	if baseName == "" || baseName == "." || baseName == ".." ||
		setName == "" || setName == "." || setName == ".." ||
		fName != helper.CleanPath(fName) {
		http.Error(w, "Error: invalid (or empty) file name: "+fName, http.StatusBadRequest)
		return
	}
	if ext != ".zip" || !strings.HasPrefix(baseName, mpn) {
		http.Error(w, "Error: file name must be: "+mpn+"Name.zip", http.StatusBadRequest)
		return
	}
	if wsn != "" && setName != wsn {
		http.Error(w, "Error: invalid file name, expected: "+mpn+wsn+".zip", http.StatusBadRequest)
		return
	}

	// if upload.progress.log file exist the retun error: upload in progress
	omppLog.Log("Upload of: ", fName)

	logPath := filepath.Join(theCfg.uploadDir, baseName+".progress.upload.log")
	if fileExist(logPath) {
		omppLog.Log("Error: upload already in progress: ", logPath)
		http.Error(w, "Model scenario upload already in progress: "+baseName, http.StatusBadRequest)
		return
	}

	// create new upload.progress.log file and write model scenario decsription
	logPath, isLog := createUpDownLog(logPath)
	if !isLog {
		omppLog.Log("Failed to create upload log file: " + baseName + ".progress.upload.log")
		http.Error(w, "Model scenario upload failed: "+baseName, http.StatusBadRequest)
		return
	}
	hdrMsg := []string{
		"------------------",
		"Upload           : " + fName,
		"Model Name       : " + m.Name,
		"Model Version    : " + m.Version + " " + m.CreateDateTime,
		"Model Digest     : " + m.Digest,
		"Scenario Name    : " + setName,
		"Folder           : " + baseName,
		"------------------",
	}
	if !appendToUpDownLog(logPath, true, "Upload of: "+baseName) {
		renameToUploadErrorLog(logPath, "")
		omppLog.Log("Failed to write into upload log file: " + baseName + ".progress.upload.log")
		http.Error(w, "Model scenario upload failed: "+baseName, http.StatusBadRequest)
		return
	}
	if !appendToUpDownLog(logPath, false, hdrMsg...) {
		renameToUploadErrorLog(logPath, "")
		omppLog.Log("Failed to write into upload log file: " + baseName + ".progress.upload.log")
		http.Error(w, "Model scenario upload failed: "+baseName, http.StatusBadRequest)
		return
	}

	// save set.zip into upload directory
	saveToPath := filepath.Join(theCfg.uploadDir, fName)

	helper.SaveTo(saveToPath, part)
	if err != nil {
		omppLog.Log("Error: unable to write into ", saveToPath, err)
		http.Error(w, "Error: unable to write into "+fName, http.StatusInternalServerError)
		return
	}

	// create model scenario upload files on separate thread
	cmd, cmdMsg := makeWorksetUploadCommand(mb, setName, logPath, isNoDigestCheck)

	go makeUpload(baseName, cmd, cmdMsg, logPath)

	// report to the client results location
	w.Header().Set("Content-Location", "/api/upload/model/"+dn+"/workset/"+setName+"/"+baseName)
}
