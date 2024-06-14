// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// upload file to user files folder, unzip if file.zip uploaded.
//
//	POST /api/files/file/:path
//	POST /api/files/file?path=....
func filesFileUploadPostHandler(w http.ResponseWriter, r *http.Request) {

	p, src, err := getFilesPathParam(r, "path")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// check if folder exists
	saveToPath := filepath.Join(theCfg.filesDir, p)
	dir, fName := filepath.Split(saveToPath)

	ok, err := helper.IsDirExist(dir)
	if err != nil {
		omppLog.Log("Error: invalid (or empty) directory: ", dir, " : ", src, " ", err.Error())
		http.Error(w, "Invalid (or empty) file path: "+src, http.StatusBadRequest)
		return
	}
	if !ok {
		http.Error(w, "Invalid (or empty) folder path: "+src, http.StatusBadRequest)
		return
	}

	// parse multipart form: only single part expected with file.name file attached
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

	// check file name: it should be the same as url parameter
	fn := part.FileName()
	if fn != fName {
		http.Error(w, "Error: invalid (or empty) file name: "+fName+" ("+fn+")", http.StatusBadRequest)
		return
	}

	// save file into user files
	omppLog.Log("Upload of: ", fName)

	err = helper.SaveTo(saveToPath, part)
	if err != nil {
		omppLog.Log("Error: unable to write into ", saveToPath, err)
		http.Error(w, "Error: unable to write into "+fName, http.StatusInternalServerError)
		return
	}

	// unzip if source file is .zip archive
	ext := filepath.Ext(fName)
	if strings.ToLower(ext) == ".zip" {

		if err = helper.UnpackZip(saveToPath, false, strings.TrimSuffix(saveToPath, ext)); err != nil {
			omppLog.Log("Error: unable to unzip ", saveToPath, err)
			http.Error(w, "Error: unable to unzip "+src, http.StatusInternalServerError)
			return
		}
	}

	// report to the client results location
	w.Header().Set("Content-Location", "/api/files/file/"+src)
}

// return file tree (file path, size, modification time) in user files by extension, underscore _ or * extension means any.
//
//	GET /api/files/file-tree/:ext/path/:path
//	GET /api/files/file-tree/:ext/path/
//	GET /api/files/file-tree/:ext/path
//	GET /api/files/file-tree/:ext/path?path=....
func filesTreeGetHandler(w http.ResponseWriter, r *http.Request) {
	doFileTreeGet(theCfg.filesDir, true, "path", true, w, r)
}

// return file tree (file path, size, modification time) by sub-folder name.
func doFileTreeGet(rootDir string, isAllowEmptyFolder bool, pathParam string, isExt bool, w http.ResponseWriter, r *http.Request) {

	// optional url or query parameters: sub-folder path and files extension
	src := getRequestParam(r, pathParam)
	if pathParam == "" || src == "" && !isAllowEmptyFolder || src == "." || src == ".." {
		http.Error(w, "Folder name invalid (or empty): "+src, http.StatusBadRequest)
		return
	}
	folder := src

	ext := ""
	if isExt {
		ext = getRequestParam(r, "ext")
		if ext == "" || ext == "." || ext == ".." {
			http.Error(w, "Files extension invalid (or empty): "+ext, http.StatusBadRequest)
			return
		}
		if ext == "_" || ext == "*" { // _ extension means * any extension
			ext = ""
		}
		if strings.ContainsAny(ext, helper.InvalidFileNameChars) {
			http.Error(w, "Files extension invalid (or empty): "+ext, http.StatusBadRequest)
			return
		}
	}

	// if folder path specified then it must be local path, not absolute and should not contain invalid characters
	if folder != "" {

		folder = filepath.Clean(folder)
		if folder == "." || folder == ".." || !filepath.IsLocal(folder) {
			http.Error(w, "Folder name invalid (or empty): "+src, http.StatusBadRequest)
			return
		}

		// folder path should not contain invalid characters
		if strings.ContainsAny(folder, helper.InvalidFilePathChars) {
			http.Error(w, "Folder name invalid (or empty): "+src, http.StatusBadRequest)
			return
		}
	}

	// get files tree
	treeLst, err := filesWalk(rootDir, folder, ext)
	if err != nil {
		omppLog.Log("Error: ", err.Error())
		http.Error(w, "Error at folder scan: "+folder, http.StatusBadRequest)
		return
	}

	jsonResponse(w, r, treeLst)
}

// return files list under rootDir / folder, if ext is not empty then filtered by extension
func filesWalk(rootDir, folder string, ext string) ([]PathItem, error) {

	// extension specified then do lower case comparison
	lcExt := strings.ToLower(ext)
	if lcExt != "" && lcExt[0] != '.' {
		lcExt = "." + lcExt
	}

	// check if folder path exist under the root dir
	folderPath := filepath.Join(rootDir, folder)
	if !dirExist(folderPath) {
		return nil, errors.New("Folder not found: " + folder)
	}
	dp := filepath.ToSlash(rootDir)
	dps := dp + "/"

	// get list of files under the folder
	treeLst := []PathItem{}
	err := filepath.Walk(folderPath, func(path string, fi fs.FileInfo, err error) error {
		if err != nil {
			omppLog.Log("Error at directory walk: ", path, " : ", err.Error())
			return err
		}
		p := filepath.ToSlash(path)
		if p == dp || p == dps {
			p = "/"
		} else {
			p = strings.TrimPrefix(p, dps)
		}
		if lcExt != "" && lcExt != strings.ToLower(filepath.Ext(p)) {
			return nil
		}
		treeLst = append(treeLst, PathItem{
			Path:    p,
			IsDir:   fi.IsDir(),
			Size:    fi.Size(),
			ModTime: fi.ModTime().UnixMilli(),
		})
		return nil
	})
	return treeLst, err
}

// create folder under user files directory.
//
//	PUT /api/files/folder/:path
func filesFolderCreatePutHandler(w http.ResponseWriter, r *http.Request) {

	_, folder, err := getFilesPathParam(r, "path")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// create folder(s) path under user files root
	folderPath := filepath.Join(theCfg.filesDir, folder)

	if err := os.MkdirAll(folderPath, 0750); err != nil {
		omppLog.Log("Error at creating folder: ", folderPath, " ", err.Error())
		http.Error(w, "Error at creating folder: "+folder, http.StatusInsufficientStorage)
		return
	}

	// report to the client results location
	w.Header().Set("Content-Location", "/api/files/folder/"+folder)
}

// delete file or folder from user files directory.
//
//	DELETE /api/files/delete/:path
func filesDeleteHandler(w http.ResponseWriter, r *http.Request) {

	_, folder, err := getFilesPathParam(r, "path")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// check: path to be deleted should not be download or upload foleder
	folderPath := filepath.Join(theCfg.filesDir, folder)

	if isReservedFilesPath(folderPath) {
		omppLog.Log("Error: unable to delete: ", folderPath)
		http.Error(w, "Error: unable to delete: "+folder, http.StatusBadRequest)
		return
	}
	// remove folder(s)
	if err := os.RemoveAll(folderPath); err != nil {
		omppLog.Log("Error at deleting folder: ", folderPath, " ", err.Error())
		http.Error(w, "Error at deleting folder: "+folder, http.StatusInsufficientStorage)
		return
	}

	// report to the client deleted location
	w.Header().Set("Content-Location", "/api/files/folder/"+folder)
}

// delete all user files and folders, keep reserved folders and it content: download and upload.
//
//	DELETE /api/files/delete-all
func filesAllDeleteHandler(w http.ResponseWriter, r *http.Request) {

	// get list of files under user files root
	pLst, err := filepath.Glob(theCfg.filesDir + "/*")
	if err != nil {
		omppLog.Log("Error at user files directory scan: ", theCfg.filesDir+"/*", " ", err.Error())
		http.Error(w, "Error at user files directory scan", http.StatusBadRequest)
		return
	}

	// delete files and remove sub-folders except of protected sub-folders: download, upload
	for k := 0; k < len(pLst); k++ {

		if isReservedFilesPath(pLst[k]) {
			continue
		}
		if err = os.RemoveAll(pLst[k]); err != nil {
			omppLog.Log("Error: unable to delete: ", pLst[k])
			http.Error(w, "Error: unable to delete: "+pLst[k], http.StatusBadRequest)
			return
		}
	}
}

// return true if path is one of "reserved" paths and cannot be deleted: . .. download upload home, etc.
func isReservedFilesPath(path string) bool {
	return path == "." || path == ".." ||
		path == theCfg.downloadDir || path == theCfg.uploadDir ||
		path == theCfg.inOutDir || path == theCfg.filesDir ||
		path == theCfg.homeDir || path == theCfg.rootDir ||
		path == theCfg.jobDir || path == theCfg.docDir ||
		path == theCfg.htmlDir || path == theCfg.etcDir
}

// get and validate path parameter from url or query parameter, return error if it is empty or . or .. or invalid
func getFilesPathParam(r *http.Request, name string) (string, string, error) {

	// url or query parameter folder required
	src := getRequestParam(r, name)
	if src == "" || src == "." || src == ".." {
		return "", src, errors.New("Folder name invalid (or empty): " + src)
	}

	// folder path should be local, not absolute and shoud not contain invalid characters
	folder := filepath.Clean(src)
	if folder == "." || folder == ".." || !filepath.IsLocal(folder) {
		return "", src, errors.New("Folder name invalid (or empty): " + src)
	}
	if strings.ContainsAny(folder, helper.InvalidFilePathChars) {
		return "", src, errors.New("Folder name invalid (or empty): " + src)
	}

	return folder, src, nil
}
