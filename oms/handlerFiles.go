// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"cmp"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
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

// return file tree (file path, size, modification time) in user files by ext which is comma separted list of extensions, underscore _ or * extension means any.
//
//	GET /api/files/file-tree/:ext/path/:path
//	GET /api/files/file-tree/:ext/path/
//	GET /api/files/file-tree/:ext/path
//	GET /api/files/file-tree/:ext/path?path=....
func filesTreeGetHandler(w http.ResponseWriter, r *http.Request) {
	doFileTreeGet(theCfg.filesDir, true, "path", true, w, r)
}

// return file tree (file path, size, modification time) by sub-folder name.
//
//	GET /api/download/file-tree/:folder
//	GET /api/upload/file-tree/:folder
//	GET /api/files/file-tree/:ext/path/
//	GET /api/files/file-tree/:ext/path/:path
//
// pathParam is a name of path parameter: "path" or "folder"
// if isAllowEmptyFolder is true then value of path parameter can be empty
// if isExt is true then request should have "ext" extnsion filter parameter
func doFileTreeGet(rootDir string, isAllowEmptyPath bool, pathParam string, isExt bool, w http.ResponseWriter, r *http.Request) {

	// optional url or query parameters: sub-folder path and files extension
	src := getRequestParam(r, pathParam)
	if pathParam == "" || src == "" && !isAllowEmptyPath || src == "." || src == ".." {
		http.Error(w, "Folder name invalid (or empty): "+src, http.StatusBadRequest)
		return
	}
	folder := src

	extCsv := ""
	if isExt {
		extCsv = getRequestParam(r, "ext")
		if extCsv == "" || extCsv == "." || extCsv == ".." {
			http.Error(w, "Files extension invalid (or empty): "+extCsv, http.StatusBadRequest)
			return
		}
		if extCsv == "_" || extCsv == "*" { // _ extension means * any extension
			extCsv = ""
		}
		if strings.ContainsAny(extCsv, helper.InvalidFileNameChars) {
			http.Error(w, "Files extension invalid (or empty): "+extCsv, http.StatusBadRequest)
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
	treeLst, err := filesWalk(rootDir, folder, extCsv, true)
	if err != nil {
		omppLog.Log("Error: ", err.Error())
		http.Error(w, "Error at folder scan: "+folder, http.StatusBadRequest)
		return
	}

	jsonResponse(w, r, treeLst)
}

// return files list or file tree under rootDir/folder directory.
// rootDir top removed from the path results.
// if extCsv is not empty then filtered by extensions in comma separated list.
// if isTree is true then return files tree else files path list.
func filesWalk(rootDir, folder string, extCsv string, isTree bool) ([]PathItem, error) {

	// parse comma separated list of extensions, if it is empty "" string then add all files, do not filter by extension
	eLst := []string{}
	isAll := extCsv == ""

	if !isAll {
		eLst = helper.ParseCsvLine(strings.ToLower(extCsv), ',')

		j := 0
		for _, elc := range eLst {
			if elc == "" {
				continue
			}
			if elc[0] != '.' {
				elc = "." + elc
			}
			eLst[j] = elc
			j++
		}
		eLst = eLst[:j]
	}

	// check if folder path exist under the root dir
	folderPath := filepath.Join(rootDir, folder)
	if !dirExist(folderPath) {
		return nil, errors.New("Folder not found: " + folder)
	}
	rDir := filepath.ToSlash(rootDir)
	rsDir := rDir + "/"

	// get list of files under the folder
	treeLst := []PathItem{}
	err := filepath.Walk(folderPath, func(path string, fi fs.FileInfo, err error) error {
		if err != nil {
			omppLog.Log("Error at directory walk: ", path, " : ", err.Error())
			return err
		}
		p := filepath.ToSlash(path)
		if p == rDir || p == rsDir {
			p = "/"
		} else {
			p = strings.TrimPrefix(p, rsDir)
		}
		elc := strings.ToLower(filepath.Ext(p))

		// if no all files then check if extension is in the list of filter extensions
		isAdd := isAll

		for k := 0; !isAdd && k < len(eLst); k++ {
			isAdd = eLst[k] == elc
		}
		if isAdd {
			treeLst = append(treeLst, PathItem{
				Path:    p,
				IsDir:   fi.IsDir(),
				Size:    fi.Size(),
				ModTime: fi.ModTime().UnixMilli(),
			})
		}
		return nil
	})

	// if required then build files tree from files path list by adding directories into the path list
	if isTree {

		pm := map[string]bool{}
		addLst := []PathItem{}

		for k := 0; k < len(treeLst); k++ {

			d := treeLst[k].Path
			pm[d] = true // mark source path as already processed

			for { // until all directories above that path are processed

				d = path.Dir(d)

				if d == "" || d == "." || d == ".." || d == "/" || d == rDir {
					break // done with that directory and all directories above
				}
				if _, ok := pm[d]; ok {
					continue // directory already processed
				}
				pm[d] = true

				// get directory stat, ignoring error can potentially lead to incorrect tree
				if fi, e := dirStat(filepath.Join(rootDir, filepath.FromSlash(d))); e == nil {
					addLst = append(addLst, PathItem{
						Path:    d,
						IsDir:   fi.IsDir(),
						Size:    fi.Size(),
						ModTime: fi.ModTime().UnixMilli(),
					})
				}
			}
		}

		// merge additional directories into files tree, sort file tree to put files after directories
		treeLst = append(treeLst, addLst...)

		slices.SortStableFunc(treeLst, func(left, right PathItem) int {
			if left.IsDir && !right.IsDir {
				return -1
			}
			if !left.IsDir && right.IsDir {
				return 1
			}
			return cmp.Compare(strings.ToLower(left.Path), strings.ToLower(right.Path))
		})
	}
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
