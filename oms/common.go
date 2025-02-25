// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/husobee/vestigo"
	"golang.org/x/text/language"

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// logRequest is a middelware to log http request
func logRequest(next http.HandlerFunc) http.HandlerFunc {
	if isLogRequest {
		return func(w http.ResponseWriter, r *http.Request) {
			omppLog.Log(r.Method, ": ", r.Host, r.URL)
			next(w, r)
		}
	} // else
	return next
}

// match request language with UI supported languages and return canonic language name
func matchRequestToUiLang(r *http.Request) string {
	rqLangTags, _, _ := language.ParseAcceptLanguage(r.Header.Get("Accept-Language"))
	tag, _, _ := uiLangMatcher.Match(rqLangTags...)
	return tag.String()
}

// get value of url parameter ?name or router parameter /:name
func getRequestParam(r *http.Request, name string) string {

	v := r.URL.Query().Get(name)
	if v == "" {
		v = vestigo.Param(r, name)
	}
	return v
}

// get boolean value of url parameter ?name or router parameter /:name
func getBoolRequestParam(r *http.Request, name string) (bool, bool) {

	v := r.URL.Query().Get(name)
	if v == "" {
		v = vestigo.Param(r, name)
	}
	if v == "" {
		return false, true // no such parameter: return = false by default
	}
	if isVal, err := strconv.ParseBool(v); err == nil {
		return isVal, true // return result: value is boolean
	}
	return false, false // value is not boolean
}

// get integer value of url parameter ?name or router parameter /:name
func getIntRequestParam(r *http.Request, name string, defaultVal int) (int, bool) {

	v := r.URL.Query().Get(name)
	if v == "" {
		v = vestigo.Param(r, name)
	}
	if v == "" {
		return defaultVal, true // no such parameter: return defult value
	}
	if nVal, err := strconv.Atoi(v); err == nil {
		return nVal, true // return result: value is integer
	}
	return defaultVal, false // value is not integer
}

// get int64 value of url parameter ?name or router parameter /:name
func getInt64RequestParam(r *http.Request, name string, defaultVal int64) (int64, bool) {

	v := r.URL.Query().Get(name)
	if v == "" {
		v = vestigo.Param(r, name)
	}
	if v == "" {
		return defaultVal, true // no such parameter: return defult value
	}
	if nVal, err := strconv.ParseInt(v, 0, 64); err == nil {
		return nVal, true // return result: value is integer
	}
	return defaultVal, false // value is not integer
}

// get languages accepted by browser and by optional language request parameter, for example: ..../lang:EN
// if language parameter specified then return it as a first element of result (it a preferred language)
func getRequestLang(r *http.Request, name string) []language.Tag {

	// browser languages
	rqLangTags, _, _ := language.ParseAcceptLanguage(r.Header.Get("Accept-Language"))

	// if optional url parameter ?lang or router parameter /:lang specified
	ln := r.URL.Query().Get(name)
	if ln == "" {
		ln = vestigo.Param(r, name)
	}

	// add lang parameter as top language
	if ln != "" {
		if t := language.Make(ln); t != language.Und {
			rqLangTags = append([]language.Tag{t}, rqLangTags...)
		}
	}
	return rqLangTags
}

// set Content-Type header by extension and invoke next handler.
// This function exist to suppress Windows registry content type overrides
func setContentType(next http.Handler) http.Handler {

	var ctDef = map[string]string{
		".css": "text/css; charset=utf-8",
		".js":  "text/javascript",
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if ext := filepath.Ext(r.URL.Path); ext != "" {
			if ct := ctDef[strings.ToLower(ext)]; ct != "" {
				w.Header().Set("Content-Type", ct)
			}
		}
		next.ServeHTTP(w, r) // invoke next handler
	})
}

// set csv response headers: Content-Type: application/csv, Content-Disposition and Cache-Control
func csvSetHeaders(w http.ResponseWriter, name string) {

	// set response headers: no Content-Length result in Transfer-Encoding: chunked
	// todo: ETag instead no-cache and utf-8 file names
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename="+`"`+url.QueryEscape(name)+".csv"+`"`)
	w.Header().Set("Cache-Control", "no-cache")

}

// dirExist return error if directory does not exist or not accessible
func dirExist(dirPath string) bool {
	if dirPath == "" {
		return false
	}
	_, err := dirStat(dirPath)
	return err == nil
}

// return file Stat if this is a directory
func dirStat(dirPath string) (fs.FileInfo, error) {

	fi, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fi, errors.New("Error: directory not exist: " + dirPath)
		}
		return fi, errors.New("Error: unable to access directory: " + dirPath + " : " + err.Error())
	}
	if !fi.IsDir() {
		return fi, errors.New("Error: directory expected: " + dirPath)
	}
	return fi, nil
}

// fileExist return error if file not exist, not accessible or it is not a regular file
func fileExist(filePath string) bool {
	if filePath == "" {
		return false
	}
	_, err := fileStat(filePath)
	return err == nil
}

// return file Stat if this is a regular file
func fileStat(filePath string) (fs.FileInfo, error) {

	fi, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fi, errors.New("Error: file not exist: " + filePath)
		}
		return fi, errors.New("Error: unable to access file: " + filePath + " : " + err.Error())
	}
	if fi.IsDir() || !fi.Mode().IsRegular() {
		return fi, errors.New("Error: it is not a regilar file: " + filePath)
	}
	return fi, nil
}

// return list of files by pattern, on error log error message
func filesByPattern(ptrn string, msg string) []string {

	fLst, err := filepath.Glob(ptrn)
	if err != nil {
		omppLog.Log(msg, ": ", ptrn)
		return []string{}
	}
	return fLst
}

// Delete file and log path if isLog is true, return false on delete error.
func fileDeleteAndLog(isLog bool, path string) bool {
	if path == "" {
		return true
	}
	if isLog {
		omppLog.Log("Delete: ", path)
	}
	if e := os.Remove(path); e != nil && !os.IsNotExist(e) {
		omppLog.Log(e)
		return false
	}
	return true
}

// Move file to new location and log it if isLog is true, return false on move error.
func fileMoveAndLog(isLog bool, srcPath string, dstPath string) bool {
	if srcPath == "" || dstPath == "" {
		return false
	}
	if isLog {
		omppLog.Log("Move: ", srcPath, " To: ", dstPath)
	}
	if e := os.Rename(srcPath, dstPath); e != nil && !os.IsNotExist(e) {
		omppLog.Log(e)
		return false
	}
	return true
}

// Create or truncate existing file and log path if isLog is true, return false on create error.
func fileCreateEmpty(isLog bool, fPath string) bool {
	if isLog {
		omppLog.Log("Create: ", fPath)
	}
	f, err := os.OpenFile(fPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		omppLog.Log(err)
		return false
	}
	defer f.Close()

	return true
}

// Copy file and log path if isLog is true, return false on error of if source file not exists
func fileCopy(isLog bool, src, dst string) bool {
	if src == "" || dst == "" || src == dst {
		return false
	}
	if isLog {
		omppLog.Log("Copy: ", src, " -> ", dst)
	}

	inp, err := os.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			if isLog {
				omppLog.Log("File not found: ", src)
			}
		} else {
			omppLog.Log(err)
		}
		return false
	}
	defer inp.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		omppLog.Log(err)
		return false
	}
	defer out.Close()

	if _, err = io.Copy(out, inp); err != nil {
		omppLog.Log(err)
		return false
	}
	return true
}

// append to message to log file
func writeToCmdLog(logPath string, isDoTimestamp bool, msg ...string) bool {

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

// dbcopyPath return path to dbcopy.exe, it is expected to be in the same directory as oms.exe.
// argument omsAbsPath expected to be /absolute/path/to/oms.exe
func dbcopyPath(omsAbsPath string) string {

	d := filepath.Dir(omsAbsPath)
	p := filepath.Join(d, "dbcopy.exe")
	if fileExist(p) {
		return p
	}
	p = filepath.Join(d, "dbcopy")
	if fileExist(p) {
		return p
	}
	return "" // dbcopy not found or not accessible or it is not a regular file
}

// wait for doneC exit signal or sleep, return true on exit signal or return false at the end of sleep interval
func isExitSleep(ms time.Duration, doneC <-chan bool) bool {
	select {
	case <-doneC:
		return true
	case <-time.After(ms * time.Millisecond):
	}
	return false
}
