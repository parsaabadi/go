// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

// jsonSetHeaders set jscon response headers: Content-Type: application/json and Access-Control-Allow-Origin
func csvSetHeaders(w http.ResponseWriter, name string) {

	// set response headers: no Content-Length result in Transfer-Encoding: chunked
	// todo: ETag instead no-cache and utf-8 file names
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename="+`"`+url.QueryEscape(name)+".csv"+`"`)
	w.Header().Set("Cache-Control", "no-cache")

}

// jsonSetHeaders set jscon response headers: Content-Type: application/json and Access-Control-Allow-Origin
func jsonSetHeaders(w http.ResponseWriter, r *http.Request) {

	// if Content-Type not set then use json
	if _, isSet := w.Header()["Content-Type"]; !isSet {
		w.Header().Set("Content-Type", "application/json")
	}

	// if request from localhost then allow response to any protocol or port
	/*
		if strings.HasPrefix(r.Host, "localhost") {
			if _, isSet := w.Header()["Access-Control-Allow-Origin"]; !isSet {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			}
		}
	*/
}

// jsonResponse set json response headers and writes src as json into w response writer.
// On error it writes 500 internal server error response.
func jsonResponse(w http.ResponseWriter, r *http.Request, src interface{}) {

	jsonSetHeaders(w, r)

	err := json.NewEncoder(w).Encode(src)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// jsonResponseBytes set json response headers and writes src bytes into w response writer.
// If src bytes empty then "{}" is written.
func jsonResponseBytes(w http.ResponseWriter, r *http.Request, src []byte) {

	jsonSetHeaders(w, r)

	if len(src) <= 0 {
		w.Write([]byte("{}"))
	}
	w.Write(src)
}

// jsonCellWriter writes each row of data page into response as list of comma separated json values.
// It is an append to response and response headers must already be set.
func jsonCellWriter(w http.ResponseWriter, enc *json.Encoder, cvtCell func(interface{}) (interface{}, error)) func(src interface{}) (bool, error) {

	isNext := false

	// write data page into response as list of comma separated json values
	cvtWr := func(src interface{}) (bool, error) {

		if isNext {
			w.Write([]byte{','}) // until the last separate array items with , comma
		}

		val := src // id's cell
		var err error

		// convert cell from id's to code if converter specified
		if cvtCell != nil {
			if val, err = cvtCell(src); err != nil {
				return false, err
			}
		}

		// write actual value
		if err := enc.Encode(val); err != nil {
			return false, err
		}
		isNext = true
		return true, nil
	}
	return cvtWr
}

// jsonRequestDecode validate Content-Type: application/json and decode json body.
// Destination for json decode: dst must be a pointer.
// If isRequired is true then json body is required else it can be empty by default.
// On error it writes error response 400 or 415 and return false.
func jsonRequestDecode(w http.ResponseWriter, r *http.Request, isRequired bool, dst interface{}) bool {

	// json body expected
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "Expected Content-Type: application/json", http.StatusUnsupportedMediaType)
		return false
	}

	// decode json
	err := json.NewDecoder(r.Body).Decode(dst)
	if err != nil {
		if err == io.EOF {
			if !isRequired {
				return true // empty default values
			} else {
				http.Error(w, "Invalid (empty) json at "+r.URL.String(), http.StatusBadRequest)
				return false
			}
		}
		omppLog.Log("Json decode error at " + r.URL.String() + ": " + err.Error())
		http.Error(w, "Json decode error at "+r.URL.String(), http.StatusBadRequest)
		return false
	}
	return true // completed OK
}

// jsonMultipartDecode decode json part of multipart form reader.
// It does move to next part, check part form name and decode json content from part body.
// Destination for json decode: dst must be a pointer.
// On error it writes error response 400 or 415 and return false.
func jsonMultipartDecode(w http.ResponseWriter, mr *multipart.Reader, name string, dst interface{}) bool {

	// open next part and check part name
	part, err := mr.NextPart()
	if err == io.EOF {
		http.Error(w, "Invalid (empty) next part of multipart form "+name, http.StatusBadRequest)
		return false
	}
	if err != nil {
		http.Error(w, "Failed to get next part of multipart form "+name+" : "+err.Error(), http.StatusBadRequest)
		return false
	}
	defer part.Close()

	if part.FormName() != name {
		http.Error(w, "Invalid part of multipart form, expected name: "+name, http.StatusBadRequest)
		return false
	}

	// decode json
	err = json.NewDecoder(part).Decode(dst)
	if err != nil {
		if err == io.EOF {
			http.Error(w, "Invalid (empty) json part of multipart form "+name, http.StatusBadRequest)
			return false
		}
		http.Error(w, "Json decode error at part of multipart form "+name, http.StatusBadRequest)
		return false
	}
	return true // completed OK
}

// jsonRequestToFile validate Content-Type: application/json and copy request body into output file as is.
// If output file already exists then it is truncated.
// On error it writes error response 400 or 500 and return false.
func jsonRequestToFile(w http.ResponseWriter, r *http.Request, outPath string) bool {

	// json body expected
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "Expected Content-Type: application/json", http.StatusUnsupportedMediaType)
		return false
	}

	// create or truncate output file
	fName := filepath.Base(outPath)

	err := helper.SaveTo(outPath, r.Body)
	if err != nil {
		omppLog.Log("Error: unable to write into ", outPath, err)
		http.Error(w, "Error: unable to write into "+fName, http.StatusInternalServerError)
		return false
	}
	return true // completed OK
}

// isDirExist return error if directory does not exist or not accessible
func isDirExist(dirPath string) error {
	_, err := dirStat(dirPath)
	return err
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

// isFileExist return error if file, or not accessible or it is not a regular file
func isFileExist(filePath string) error {
	_, err := fileStat(filePath)
	return err
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

// dbcopyPath return path to dbcopy.exe, it is expected to be in the same directory as oms.exe.
// argument omsAbsPath expected to be /absolute/path/to/oms.exe
func dbcopyPath(omsAbsPath string) string {

	d := filepath.Dir(omsAbsPath)
	p := filepath.Join(d, "dbcopy.exe")
	if e := isFileExist(p); e == nil {
		return p
	}
	p = filepath.Join(d, "dbcopy")
	if e := isFileExist(p); e == nil {
		return p
	}
	return "" // dbcopy not found or not accessible or not regular file
}
