// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/husobee/vestigo"
	"golang.org/x/text/language"

	"go.openmpp.org/ompp/omppLog"
)

// jsonResponse set response headers and writes src as json into w response writer.
// On return error it writes 500 internal server error response.
func jsonResponse(w http.ResponseWriter, r *http.Request, src interface{}) {

	// if Content-Type not set then use json
	if _, isSet := w.Header()["Content-Type"]; !isSet {
		w.Header().Set("Content-Type", "application/json")
	}

	// if request from localhost then allow response to any protocol or port
	if strings.HasPrefix(r.Host, "localhost") {
		if _, isSet := w.Header()["Access-Control-Allow-Origin"]; !isSet {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
	}

	// write json
	err := json.NewEncoder(w).Encode(src)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// set Content-Type header by extension and invoke next handler.
// This function exist to supress Windows registry content type overrides
func setContentType(next http.Handler) http.Handler {

	var ctDef = map[string]string{
		".css": "text/css; charset=utf-8",
		".js":  "application/javascript",
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// if Content-Type not set and it is one of "forced" extensions
		// then set content type
		if _, isSet := w.Header()["Content-Type"]; !isSet {
			if ext := filepath.Ext(r.URL.Path); ext != "" {
				if ct := ctDef[strings.ToLower(ext)]; ct != "" {
					w.Header().Set("Content-Type", ct)
				}
			}
		}

		next.ServeHTTP(w, r) // invoke next handler
	})
}

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

// get url parameter ?name or router parameter /:name
func getRequestParam(r *http.Request, name string) string {

	v := r.URL.Query().Get(name)
	if v == "" {
		v = vestigo.Param(r, name)
	}
	return v
}

// get languages accepted by browser and
// append optional language argument on top of the list
func getRequestLang(r *http.Request) []language.Tag {

	// browser languages
	rqLangTags, _, _ := language.ParseAcceptLanguage(r.Header.Get("Accept-Language"))

	// if optional url parameter ?lang or router parameter /:lang specified
	ln := r.URL.Query().Get("lang")
	if ln == "" {
		ln = vestigo.Param(r, "lang")
	}

	// add lang parameter as top language
	if ln != "" {
		if t := language.Make(ln); t != language.Und {
			rqLangTags = append([]language.Tag{t}, rqLangTags...)
		}
	}
	return rqLangTags
}

// match request language with UI supported languages and return canonic language name
func matchRequestToUiLang(r *http.Request) string {
	rqLangTags, _, _ := language.ParseAcceptLanguage(r.Header.Get("Accept-Language"))
	tag, _, _ := uiLangMatcher.Match(rqLangTags...)
	return tag.String()
}
