// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/json"
	"io"
	"net/http"

	"go.openmpp.org/ompp/db"
)

// worksetReadonlyHandler update workset read-only status from json post:
// POST /api/workset-readonly
// Json keys: model digest or name and workset name.
// Json content: workset "public" metadata.
// Only read-only workset falg is used from workset "public" metadata.
// If multiple models with same name exist then result is undefined.
// If no such workset exist in database then result is undefined.
func worksetReadonlyHandler(w http.ResponseWriter, r *http.Request) {

	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Expected Content-Type: application/json", http.StatusUnsupportedMediaType)
		return
	}

	// decode json
	var wp db.WorksetPub
	err := json.NewDecoder(r.Body).Decode(&wp)
	if err != nil {
		if err == io.EOF {
			http.Error(w, "Invalid (empty) json at "+r.URL.String(), http.StatusBadRequest)
			return
		}
		http.Error(w, "Json decode error at "+r.URL.String(), http.StatusBadRequest)
		return
	}

	// update workset read-only status
	dn := wp.ModelDigest
	if dn == "" {
		dn = wp.ModelName
	}
	digest, ws, ok := theCatalog.UpdateWorksetReadonly(dn, wp.Name, wp.IsReadonly)
	if ok {
		w.Header().Set("Location", "/api/model/"+digest+"/workset/"+ws.Name)
	}
}

// worksetReadonlyUrlHandler update workset read-only status by model digest-or-name and workset name:
// POST /api/model/:dn/workset/:wsn/readonly/:val
// If multiple models with same name exist then result is undefined.
// If no such workset exist in database then empty result returned.
func worksetReadonlyUrlHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	wsn := getRequestParam(r, "wsn")

	// convert readonly flag
	isReadonly, ok := getBoolRequestParam(r, "val")
	if !ok {
		http.Error(w, "Invalid value of workset read-only flag "+wsn, http.StatusBadRequest)
		return
	}

	// update workset read-only status
	digest, ws, ok := theCatalog.UpdateWorksetReadonly(dn, wsn, isReadonly)
	if ok {
		w.Header().Set("Location", "/api/model/"+digest+"/workset/"+ws.Name)
	}
}

// worksetUpdateHandler update workset metadata from json post:
// POST /api/workset-meta
// Json keys: model digest or name and workset name.
// Json content: workset "public" metadata.
// If multiple models with same name exist then result is undefined.
// If no such workset exist in database then new workset created.
// If workset already exists and new list of parameters smaller than existing
// then parameters removed form database, including parameter values
func worksetUpdateHandler(w http.ResponseWriter, r *http.Request) {

	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Expected Content-Type: application/json", http.StatusUnsupportedMediaType)
		return
	}

	// decode json
	var wp db.WorksetPub
	err := json.NewDecoder(r.Body).Decode(&wp)
	if err != nil {
		if err == io.EOF {
			http.Error(w, "Invalid (empty) json at "+r.URL.String(), http.StatusBadRequest)
			return
		}
		http.Error(w, "Json decode error at "+r.URL.String(), http.StatusBadRequest)
		return
	}

	// update workset metadata
	ok, err := theCatalog.UpdateWorkset(&wp)
	if err != nil {
		http.Error(w, "Workset update failed "+wp.Name, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Location", "/api/model/"+wp.ModelDigest+"/workset/"+wp.Name)
	}
}
