// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/json"
	"io"
	"net/http"

	"go.openmpp.org/ompp/db"
)

// worksetParameterIdReadHandler read a "page" workset parameter values.
// POST /api/model/:dn/workset/:wsn/parameter/value-id
// Json is posted to specify parameter name, "page" size and other read arguments,
// see db.ReadLayout for more details.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func worksetParameterIdReadHandler(w http.ResponseWriter, r *http.Request) {

	// url parameters: model digest-or-name and workset name
	dn := getRequestParam(r, "dn")
	wsn := getRequestParam(r, "wsn")

	// json body expected
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Expected Content-Type: application/json", http.StatusUnsupportedMediaType)
		return
	}

	// decode json
	var layout db.ReadLayout
	err := json.NewDecoder(r.Body).Decode(&layout)
	if err != nil {
		if err == io.EOF {
			http.Error(w, "Invalid (empty) json at "+r.URL.String(), http.StatusBadRequest)
			return
		}
		http.Error(w, "Json decode error at "+r.URL.String(), http.StatusBadRequest)
		return
	}

	if !layout.IsFromSet { // read from workset expected
		http.Error(w, "It must be read of parameter from workset "+wsn+": "+layout.Name, http.StatusBadRequest)
		return
	}

	// read parameter page
	if cLst, ok := theCatalog.ReadParameter(dn, wsn, &layout); ok {
		jsonListResponse(w, r, cLst)
	} else {
		http.Error(w, "Error at workset parameter read "+wsn+": "+layout.Name, http.StatusBadRequest)
		return
	}
}

// worksetParameterCodeReadHandler read a "page" workset parameter values.
// POST /api/model/:dn/workset/:wsn/parameter/value
// Json is posted to specify parameter name, "page" size and other read arguments,
// see db.ReadLayout for more details.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum codes.
func worksetParameterCodeReadHandler(w http.ResponseWriter, r *http.Request) {

	// url parameters: model digest-or-name and workset name
	dn := getRequestParam(r, "dn")
	wsn := getRequestParam(r, "wsn")

	// json body expected
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Expected Content-Type: application/json", http.StatusUnsupportedMediaType)
		return
	}

	// decode json
	var layout db.ReadLayout
	err := json.NewDecoder(r.Body).Decode(&layout)
	if err != nil {
		if err == io.EOF {
			http.Error(w, "Invalid (empty) json at "+r.URL.String(), http.StatusBadRequest)
			return
		}
		http.Error(w, "Json decode error at "+r.URL.String(), http.StatusBadRequest)
		return
	}

	if !layout.IsFromSet { // read from workset expected
		http.Error(w, "It must be read of parameter from workset "+wsn+": "+layout.Name, http.StatusBadRequest)
		return
	}

	// read parameter page
	if cLst, ok := theCatalog.ReadParameter(dn, wsn, &layout); ok {
		jsonListResponse(w, r, cLst)
	} else {
		http.Error(w, "Error at workset parameter read "+wsn+": "+layout.Name, http.StatusBadRequest)
		return
	}
}

// worksetParameterIdReadGetHandler read a "page" workset parameter values.
// GET /api/workset-parameter-value-id?dn=a1b2c3d&wsn=mySet&pdn=ff00ee11
// GET /api/workset-parameter-value-id?dn=modelOne&wsn=mySet&pdn=ageSex
// GET /api/workset-parameter-value-id-id?dn=modelOne&wsn=mySet&pdn=ageSex&start=0
// GET /api/workset-parameter-value-id?dn=modelOne&wsn=mySet&pdn=ageSex&start=0&count=100
// GET /api/model/:dn/workset/:wsn/parameter/:pdn/value-id
// GET /api/model/:dn/workset/:wsn/parameter/:pdn/value-id/start/:start
// GET /api/model/:dn/workset/:wsn/parameter/:pdn/value-id/start/:start/count/:count
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func worksetParameterIdReadGetHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "dn")   // model digest-or-name
	wsn := getRequestParam(r, "wsn") // workset name
	pdn := getRequestParam(r, "pdn") // parameter digest-or-name

	// url or query parameters: page offset and page size
	start, ok := getInt64RequestParam(r, "start", 0)
	if !ok {
		http.Error(w, "Invalid value of start row number to read "+pdn, http.StatusBadRequest)
		return
	}
	count, ok := getIntRequestParam(r, "count", pageMaxSize)
	if !ok {
		http.Error(w, "Invalid value of max row count to read "+pdn, http.StatusBadRequest)
		return
	}

	// setup read layout, layout.Name is digest-or-name
	layout := db.ReadLayout{
		Name:      pdn,
		IsFromSet: true,
		IsEditSet: false,
		Offset:    start,
		Size:      int64(count),
	}

	// read parameter page
	if cLst, ok := theCatalog.ReadParameter(dn, wsn, &layout); ok {
		jsonListResponse(w, r, cLst)
	} else {
		http.Error(w, "Error at workset parameter read "+wsn+": "+layout.Name, http.StatusBadRequest)
		return
	}
}
