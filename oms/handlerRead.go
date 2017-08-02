// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"go.openmpp.org/ompp/db"
)

// worksetParameterIdReadHandler read a "page" of parameter values from workset.
// POST /api/model/:dn/workset/:wsn/parameter/value-id
// Json is posted to specify parameter name, "page" size and other read arguments,
// see db.ReadParamLayout for more details.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func worksetParameterIdReadHandler(w http.ResponseWriter, r *http.Request) {

	// get url parameters and decode json request body
	dn, wsn, layout, ok := requestJsonToReadParamLayout(w, r, "wsn", true)
	if !ok {
		return // error in parameters, response done with http error
	}

	// read parameter page
	if cLst, ok := theCatalog.ReadParameter(dn, wsn, layout); ok {
		jsonListResponse(w, r, cLst)
	} else {
		http.Error(w, "Error at workset parameter read "+wsn+": "+layout.Name, http.StatusBadRequest)
		return
	}
}

// worksetParameterIdReadGetHandler read a "page" of parameter values from workset.
// GET /api/workset-parameter-value-id?dn=modelOne&wsn=mySet&name=ageSex
// GET /api/workset-parameter-value-id?dn=modelOne&wsn=mySet&name=ageSex&start=0
// GET /api/workset-parameter-value-id?dn=modelOne&wsn=mySet&name=ageSex&start=0&count=100
// GET /api/model/:dn/workset/:wsn/parameter/:name/value-id
// GET /api/model/:dn/workset/:wsn/parameter/:name/value-id/start/:start
// GET /api/model/:dn/workset/:wsn/parameter/:name/value-id/start/:start/count/:count
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func worksetParameterIdReadGetHandler(w http.ResponseWriter, r *http.Request) {

	// from url or query parameters to read laout
	dn, wsn, layout, ok := requestGetToReadParamLayout(w, r, "wsn", true)
	if !ok {
		return // error in parameters, response done with http error
	}
	layout.IsEditSet = false // select for read, not for edit

	// read parameter page
	if cLst, ok := theCatalog.ReadParameter(dn, wsn, layout); ok {
		jsonListResponse(w, r, cLst)
	} else {
		http.Error(w, "Error at workset parameter read "+wsn+": "+layout.Name, http.StatusBadRequest)
		return
	}
}

// worksetParameterCodeReadHandler read a "page" of parameter values from workset.
// POST /api/model/:dn/workset/:wsn/parameter/value
// Json is posted to specify parameter name, "page" size and other read arguments,
// see db.ReadParamLayout for more details.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum codes.
func worksetParameterCodeReadHandler(w http.ResponseWriter, r *http.Request) {
	// TODO
}

// runParameterIdReadHandler read a "page" of parameter values from model run.
// POST /api/model/:dn/run/:rdn/parameter/value-id
// Json is posted to specify parameter name, "page" size and other read arguments,
// see db.ReadParamLayout for more details.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func runParameterIdReadHandler(w http.ResponseWriter, r *http.Request) {

	// get url parameters and decode json request body
	dn, rdn, layout, ok := requestJsonToReadParamLayout(w, r, "rdn", false)
	if !ok {
		return // error in parameters, response done with http error
	}

	// read parameter page
	if cLst, ok := theCatalog.ReadParameter(dn, rdn, layout); ok {
		jsonListResponse(w, r, cLst)
	} else {
		http.Error(w, "Error at run parameter read "+rdn+": "+layout.Name, http.StatusBadRequest)
		return
	}
}

// runParameterIdReadGetHandler read a "page" of parameter values from model run results.
// GET /api/run-parameter-value-id?dn=a1b2c3d&rdn=1f2e3d4&name=ageSex
// GET /api/run-parameter-value-id?dn=modelOne&rdn=myRun&name=ageSex
// GET /api/run-parameter-value-id?dn=modelOne&rdn=1f2e3d4&name=ageSex&start=0
// GET /api/run-parameter-value-id?dn=modelOne&rdn=myRun&name=ageSex&start=0&count=100
// GET /api/model/:dn/run/:rdn/parameter/:name/value-id
// GET /api/model/:dn/run/:rdn/parameter/:name/value-id/start/:start
// GET /api/model/:dn/run/:rdn/parameter/:name/value-id/start/:start/count/:count
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func runParameterIdReadGetHandler(w http.ResponseWriter, r *http.Request) {

	// from url or query parameters to read laout
	dn, rdn, layout, ok := requestGetToReadParamLayout(w, r, "rdn", false)
	if !ok {
		return // error in parameters, response done with http error
	}

	// read parameter page
	if cLst, ok := theCatalog.ReadParameter(dn, rdn, layout); ok {
		jsonListResponse(w, r, cLst)
	} else {
		http.Error(w, "Error at run parameter read "+rdn+": "+layout.Name, http.StatusBadRequest)
		return
	}
}

// GET /api/run-table-value-id-expr?dn=modelOne&rdn=myRun&name=salarySex
// GET /api/run-table-value-id-expr?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0
// GET /api/run-table-value-id-expr?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0&count=100
// GET /api/model/:dn/run/:rdn/table/:name/value-id/expr
// GET /api/model/:dn/run/:rdn/table/:name/value-id/expr/start/:start
// GET /api/model/:dn/run/:rdn/table/:name/value-id/expr/start/:start/count/:count
func runTableIdExprReadGetHandler(w http.ResponseWriter, r *http.Request) {
	// TODO
}

// GET /api/run-table-value-id-acc?dn=modelOne&rdn=myRun&name=salarySex
// GET /api/run-table-value-id-acc?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0
// GET /api/run-table-value-id-acc?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0&count=100
// GET /api/model/:dn/run/:rdn/table/:name/value-id/acc
// GET /api/model/:dn/run/:rdn/table/:name/value-id/acc/start/:start
// GET /api/model/:dn/run/:rdn/table/:name/value-id/acc/start/:start/count/:count
func runTableIdAccReadGetHandler(w http.ResponseWriter, r *http.Request) {
	// TODO
}

// GET /api/run-table-value-id-all-acc?dn=modelOne&rdn=myRun&name=salarySex
// GET /api/run-table-value-id-all-acc?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0
// GET /api/run-table-value-id-all-acc?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0&count=100
// GET /api/model/:dn/run/:rdn/table/:name/value-id/all-acc
// GET /api/model/:dn/run/:rdn/table/:name/value-id/all-acc/start/:start
// GET /api/model/:dn/run/:rdn/table/:name/value-id/all-acc/start/:start/count/:count
func runTableIdAllAccReadGetHandler(w http.ResponseWriter, r *http.Request) {
	// TODO
}

// POST /api/model/:dn/run/:rdn/table/value-id
func runTableIdReadHandler(w http.ResponseWriter, r *http.Request) {
	// TODO
}

// requestJsonToReadParamLayout return model, workset or run digest-or name and ReadLayout
// from url parameters and JSON body
func requestJsonToReadParamLayout(
	w http.ResponseWriter, r *http.Request, srcParam string, isSet bool) (string, string, *db.ReadParamLayout, bool) {

	// url parameters
	dn := getRequestParam(r, "dn")      // model digest-or-name
	src := getRequestParam(r, srcParam) // workset name or run digest-or-name

	// decode json request body
	var layout db.ReadParamLayout
	if !jsonRequestDecode(w, r, &layout) {
		return "", "", &db.ReadParamLayout{}, false // error at json decode, response done with http error
	}

	// check: is it read from model run or from workset
	if layout.IsFromSet != isSet {
		if isSet {
			http.Error(w, "It must be read of parameter from workset "+src+": "+layout.Name, http.StatusBadRequest)
		} else {
			http.Error(w, "It must be read of parameter from model run "+src+": "+layout.Name, http.StatusBadRequest)
		}
		return "", "", &db.ReadParamLayout{}, false
	}

	return dn, src, &layout, true
}

// requestGetToReadLayout return model, workset or run digest-or name and ReadLayout
// from GET parameters or url parameters
func requestGetToReadParamLayout(
	w http.ResponseWriter, r *http.Request, srcParam string, isSet bool) (string, string, *db.ReadParamLayout, bool) {

	// url or query parameters
	dn := getRequestParam(r, "dn")      // model digest-or-name
	src := getRequestParam(r, srcParam) // workset name or run digest-or-name
	name := getRequestParam(r, "name")  // parameter name

	// url or query parameters: page offset and page size
	start, ok := getInt64RequestParam(r, "start", 0)
	if !ok {
		http.Error(w, "Invalid value of start row number to read "+name, http.StatusBadRequest)
		return "", "", &db.ReadParamLayout{}, false
	}
	count, ok := getInt64RequestParam(r, "count", pageMaxSize)
	if !ok {
		http.Error(w, "Invalid value of max row count to read "+name, http.StatusBadRequest)
		return "", "", &db.ReadParamLayout{}, false
	}

	// setup read layout
	layout := db.ReadParamLayout{
		ReadLayout: db.ReadLayout{Name: name, Offset: start, Size: count},
		IsFromSet:  isSet,
	}
	return dn, src, &layout, true
}
