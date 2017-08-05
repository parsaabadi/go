// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"go.openmpp.org/ompp/db"
)

// worksetParameterCodeReadHandler read a "page" of parameter values from workset.
// POST /api/model/:dn/workset/:wsn/parameter/value
// Dimension(s) and enum-based parameters returned as enum codes.
func worksetParameterCodeReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadParameterHandler(w, r, "wsn", true, true)
}

// worksetParameterIdReadHandler read a "page" of parameter values from workset.
// POST /api/model/:dn/workset/:wsn/parameter/value-id
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func worksetParameterIdReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadParameterHandler(w, r, "wsn", true, false)
}

// runParameterCodeReadHandler read a "page" of parameter values from model run.
// POST /api/model/:dn/run/:rdn/parameter/value
// Dimension(s) and enum-based parameters returned as enum codes.
func runParameterCodeReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadParameterHandler(w, r, "rdn", false, true)
}

// runParameterIdReadHandler read a "page" of parameter values from model run.
// POST /api/model/:dn/run/:rdn/parameter/value-id
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func runParameterIdReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadParameterHandler(w, r, "rdn", false, false)
}

// doReadParameterHandler read a "page" of parameter values from workset or model run.
// Json is posted to specify parameter name, "page" size and other read arguments,
// see db.ReadParamLayout for more details.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum codes or enum id's
func doReadParameterHandler(w http.ResponseWriter, r *http.Request, srcArg string, isSet, isCode bool) {

	// url parameters
	dn := getRequestParam(r, "dn")    // model digest-or-name
	src := getRequestParam(r, srcArg) // workset name or run digest-or-name

	// decode json request body
	var layout db.ReadParamLayout
	if !jsonRequestDecode(w, r, &layout) {
		return // error at json decode, response done with http error
	}

	// check: is it read from model run or from workset
	if layout.IsFromSet != isSet {
		if isSet {
			http.Error(w, "It must be read of parameter from workset "+src+": "+layout.Name, http.StatusBadRequest)
		} else {
			http.Error(w, "It must be read of parameter from model run "+src+": "+layout.Name, http.StatusBadRequest)
		}
		return
	}

	// read parameter page and respond with json and convert enum id's to code if requested
	cLst, ok := theCatalog.ReadParameter(dn, src, isCode, &layout)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
		return
	}

	jsonListResponse(w, r, cLst)
}

// runTableCodeReadHandler read a "page" of output table values
// from expression table, accumulator table or "all-accumulators" view of model run.
// POST /api/model/:dn/run/:rdn/table/value
// Dimension items returned as enum codes or, if dimension type simple as string values
func runTableCodeReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadTableHandler(w, r, true)
}

// runTableIdReadHandler read a "page" of output table values
// from expression table, accumulator table or "all-accumulators" view of model run.
// POST /api/model/:dn/run/:rdn/table/value-id
// Dimension(s) returned as enum id, not enum codes.
func runTableIdReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadTableHandler(w, r, false)
}

// doReadTableHandler read a "page" of output table values
// from expression table, accumulator table or "all-accumulators" view of model run.
// Json is posted to specify parameter name, "page" size and other read arguments,
// see db.ReadOutTableLayout for more details.
// Page is part of output table values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension items returned enum id's or as enum codes and for dimension type simple as string values.
func doReadTableHandler(w http.ResponseWriter, r *http.Request, isCode bool) {

	// url parameters
	dn := getRequestParam(r, "dn")   // model digest-or-name
	rdn := getRequestParam(r, "rdn") // run digest-or-name

	// decode json request body
	var layout db.ReadOutTableLayout
	if !jsonRequestDecode(w, r, &layout) {
		return // error at json decode, response done with http error
	}

	// read output table page and respond with json and convert enum id's to code if requested
	cLst, ok := theCatalog.ReadOutTable(dn, rdn, isCode, &layout)
	if !ok {
		http.Error(w, "Error at run output table read "+rdn+": "+layout.Name, http.StatusBadRequest)
		return
	}

	jsonListResponse(w, r, cLst)
}

// worksetParameterIdReadGetHandler read a "page" of parameter values from workset.
// GET /api/workset-parameter-value?dn=modelOne&wsn=mySet&name=ageSex
// GET /api/workset-parameter-value?dn=modelOne&wsn=mySet&name=ageSex&start=0
// GET /api/workset-parameter-value?dn=modelOne&wsn=mySet&name=ageSex&start=0&count=100
// GET /api/model/:dn/workset/:wsn/parameter/:name/value
// GET /api/model/:dn/workset/:wsn/parameter/:name/value/start/:start
// GET /api/model/:dn/workset/:wsn/parameter/:name/value/start/:start/count/:count
// Dimension(s) and enum-based parameters returned as enum codes.
func worksetParameterPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetPageHandler(w, r, "wsn", true, true)
}

// runParameterIdReadGetHandler read a "page" of parameter values from model run results.
// GET /api/run-parameter-value?dn=a1b2c3d&rdn=1f2e3d4&name=ageSex
// GET /api/run-parameter-value?dn=modelOne&rdn=myRun&name=ageSex
// GET /api/run-parameter-value?dn=modelOne&rdn=1f2e3d4&name=ageSex&start=0
// GET /api/run-parameter-value?dn=modelOne&rdn=myRun&name=ageSex&start=0&count=100
// GET /api/model/:dn/run/:rdn/parameter/:name/value
// GET /api/model/:dn/run/:rdn/parameter/:name/value/start/:start
// GET /api/model/:dn/run/:rdn/parameter/:name/value/start/:start/count/:count
// Dimension(s) and enum-based parameters returned as enum codes.
func runParameterPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetPageHandler(w, r, "rdn", false, true)
}

// doParameterGetPageHandler read a "page" of parameter values from workset or model run.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum codes or enum id's.
func doParameterGetPageHandler(w http.ResponseWriter, r *http.Request, srcArg string, isSet, isCode bool) {

	// url or query parameters
	dn := getRequestParam(r, "dn")     // model digest-or-name
	src := getRequestParam(r, srcArg)  // workset name or run digest-or-name
	name := getRequestParam(r, "name") // parameter name

	// url or query parameters: page offset and page size
	start, ok := getInt64RequestParam(r, "start", 0)
	if !ok {
		http.Error(w, "Invalid value of start row number to read "+name, http.StatusBadRequest)
		return
	}
	count, ok := getInt64RequestParam(r, "count", pageMaxSize)
	if !ok {
		http.Error(w, "Invalid value of max row count to read "+name, http.StatusBadRequest)
		return
	}

	// setup read layout
	layout := db.ReadParamLayout{
		ReadLayout: db.ReadLayout{Name: name, Offset: start, Size: count},
		IsFromSet:  isSet,
	}

	// read parameter page and respond with json
	cLst, ok := theCatalog.ReadParameter(dn, src, isCode, &layout)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
		return
	}

	jsonListResponse(w, r, cLst)
}

// runTableIdExprReadGetHandler read a "page" of output table expression(s) values from model run results.
// GET /api/run-table-expr?dn=modelOne&rdn=myRun&name=salarySex
// GET /api/run-table-expr?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0
// GET /api/run-table-expr?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0&count=100
// GET /api/model/:dn/run/:rdn/table/:name/expr
// GET /api/model/:dn/run/:rdn/table/:name/expr/start/:start
// GET /api/model/:dn/run/:rdn/table/:name/expr/start/:start/count/:count
// Enum-based dimension items returned as enum codes.
func runTableExprPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTablePageGetHandler(w, r, false, false, true)
}

// runTableIdAccReadGetHandler read a "page" of output table accumulator(s) values from model run results.
// GET /api/run-table-acc?dn=modelOne&rdn=myRun&name=salarySex
// GET /api/run-table-acc?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0
// GET /api/run-table-acc?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0&count=100
// GET /api/model/:dn/run/:rdn/table/:name/acc
// GET /api/model/:dn/run/:rdn/table/:name/acc/start/:start
// GET /api/model/:dn/run/:rdn/table/:name/acc/start/:start/count/:count
// Enum-based dimension items returned as enum codes.
func runTableAccPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTablePageGetHandler(w, r, true, false, true)
}

// runTableIdAllAccReadGetHandler read a "page" of output table accumulator(s) values
// from "all-accumulators" view of model run results.
// GET /api/run-table-all-acc?dn=modelOne&rdn=myRun&name=salarySex
// GET /api/run-table-all-acc?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0
// GET /api/run-table-all-acc?dn=a1b2c3d&rdn=1f2e3d4&name=salarySex&start=0&count=100
// GET /api/model/:dn/run/:rdn/table/:name/all-acc
// GET /api/model/:dn/run/:rdn/table/:name/all-acc/start/:start
// GET /api/model/:dn/run/:rdn/table/:name/all-acc/start/:start/count/:count
// Enum-based dimension items returned as enum codes.
func runTableAllAccPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTablePageGetHandler(w, r, true, true, true)
}

// doTablePageGetHandler read a "page" of values from
// output table expressions, accumulators or "all-accumulators" views.
// Page is part of output table values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Enum-based dimension items returned as enum id or as enum codes.
func doTablePageGetHandler(w http.ResponseWriter, r *http.Request, isAcc, isAllAcc, isCode bool) {

	// url or query parameters
	dn := getRequestParam(r, "dn")     // model digest-or-name
	rdn := getRequestParam(r, "rdn")   // run digest-or-name
	name := getRequestParam(r, "name") // output table name

	// url or query parameters: page offset and page size
	start, ok := getInt64RequestParam(r, "start", 0)
	if !ok {
		http.Error(w, "Invalid value of start row number to read "+name, http.StatusBadRequest)
		return
	}
	count, ok := getInt64RequestParam(r, "count", pageMaxSize)
	if !ok {
		http.Error(w, "Invalid value of max row count to read "+name, http.StatusBadRequest)
		return
	}

	// setup read layout
	layout := db.ReadOutTableLayout{
		ReadLayout: db.ReadLayout{Name: name, Offset: start, Size: count},
		IsAccum:    isAcc,
		IsAllAccum: isAllAcc,
	}

	// read output table page and respond with json
	cLst, ok := theCatalog.ReadOutTable(dn, rdn, isCode, &layout)
	if !ok {
		http.Error(w, "Error at run output table read "+rdn+": "+layout.Name, http.StatusBadRequest)
		return
	}

	jsonListResponse(w, r, cLst)
}
