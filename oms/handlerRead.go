// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/json"
	"net/http"

	"go.openmpp.org/ompp/db"
)

// worksetParameterPageReadHandler read a "page" of parameter values from workset.
// POST /api/model/:model/workset/:set/parameter/value
// Dimension(s) and enum-based parameters returned as enum codes.
func worksetParameterPageReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadParameterPageHandler(w, r, "set", true, true)
}

// worksetParameterIdPageReadHandler read a "page" of parameter values from workset.
// POST /api/model/:model/workset/:set/parameter/value-id
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func worksetParameterIdPageReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadParameterPageHandler(w, r, "set", true, false)
}

// runParameterPageReadHandler read a "page" of parameter values from model run.
// POST /api/model/:model/run/:run/parameter/value
// Dimension(s) and enum-based parameters returned as enum codes.
func runParameterPageReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadParameterPageHandler(w, r, "run", false, true)
}

// runParameterIdPageReadHandler read a "page" of parameter values from model run.
// POST /api/model/:model/run/:run/parameter/value-id
// Dimension(s) and enum-based parameters returned as enum id, not enum codes.
func runParameterIdPageReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadParameterPageHandler(w, r, "run", false, false)
}

// doReadParameterPageHandler read a "page" of parameter values from workset or model run.
// Json is posted to specify parameter name, "page" size and other read arguments,
// see db.ReadParamLayout for more details.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum codes or enum id's
func doReadParameterPageHandler(w http.ResponseWriter, r *http.Request, srcArg string, isSet, isCode bool) {

	// url parameters
	dn := getRequestParam(r, "model") // model digest-or-name
	src := getRequestParam(r, srcArg) // workset name or run digest-or-name

	// decode json request body
	var layout db.ReadParamLayout
	if !jsonRequestDecode(w, r, &layout) {
		return // error at json decode, response done with http error
	}
	layout.IsFromSet = isSet // overwrite json value, it was likely default

	// read parameter page and respond with json and convert enum id's to code if requested
	cLst, lt, ok := theCatalog.ReadParameter(dn, src, &layout)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
		return
	}

	// get converter from id's cell into code cell
	var cvt func(interface{}) (interface{}, error)
	if isCode {
		cvt, ok = theCatalog.ParameterCellConverter(false, dn, layout.Name)
		if !ok {
			http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
			return
		}
	}

	// write to response: page layout and page data
	jsonSetHeaders(w, r) // start response with set json headers, i.e. content type

	// output page layout: offset, size, last page flag
	w.Write([]byte("{\"Layout\":"))
	err := json.NewEncoder(w).Encode(lt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Write([]byte(",\"Page\":"))             // start of data page
	jsonAppendListToResponse(w, r, cLst, cvt) // append data page to response
	w.Write([]byte("}"))                      // end of data page and end of json
}

// runTablePageReadHandler read a "page" of output table values
// from expression table, accumulator table or "all-accumulators" view of model run.
// POST /api/model/:model/run/:run/table/value
// Dimension items returned as enum codes or, if dimension type simple as string values
func runTablePageReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadTablePageHandler(w, r, true)
}

// runTableIdPageReadHandler read a "page" of output table values
// from expression table, accumulator table or "all-accumulators" view of model run.
// POST /api/model/:model/run/:run/table/value-id
// Dimension(s) returned as enum id, not enum codes.
func runTableIdPageReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadTablePageHandler(w, r, false)
}

// doReadTablePageHandler read a "page" of output table values
// from expression table, accumulator table or "all-accumulators" view of model run.
// Json is posted to specify parameter name, "page" size and other read arguments,
// see db.ReadTableLayout for more details.
// Page is part of output table values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension items returned enum id's or as enum codes and for dimension type simple as string values.
func doReadTablePageHandler(w http.ResponseWriter, r *http.Request, isCode bool) {

	// url parameters
	dn := getRequestParam(r, "model") // model digest-or-name
	rdn := getRequestParam(r, "run")  // run digest-or-name

	// decode json request body
	var layout db.ReadTableLayout
	if !jsonRequestDecode(w, r, &layout) {
		return // error at json decode, response done with http error
	}

	// read output table page and respond with json and convert enum id's to code if requested
	cLst, lt, ok := theCatalog.ReadOutTable(dn, rdn, &layout)
	if !ok {
		http.Error(w, "Error at run output table read "+rdn+": "+layout.Name, http.StatusBadRequest)
		return
	}

	// if required get converter from id's cell into code cell
	var cvt func(interface{}) (interface{}, error)

	if isCode {
		cvt, ok = theCatalog.TableToCodeCellConverter(dn, layout.Name, layout.IsAccum, layout.IsAllAccum)
		if !ok {
			http.Error(w, "Failed to create output table cell id's to code converter: "+layout.Name, http.StatusBadRequest)
			return
		}
	}

	// write to response: page layout and page data
	jsonSetHeaders(w, r) // start response with set json headers, i.e. content type

	// output page layout: offset, size, last page flag
	w.Write([]byte("{\"Layout\":"))
	err := json.NewEncoder(w).Encode(lt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Write([]byte(",\"Page\":"))             // start of data page
	jsonAppendListToResponse(w, r, cLst, cvt) // append data page to response
	w.Write([]byte("}"))                      // end of data page and end of json
}

// worksetParameterPageGetHandler read a "page" of parameter values from workset.
// GET /api/model/:model/workset/:set/parameter/:name/value
// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start
// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start/count/:count
// GET /api/workset-parameter-value?model=modelNameOrDigest&set=setName&name=parameterName&start=0&count=100
// Dimension(s) and enum-based parameters returned as enum codes.
func worksetParameterPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetPageHandler(w, r, "set", true, true)
}

// runParameterPageGetHandler read a "page" of parameter values from model run results.
// GET /api/model/:model/run/:run/parameter/:name/value
// GET /api/model/:model/run/:run/parameter/:name/value/start/:start
// GET /api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count
// GET /api/run-parameter-value?model=modelNameOrDigest&run=runDigestOrStampOrName&name=parameterName&start=0&count=100
// Dimension(s) and enum-based parameters returned as enum codes.
func runParameterPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetPageHandler(w, r, "run", false, true)
}

// doParameterGetPageHandler read a "page" of parameter values from workset or model run.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Dimension(s) and enum-based parameters returned as enum codes or enum id's.
func doParameterGetPageHandler(w http.ResponseWriter, r *http.Request, srcArg string, isSet, isCode bool) {

	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	src := getRequestParam(r, srcArg)  // workset name or run digest-or-stamp-or-name
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
		ReadLayout: db.ReadLayout{
			Name:           name,
			ReadPageLayout: db.ReadPageLayout{Offset: start, Size: count},
		},
		IsFromSet: isSet,
	}

	// read parameter page and respond with json
	cLst, _, ok := theCatalog.ReadParameter(dn, src, &layout)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
		return
	}

	// if required get converter from id's cell into code cell
	var cvt func(interface{}) (interface{}, error)

	if isCode {
		cvt, ok = theCatalog.ParameterCellConverter(false, dn, name)
		if !ok {
			http.Error(w, "Failed to create parameter cell id's to code converter: "+name, http.StatusBadRequest)
			return
		}
	}

	jsonSetHeaders(w, r)                      // start response with set json headers, i.e. content type
	jsonAppendListToResponse(w, r, cLst, cvt) // append data page to response
}

// runTableExprPageGetHandler read a "page" of output table expression(s) values from model run results.
// GET /api/model/:model/run/:run/table/:name/expr
// GET /api/model/:model/run/:run/table/:name/expr/start/:start
// GET /api/model/:model/run/:run/table/:name/expr/start/:start/count/:count
// GET /api/run-table-expr?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&start=0&count=100
// Enum-based dimension items returned as enum codes.
func runTableExprPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetPageHandler(w, r, false, false, true)
}

// runTableAccPageGetHandler read a "page" of output table accumulator(s) values from model run results.
// GET /api/model/:model/run/:run/table/:name/acc/start/:start
// GET /api/model/:model/run/:run/table/:name/acc/start/:start/count/:count
// GET /api/run-table-acc?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&start=0&count=100
// Enum-based dimension items returned as enum codes.
func runTableAccPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetPageHandler(w, r, true, false, true)
}

// runTableAllAccPageGetHandler read a "page" of output table accumulator(s) values
// from "all-accumulators" view of model run results.
// GET /api/model/:model/run/:run/table/:name/all-acc
// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start
// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count
// GET /api/run-table-all-acc?model=modelNameOrDigest&run=runDigestOrStampOrName&name=tableName&start=0&count=100
// Enum-based dimension items returned as enum codes.
func runTableAllAccPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetPageHandler(w, r, true, true, true)
}

// doTableGetPageHandler read a "page" of values from
// output table expressions, accumulators or "all-accumulators" views.
// Page is part of output table values defined by zero-based "start" row number and row count.
// If row count <= 0 then all rows returned.
// Enum-based dimension items returned as enum id or as enum codes.
func doTableGetPageHandler(w http.ResponseWriter, r *http.Request, isAcc, isAllAcc, isCode bool) {

	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	rdsn := getRequestParam(r, "run")  // run digest-or-stamp-or-name
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
	layout := db.ReadTableLayout{
		ReadLayout: db.ReadLayout{Name: name,
			ReadPageLayout: db.ReadPageLayout{Offset: start, Size: count},
		},
		IsAccum:    isAcc,
		IsAllAccum: isAllAcc,
	}

	// read output table page and respond with json
	cLst, _, ok := theCatalog.ReadOutTable(dn, rdsn, &layout)
	if !ok {
		http.Error(w, "Error at run output table read "+rdsn+": "+layout.Name, http.StatusBadRequest)
		return
	}

	// if required get converter from id's cell into code cell
	var cvt func(interface{}) (interface{}, error)

	if isCode {
		cvt, ok = theCatalog.TableToCodeCellConverter(dn, layout.Name, layout.IsAccum, layout.IsAllAccum)
		if !ok {
			http.Error(w, "Failed to create output table cell id's to code converter: "+layout.Name, http.StatusBadRequest)
			return
		}
	}

	jsonSetHeaders(w, r)                      // start response with set json headers, i.e. content type
	jsonAppendListToResponse(w, r, cLst, cvt) // append data page to response
}
