// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/csv"
	"net/http"
	"net/url"
	"strconv"

	"go.openmpp.org/ompp/omppLog"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
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
	cLst, ok := theCatalog.ReadParameter(dn, src, isCode, &layout)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
		return
	}

	// get converter from id's cell into code cell
	var cvt func(interface{}) (interface{}, error)
	if isCode {
		cvt, ok = theCatalog.ParameterToCodeCellConverter(dn, src, layout.Name)
		if !ok {
			http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
			return
		}
	}

	jsonListResponse(w, r, cLst, cvt)
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
	cLst, ok := theCatalog.ReadOutTable(dn, rdn, isCode, &layout)
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

	jsonListResponse(w, r, cLst, cvt) // write response
}

// worksetParameterPageGetHandler read a "page" of parameter values from workset.
// GET /api/workset-parameter-value?model=modelOne&set=mySet&name=ageSex
// GET /api/workset-parameter-value?model=modelOne&set=mySet&name=ageSex&start=0
// GET /api/workset-parameter-value?model=modelOne&set=mySet&name=ageSex&start=0&count=100
// GET /api/model/:model/workset/:set/parameter/:name/value
// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start
// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start/count/:count
// Dimension(s) and enum-based parameters returned as enum codes.
func worksetParameterPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetPageHandler(w, r, "set", true, true)
}

// runParameterPageGetHandler read a "page" of parameter values from model run results.
// GET /api/run-parameter-value?model=modelOne&run=myRun&name=ageSex
// GET /api/run-parameter-value?model=modelOne&run=myRun&name=ageSex&start=0
// GET /api/run-parameter-value?model=modelOne&run=myRun&name=ageSex&start=0&count=100
// GET /api/model/:model/run/:run/parameter/:name/value
// GET /api/model/:model/run/:run/parameter/:name/value/start/:start
// GET /api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count
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

	// if required get converter from id's cell into code cell
	var cvt func(interface{}) (interface{}, error)

	if isCode {
		cvt, ok = theCatalog.ParameterToCodeCellConverter(dn, src, name)
		if !ok {
			http.Error(w, "Failed to create parameter cell id's to code converter: "+name, http.StatusBadRequest)
			return
		}
	}

	jsonListResponse(w, r, cLst, cvt) // write response
}

// worksetParameterCsvGetHandler read a parameter values from workset and write it as csv response.
// GET /api/workset-parameter-csv?model=modelOne&set=mySet&name=ageSex&bom=true
// GET /api/model/:model/workset/:set/parameter/:name/csv
// Dimension(s) and enum-based parameters returned as enum codes.
func worksetParameterCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doParameterGetCsvHandler(w, r, "set", true, true, isBom)
}

// worksetParameterCsvBomGetHandler read a parameter values from workset and write it as csv response.
// GET /api/model/:model/workset/:set/parameter/:name/csv-bom
// Dimension(s) and enum-based parameters returned as enum codes.
// Response starts from utf-8 BOM bytes.
func worksetParameterCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetCsvHandler(w, r, "set", true, true, true)
}

// worksetParameterIdCsvGetHandler read a parameter values from workset and write it as csv response.
// GET /api/workset-parameter-csv-id?model=modelOne&set=mySet&name=ageSex&bom=true
// GET /api/model/:model/workset/:set/parameter/:name/csv-id
// Dimension(s) and enum-based parameters returned as enum id's.
func worksetParameterIdCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doParameterGetCsvHandler(w, r, "set", true, false, isBom)
}

// worksetParameterIdCsvBomGetHandler read a parameter values from workset and write it as csv response.
// GET /api/model/:model/workset/:set/parameter/:name/csv-id-bom
// Dimension(s) and enum-based parameters returned as enum id's.
// Response starts from utf-8 BOM bytes.
func worksetParameterIdCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetCsvHandler(w, r, "set", true, false, true)
}

// runParameterCsvGetHandler read a parameter values from model run results and write it as csv response.
// GET /api/run-parameter-csv?model=modelOne&run=myRun&name=ageSex&bom=true
// GET /api/model/:model/run/:run/parameter/:name/csv
// Dimension(s) and enum-based parameters returned as enum codes.
func runParameterCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doParameterGetCsvHandler(w, r, "run", false, true, isBom)
}

// runParameterCsvBomGetHandler read a parameter values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/parameter/:name/csv-bom
// Dimension(s) and enum-based parameters returned as enum codes.
// Response starts from utf-8 BOM bytes.
func runParameterCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetCsvHandler(w, r, "run", false, true, true)
}

// runParameterIdCsvGetHandler read a parameter values from model run results and write it as csv response.
// GET /api/run-parameter-csv-id?model=modelOne&run=myRun&name=ageSex&bom=true
// GET /api/model/:model/run/:run/parameter/:name/csv-id
// Dimension(s) and enum-based parameters returned as enum id's.
func runParameterIdCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doParameterGetCsvHandler(w, r, "run", false, false, isBom)
}

// runParameterIdCsvBomGetHandler read a parameter values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/parameter/:name/csv-id-bom
// Dimension(s) and enum-based parameters returned as enum id's.
// Response starts from utf-8 BOM bytes.
func runParameterIdCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetCsvHandler(w, r, "run", false, false, true)
}

// doParameterGetCsvHandler read parameter values from workset or model run and write it as csv response.
// It does read all parameter values, not a "page" of values.
// Dimension(s) and enum-based parameters returned as enum codes or enum id's.
func doParameterGetCsvHandler(w http.ResponseWriter, r *http.Request, srcArg string, isSet, isCode, isBom bool) {

	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	src := getRequestParam(r, srcArg)  // workset name or run digest-or-name
	name := getRequestParam(r, "name") // parameter name

	// read parameter values, page size =0: read all values
	layout := db.ReadParamLayout{
		ReadLayout: db.ReadLayout{Name: name}, IsFromSet: isSet,
	}

	cLst, ok := theCatalog.ReadParameter(dn, src, isCode, &layout)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
		return
	}

	// get converter from cell list to csv rows []string
	hdr, cvt, ok := theCatalog.ParameterToCsvConverter(dn, isCode, name)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+name, http.StatusBadRequest)
		return
	}

	// calculate Content-Length
	nb := 0
	if isBom {
		nb += len(helper.Utf8bom)
	}

	// header length
	for k := range hdr {
		nb += len(hdr[k]) + 1
	}

	// each csv line length: comma-separated and lf as eol
	cs := append([]string{}, hdr...)

	for c := cLst.Front(); c != nil; c = c.Next() {
		if err := cvt(c.Value, cs); err != nil {
			omppLog.Log("Error at convert parameter cell: ", name, ": ", err.Error())
			http.Error(w, "Error at convert cell value of: "+src+": "+name, http.StatusBadRequest)
			return
		}
		for k := range cs {
			nb += len(cs[k]) + 1
		}
	}

	// set response headers
	// todo: ETag instead no-cache and utf-8 file names
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Dispostion", "attachment; filename="+`"`+url.QueryEscape(name)+".csv"+`"`)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Length", strconv.Itoa(nb))

	// write csv body
	if isBom {
		if _, err := w.Write(helper.Utf8bom); err != nil {
			http.Error(w, "Error at csv parameter write: "+src+": "+name, http.StatusBadRequest)
			return
		}
	}

	csvWr := csv.NewWriter(w)

	if err := csvWr.Write(hdr); err != nil {
		http.Error(w, "Error at csv parameter write: "+src+": "+name, http.StatusBadRequest)
		return
	}

	for c := cLst.Front(); c != nil; c = c.Next() {
		if err := cvt(c.Value, cs); err != nil {
			http.Error(w, "Error at convert cell value of: "+src+": "+name, http.StatusBadRequest)
			return
		}
		if err := csvWr.Write(cs); err != nil {
			http.Error(w, "Error at csv parameter write: "+src+": "+name, http.StatusBadRequest)
			return
		}
	}
	csvWr.Flush() // flush csv to response
}

// runTableExprPageGetHandler read a "page" of output table expression(s) values from model run results.
// GET /api/run-table-expr?model=modelOne&run=myRun&name=salarySex&start=0
// GET /api/run-table-expr?model=modelOne&run=myRun&name=salarySex&start=0&count=100
// GET /api/model/:model/run/:run/table/:name/expr
// GET /api/model/:model/run/:run/table/:name/expr/start/:start
// GET /api/model/:model/run/:run/table/:name/expr/start/:start/count/:count
// Enum-based dimension items returned as enum codes.
func runTableExprPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetPageHandler(w, r, false, false, true)
}

// runTableAccPageGetHandler read a "page" of output table accumulator(s) values from model run results.
// GET /api/run-table-acc?model=modelOne&run=myRun&name=salarySex
// GET /api/run-table-acc?model=modelOne&run=myRun&name=salarySex&start=0
// GET /api/run-table-acc?model=modelOne&run=myRun&name=salarySex&start=0&count=100
// GET /api/model/:model/run/:run/table/:name/acc
// GET /api/model/:model/run/:run/table/:name/acc/start/:start
// GET /api/model/:model/run/:run/table/:name/acc/start/:start/count/:count
// Enum-based dimension items returned as enum codes.
func runTableAccPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetPageHandler(w, r, true, false, true)
}

// runTableAllAccPageGetHandler read a "page" of output table accumulator(s) values
// from "all-accumulators" view of model run results.
// GET /api/run-table-all-acc?model=modelOne&run=myRun&name=salarySex
// GET /api/run-table-all-acc?model=modelOne&run=myRun&name=salarySex&start=0
// GET /api/run-table-all-acc?model=modelOne&run=myRun&name=salarySex&start=0&count=100
// GET /api/model/:model/run/:run/table/:name/all-acc
// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start
// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count
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
	rdn := getRequestParam(r, "run")   // run digest-or-name
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

	// if required get converter from id's cell into code cell
	var cvt func(interface{}) (interface{}, error)

	if isCode {
		cvt, ok = theCatalog.TableToCodeCellConverter(dn, layout.Name, layout.IsAccum, layout.IsAllAccum)
		if !ok {
			http.Error(w, "Failed to create output table cell id's to code converter: "+layout.Name, http.StatusBadRequest)
			return
		}
	}

	jsonListResponse(w, r, cLst, cvt) // write response
}
