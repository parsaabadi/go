// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/json"
	"net/http"

	"github.com/openmpp/go/ompp/db"
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
	if !jsonRequestDecode(w, r, true, &layout) {
		return // error at json decode, response done with http error
	}
	layout.IsFromSet = isSet // overwrite json value, it was likely default

	// get converter from id's cell into code cell
	var cvtCell func(interface{}) (interface{}, error)
	if isCode {
		ok := false
		cvtCell, ok = theCatalog.ParameterCellConverter(false, dn, layout.Name)
		if !ok {
			http.Error(w, "Error at parameter read: "+layout.Name, http.StatusBadRequest)
			return
		}
	}

	// write to response: page layout and page data
	jsonSetHeaders(w, r) // start response with set json headers, i.e. content type

	w.Write([]byte("{\"Page\":[")) // start of data page and start of json output array

	enc := json.NewEncoder(w)
	cvtWr := jsonCellWriter(w, enc, cvtCell)

	// read parameter page into json array response, convert enum id's to code if requested
	lt, ok := theCatalog.ReadParameterTo(dn, src, &layout, cvtWr)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
		return
	}
	w.Write([]byte{']'}) // end of data page array

	// continue response with output page layout: offset, size, last page flag
	w.Write([]byte(",\"Layout\":"))

	err := json.NewEncoder(w).Encode(lt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write([]byte("}")) // end of data page and end of json
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
	if !jsonRequestDecode(w, r, true, &layout) {
		return // error at json decode, response done with http error
	}

	// if required get converter from id's cell into code cell
	var cvtCell func(interface{}) (interface{}, error)
	if isCode {
		ok := false
		cvtCell, ok = theCatalog.TableToCodeCellConverter(dn, layout.Name, layout.IsAccum, layout.IsAllAccum)
		if !ok {
			http.Error(w, "Error at output table read: "+layout.Name, http.StatusBadRequest)
			return
		}
	}

	// write to response: page layout and page data
	jsonSetHeaders(w, r) // start response with set json headers, i.e. content type

	w.Write([]byte("{\"Page\":[")) // start of data page and start of json output array

	enc := json.NewEncoder(w)
	cvtWr := jsonCellWriter(w, enc, cvtCell)

	// read parameter page into json array response, convert enum id's to code if requested
	lt, ok := theCatalog.ReadOutTableTo(dn, rdn, &layout, cvtWr)
	if !ok {
		http.Error(w, "Error at run output table read "+rdn+": "+layout.Name, http.StatusBadRequest)
		return
	}
	w.Write([]byte{']'}) // end of data page array

	// continue response with output page layout: offset, size, last page flag
	w.Write([]byte(",\"Layout\":"))

	err := json.NewEncoder(w).Encode(lt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write([]byte("}")) // end of data page and end of json
}

// worksetParameterPageGetHandler read a "page" of parameter values from workset.
// GET /api/model/:model/workset/:set/parameter/:name/value
// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start
// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start/count/:count
// Dimension(s) and enum-based parameters returned as enum codes.
func worksetParameterPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doParameterGetPageHandler(w, r, "set", true, true)
}

// runParameterPageGetHandler read a "page" of parameter values from model run results.
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
	src := getRequestParam(r, srcArg)  // workset name or run digest-or-stamp-or-name
	name := getRequestParam(r, "name") // parameter name

	// url or query parameters: page offset and page size
	start, ok := getInt64RequestParam(r, "start", 0)
	if !ok {
		http.Error(w, "Invalid value of start row number to read "+name, http.StatusBadRequest)
		return
	}
	count, ok := getInt64RequestParam(r, "count", theCfg.pageMaxSize)
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

	// if required get converter from id's cell into code cell
	var cvtCell func(interface{}) (interface{}, error)
	if isCode {
		cvtCell, ok = theCatalog.ParameterCellConverter(false, dn, name)
		if !ok {
			http.Error(w, "Error at parameter read: "+name, http.StatusBadRequest)
			return
		}
	}

	// write to response
	jsonSetHeaders(w, r) // start response with set json headers, i.e. content type

	w.Write([]byte{'['}) // start of json output array

	enc := json.NewEncoder(w)
	cvtWr := jsonCellWriter(w, enc, cvtCell)

	// read parameter page into json array response, convert enum id's to code if requested
	_, ok = theCatalog.ReadParameterTo(dn, src, &layout, cvtWr)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+layout.Name, http.StatusBadRequest)
		return
	}
	w.Write([]byte{']'}) // end of json output array
}

// runTableExprPageGetHandler read a "page" of output table expression(s) values from model run results.
// GET /api/model/:model/run/:run/table/:name/expr
// GET /api/model/:model/run/:run/table/:name/expr/start/:start
// GET /api/model/:model/run/:run/table/:name/expr/start/:start/count/:count
// Enum-based dimension items returned as enum codes.
func runTableExprPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetPageHandler(w, r, false, false, true)
}

// runTableAccPageGetHandler read a "page" of output table accumulator(s) values from model run results.
// GET /api/model/:model/run/:run/table/:name/acc/start/:start
// GET /api/model/:model/run/:run/table/:name/acc/start/:start/count/:count
// Enum-based dimension items returned as enum codes.
func runTableAccPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetPageHandler(w, r, true, false, true)
}

// runTableAllAccPageGetHandler read a "page" of output table accumulator(s) values
// from "all-accumulators" view of model run results.
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
	rdsn := getRequestParam(r, "run")  // run digest-or-stamp-or-name
	name := getRequestParam(r, "name") // output table name

	// url or query parameters: page offset and page size
	start, ok := getInt64RequestParam(r, "start", 0)
	if !ok {
		http.Error(w, "Invalid value of start row number to read "+name, http.StatusBadRequest)
		return
	}
	count, ok := getInt64RequestParam(r, "count", theCfg.pageMaxSize)
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

	// if required get converter from id's cell into code cell
	var cvtCell func(interface{}) (interface{}, error)
	if isCode {
		cvtCell, ok = theCatalog.TableToCodeCellConverter(dn, layout.Name, layout.IsAccum, layout.IsAllAccum)
		if !ok {
			http.Error(w, "Error at run output table read: "+name, http.StatusBadRequest)
			return
		}
	}

	// write to response: page layout and page data
	jsonSetHeaders(w, r) // start response with set json headers, i.e. content type

	w.Write([]byte{'['}) // start of json output array

	enc := json.NewEncoder(w)
	cvtWr := jsonCellWriter(w, enc, cvtCell)

	// read parameter page into json array response, convert enum id's to code if requested
	_, ok = theCatalog.ReadOutTableTo(dn, rdsn, &layout, cvtWr)
	if !ok {
		http.Error(w, "Error at run output table read "+rdsn+": "+layout.Name, http.StatusBadRequest)
		return
	}
	w.Write([]byte{']'}) // end of json output array
}

// runMicrodataPageReadHandler read a "page" of microdata values from model run.
// POST /api/model/:model/run/:run/microdata/value
// Enum-based microdata attributes returned as enum codes.
func runMicrodataPageReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadMicrodataPageHandler(w, r, true)
}

// runMicrodataIdPageReadHandler read a "page" of microdata values from model run.
// POST /api/model/:model/run/:run/microdata/value-id
// Enum-based microdata attributes returned as enum id, not enum codes.
func runMicrodataIdPageReadHandler(w http.ResponseWriter, r *http.Request) {
	doReadMicrodataPageHandler(w, r, false)
}

// doReadMicrodataPageHandler read a "page" of microdata values from model run.
// Json is posted to specify entity name, "page" size and other read arguments,
// see db.ReadMicroLayout for more details.
// If generation digest not specified in ReadMicroLayout then it found by entity name and run digest.
// Page of values is a rows from microdata value table started at zero based offset row
// and up to max page size rows, if page size <= 0 then all values returned.
// Enum-based microdata attributes returned as enum codes or enum id's.
func doReadMicrodataPageHandler(w http.ResponseWriter, r *http.Request, isCode bool) {

	// url parameters
	dn := getRequestParam(r, "model") // model digest-or-name
	rdsn := getRequestParam(r, "run") // run digest-or-stamp-or-name

	// decode json request body
	var layout db.ReadMicroLayout
	if !jsonRequestDecode(w, r, true, &layout) {
		return // error at json decode, response done with http error
	}

	// get converter from id's cell into code cell
	var cvtCell func(interface{}) (interface{}, error)
	if isCode {

		ok := false
		genDigest := ""
		_, genDigest, cvtCell, ok = theCatalog.MicrodataCellConverter(false, dn, rdsn, layout.Name)
		if !ok {
			http.Error(w, "Error at run microdata read: "+layout.Name, http.StatusBadRequest)
			return
		}
		if layout.GenDigest != "" && layout.GenDigest != genDigest {
			http.Error(w, "Error at run microdata read, generation digest not found: "+layout.GenDigest+" expected: "+genDigest+": "+layout.Name, http.StatusBadRequest)
			return
		}
	}

	// write to response: page layout and page data
	jsonSetHeaders(w, r) // start response with set json headers, i.e. content type

	w.Write([]byte("{\"Page\":[")) // start of data page and start of json output array

	enc := json.NewEncoder(w)
	cvtWr := jsonCellWriter(w, enc, cvtCell)

	// read microdata page into json array response, convert enum id's to code if requested
	lt, ok := theCatalog.ReadMicrodataTo(dn, rdsn, &layout, cvtWr)
	if !ok {
		http.Error(w, "Error at run microdata read "+rdsn+": "+layout.Name, http.StatusBadRequest)
		return
	}
	w.Write([]byte{']'}) // end of data page array

	// continue response with output page layout: offset, size, last page flag
	w.Write([]byte(",\"Layout\":"))

	err := json.NewEncoder(w).Encode(lt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write([]byte("}")) // end of data page and end of json
}

// runMicrodatarPageGetHandler read a "page" of microdata values from model run results.
// GET /api/model/:model/run/:run/microdata/:name/value
// GET /api/model/:model/run/:run/microdata/:name/value/start/:start
// GET /api/model/:model/run/:run/microdata/:name/value/start/:start/count/:count
// Enum-based microdata attributes returned as enum codes.
func runMicrodatarPageGetHandler(w http.ResponseWriter, r *http.Request) {
	doMicrodataGetPageHandler(w, r, true)
}

// doMicrodataGetPageHandler read a "page" of microdata values from model run.
// Page of values is a rows from microdata value table started at zero based offset row
// and up to max page size rows, if page size <= 0 then all values returned.
// Enum-based microdata attributes returned as enum codes or enum id's.
func doMicrodataGetPageHandler(w http.ResponseWriter, r *http.Request, isCode bool) {

	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	rdsn := getRequestParam(r, "run")  // run digest-or-stamp-or-name
	name := getRequestParam(r, "name") // entity name

	// url or query parameters: page offset and page size
	start, ok := getInt64RequestParam(r, "start", 0)
	if !ok {
		http.Error(w, "Invalid value of start row number to read "+name, http.StatusBadRequest)
		return
	}
	count, ok := getInt64RequestParam(r, "count", theCfg.pageMaxSize)
	if !ok {
		http.Error(w, "Invalid value of max row count to read "+name, http.StatusBadRequest)
		return
	}

	// setup read layout
	layout := db.ReadMicroLayout{
		ReadLayout: db.ReadLayout{
			Name:           name,
			ReadPageLayout: db.ReadPageLayout{Offset: start, Size: count},
		},
	}

	// get converter from id's cell into code cell
	var cvtCell func(interface{}) (interface{}, error)
	if isCode {

		ok := false
		genDigest := ""
		_, genDigest, cvtCell, ok = theCatalog.MicrodataCellConverter(false, dn, rdsn, layout.Name)
		if !ok {
			http.Error(w, "Error at run microdata read: "+name, http.StatusBadRequest)
			return
		}
		layout.GenDigest = genDigest
	}

	// write to response
	jsonSetHeaders(w, r) // start response with set json headers, i.e. content type

	w.Write([]byte{'['}) // start of json output array

	enc := json.NewEncoder(w)
	cvtWr := jsonCellWriter(w, enc, cvtCell)

	// read microdata page into json array response, convert enum id's to code if requested
	_, ok = theCatalog.ReadMicrodataTo(dn, rdsn, &layout, cvtWr)
	if !ok {
		http.Error(w, "Error at run microdata read "+rdsn+": "+layout.Name, http.StatusBadRequest)
		return
	}
	w.Write([]byte{']'}) // end of json output array
}
