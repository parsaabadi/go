// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"encoding/csv"
	"net/http"
	"net/url"
	"strconv"

	"go.openmpp.org/ompp/omppLog"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
)

// worksetParameterCsvGetHandler read a parameter values from workset and write it as csv response.
// GET /api/model/:model/workset/:set/parameter/:name/csv
// GET /api/workset-parameter-csv?model=modelNameOrDigest&set=setName&name=parameterName&bom=true
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
// GET /api/model/:model/workset/:set/parameter/:name/csv-id
// GET /api/workset-parameter-csv-id?model=modelNameOrDigest&set=setName&name=parameterName&bom=true
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
// GET /api/model/:model/run/:run/parameter/:name/csv
// GET /api/run-parameter-csv?model=modelNameOrDigest&run=runNameOrDigest&name=parameterName&bom=true
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
// GET /api/model/:model/run/:run/parameter/:name/csv-id
// GET /api/run-parameter-csv-id?model=modelNameOrDigest&run=runNameOrDigest&name=parameterName&bom=true
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

	cLst, _, ok := theCatalog.ReadParameter(dn, src, &layout)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+name, http.StatusBadRequest)
		return
	}

	// get converter from cell list to csv rows []string
	hdr, cvt, ok := theCatalog.ParameterToCsvConverter(dn, isCode, name)
	if !ok {
		http.Error(w, "Failed to create parameter csv converter "+src+": "+name, http.StatusBadRequest)
		return
	}

	// convert to csv and write into http response
	writeCsvResponse(w, name, src, isBom, hdr, cLst, cvt)
}

// runTableExprCsvGetHandler read table expression(s) values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/expr/csv
// GET /api/run-table-expr-csv?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
// Dimension(s) returned as enum codes.
func runTableExprCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doTableGetCsvHandler(w, r, false, false, true, isBom)
}

// runTableExprCsvBomGetHandler read table expression(s) values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/expr/csv-bom
// Dimension(s) returned as enum codes.
// Response starts from utf-8 BOM bytes.
func runTableExprCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetCsvHandler(w, r, false, false, true, true)
}

// runTableExprIdCsvGetHandler read table expression(s) values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/expr/csv-id
// GET /api/run-table-expr-csv-id?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
// Dimension(s) returned as enum id's.
func runTableExprIdCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doTableGetCsvHandler(w, r, false, false, false, isBom)
}

// runTableExprIdCsvBomGetHandler read table expression(s) values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/expr/csv-id-bom
// Dimension(s) returned as enum id's.
// Response starts from utf-8 BOM bytes.
func runTableExprIdCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetCsvHandler(w, r, false, false, false, true)
}

// runTableAccCsvGetHandler read table accumultor(s) values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/acc/csv
// GET /api/run-table-acc-csv?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
// Dimension(s) returned as enum codes.
func runTableAccCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doTableGetCsvHandler(w, r, true, false, true, isBom)
}

// runTableAccCsvBomGetHandler read table accumultor(s) values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/acc/csv-bom
// Dimension(s) returned as enum codes.
// Response starts from utf-8 BOM bytes.
func runTableAccCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetCsvHandler(w, r, true, false, true, true)
}

// runTableAccIdCsvGetHandler read table accumultor(s) values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/acc/csv-id
// GET /api/run-table-acc-csv-id?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
// Dimension(s) returned as enum id's.
func runTableAccIdCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doTableGetCsvHandler(w, r, true, false, false, isBom)
}

// runTableAccIdCsvBomGetHandler read table accumultor(s) values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/acc/csv-id-bom
// Dimension(s) returned as enum id's.
// Response starts from utf-8 BOM bytes.
func runTableAccIdCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetCsvHandler(w, r, true, false, false, true)
}

// runTableAllAccCsvGetHandler read table "all-accumulators" values
// from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/all-acc/csv
// GET /api/run-table-all-acc-csv?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
// Dimension(s) returned as enum codes.
func runTableAllAccCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doTableGetCsvHandler(w, r, true, true, true, isBom)
}

// runTableAllAccCsvBomGetHandler read table "all-accumulators" values
// from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/all-acc/csv-bom
// Dimension(s) returned as enum codes.
// Response starts from utf-8 BOM bytes.
func runTableAllAccCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetCsvHandler(w, r, true, true, true, true)
}

// runTableAllAccIdCsvGetHandler read table "all-accumulators" values
// from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id
// GET /api/run-table-all-acc-csv-id?model=modelNameOrDigest&run=runNameOrDigest&name=tableName&bom=true
// Dimension(s) returned as enum id's.
func runTableAllAccIdCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	isBom, _ := getBoolRequestParam(r, "bom") // is utf-8 bom required
	doTableGetCsvHandler(w, r, true, true, false, isBom)
}

// runTableAllAccIdCsvBomGetHandler read table "all-accumulators" values
// from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id-bom
// Dimension(s) returned as enum id's.
// Response starts from utf-8 BOM bytes.
func runTableAllAccIdCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableGetCsvHandler(w, r, true, true, false, true)
}

// doTableGetCsvHandler read output table expression, accumulator or "all-accumulator" values
// from model run and write it as csv response.
// It does read all output table values, not a "page" of values.
// Dimension(s) and enum-based parameters returned as enum codes or enum id's.
func doTableGetCsvHandler(w http.ResponseWriter, r *http.Request, isAcc, isAllAcc, isCode, isBom bool) {

	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	rdn := getRequestParam(r, "run")   //  run digest-or-name
	name := getRequestParam(r, "name") // parameter name

	// read output table values, page size =0: read all values
	layout := db.ReadTableLayout{
		ReadLayout: db.ReadLayout{Name: name},
		IsAccum:    isAcc,
		IsAllAccum: isAllAcc,
	}

	cLst, _, ok := theCatalog.ReadOutTable(dn, rdn, &layout)
	if !ok {
		http.Error(w, "Error at run output table read "+rdn+": "+name, http.StatusBadRequest)
		return
	}

	// get converter from cell list to csv rows []string
	hdr, cvt, ok := theCatalog.TableToCsvConverter(dn, isCode, name, layout.IsAccum, layout.IsAllAccum)
	if !ok {
		http.Error(w, "Failed to create output table csv converter: "+name, http.StatusBadRequest)
		return
	}

	// convert to csv and write into http response
	writeCsvResponse(w, name, rdn, isBom, hdr, cLst, cvt)
}

// writeCsvResponse convert csv parameter or output table values and write it into http response
func writeCsvResponse(
	w http.ResponseWriter, name, src string, isBom bool, hdr []string, cLst *list.List, cvt func(interface{}, []string) error,
) {
	// calculate Content-Length: start from BOM length, if BOM required
	nb := 0
	if isBom {
		nb += len(helper.Utf8bom)
	}

	// Content-Length: csv header length
	for k := range hdr {
		nb += len(hdr[k]) + 1
	}

	// add each csv line length: comma-separated and lf as eol
	cs := append([]string{}, hdr...)

	for c := cLst.Front(); c != nil; c = c.Next() {
		if err := cvt(c.Value, cs); err != nil {
			omppLog.Log("Error at convert cell to csv: ", name, ": ", err.Error())
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
			http.Error(w, "Error at csv write: "+src+": "+name, http.StatusBadRequest)
			return
		}
	}

	csvWr := csv.NewWriter(w)

	if err := csvWr.Write(hdr); err != nil {
		http.Error(w, "Error at csv write: "+src+": "+name, http.StatusBadRequest)
		return
	}

	for c := cLst.Front(); c != nil; c = c.Next() {
		if err := cvt(c.Value, cs); err != nil {
			http.Error(w, "Error at convert cell value of: "+src+": "+name, http.StatusBadRequest)
			return
		}
		if err := csvWr.Write(cs); err != nil {
			http.Error(w, "Error at csv write: "+src+": "+name, http.StatusBadRequest)
			return
		}
	}
	csvWr.Flush() // flush csv to response
}
