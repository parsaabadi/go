// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/csv"
	"net/http"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
)

// worksetParameterCsvGetHandler read a parameter values from workset and write it as csv response.
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
	src := getRequestParam(r, srcArg)  // workset name or run digest-or-stamp-or-name
	name := getRequestParam(r, "name") // parameter name

	// read parameter values, page size =0: read all values
	layout := db.ReadParamLayout{
		ReadLayout: db.ReadLayout{Name: name}, IsFromSet: isSet,
	}

	// get converter from cell list to csv rows []string
	hdr, cvtRow, ok := theCatalog.ParameterToCsvConverter(dn, isCode, name)
	if !ok {
		http.Error(w, "Failed to create parameter csv converter "+src+": "+name, http.StatusBadRequest)
		return
	}

	// set response headers: Content-Disposition: attachment; filename=name.csv
	csvSetHeaders(w, name)

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

	// convert output table cell into []string and write line into csv file
	cs := make([]string, len(hdr))

	cvtWr := func(c interface{}) (bool, error) {

		if err := cvtRow(c, cs); err != nil {
			return false, err
		}
		if err := csvWr.Write(cs); err != nil {
			return false, err
		}
		return true, nil
	}

	_, ok = theCatalog.ReadParameterTo(dn, src, &layout, cvtWr)
	if !ok {
		http.Error(w, "Error at parameter read "+src+": "+name, http.StatusBadRequest)
		return
	}
	csvWr.Flush() // flush csv to response
}

// runTableExprCsvGetHandler read table expression(s) values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/table/:name/expr/csv
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
	rdsn := getRequestParam(r, "run")  // run digest-or-stamp-or-name
	name := getRequestParam(r, "name") // parameter name

	// read output table values, page size =0: read all values
	layout := db.ReadTableLayout{
		ReadLayout: db.ReadLayout{Name: name},
		IsAccum:    isAcc,
		IsAllAccum: isAllAcc,
	}

	// get converter from cell list to csv rows []string
	hdr, cvtRow, ok := theCatalog.TableToCsvConverter(dn, isCode, name, layout.IsAccum, layout.IsAllAccum)
	if !ok {
		http.Error(w, "Failed to create output table csv converter: "+name, http.StatusBadRequest)
		return
	}

	// set response headers: Content-Disposition: attachment; filename=name.csv
	fn := name
	if isAcc {
		if isAllAcc {
			fn += ".acc-all"
		} else {
			fn += ".acc"
		}
	}
	csvSetHeaders(w, fn)

	// write csv body
	if isBom {
		if _, err := w.Write(helper.Utf8bom); err != nil {
			http.Error(w, "Error at csv write: "+rdsn+": "+name, http.StatusBadRequest)
			return
		}
	}

	csvWr := csv.NewWriter(w)

	if err := csvWr.Write(hdr); err != nil {
		http.Error(w, "Error at csv write: "+rdsn+": "+name, http.StatusBadRequest)
		return
	}

	// convert output table cell into []string and write line into csv file
	cs := make([]string, len(hdr))

	cvtWr := func(c interface{}) (bool, error) {

		if err := cvtRow(c, cs); err != nil {
			return false, err
		}
		if err := csvWr.Write(cs); err != nil {
			return false, err
		}
		return true, nil
	}
	_, ok = theCatalog.ReadOutTableTo(dn, rdsn, &layout, cvtWr)
	if !ok {
		http.Error(w, "Error at run output table read "+rdsn+": "+name, http.StatusBadRequest)
		return
	}
	csvWr.Flush() // flush csv to response
}
