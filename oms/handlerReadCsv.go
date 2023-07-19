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
	doParameterGetCsvHandler(w, r, "set", true, true, false)
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
	doParameterGetCsvHandler(w, r, "set", true, false, false)
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
	doParameterGetCsvHandler(w, r, "run", false, true, false)
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
	doParameterGetCsvHandler(w, r, "run", false, false, false)
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
	doTableGetCsvHandler(w, r, false, false, true, false)
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
	doTableGetCsvHandler(w, r, false, false, false, false)
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
	doTableGetCsvHandler(w, r, true, false, true, false)
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
	doTableGetCsvHandler(w, r, true, false, false, false)
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
	doTableGetCsvHandler(w, r, true, true, true, false)
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
	doTableGetCsvHandler(w, r, true, true, false, false)
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
	name := getRequestParam(r, "name") // output table name

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

// runTableCalcCsvGetHandler write into CSV response all output table expressions
// and for each expression additional measure: SUM AVG COUNT MIN MAX VAR SD SE CV.
// GET /api/model/:model/run/:run/table/:name/calc/:calc/csv
// Dimension(s) returned as enum codes.
func runTableCalcCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableCalcGetCsvHandler(w, r, true, false)
}

// runTableCalcCsvBomGetHandler write into CSV response all output table expressions
// and for each expression additional measure: SUM AVG COUNT MIN MAX VAR SD SE CV.
// GET /api/model/:model/run/:run/table/:name/calc/:calc/csv-bom
// Dimension(s) returned as enum codes.
// Response starts from utf-8 BOM bytes.
func runTableCalcCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableCalcGetCsvHandler(w, r, true, true)
}

// runTableCalcIdCsvGetHandler write into CSV response all output table expressions
// and for each expression additional measure: SUM AVG COUNT MIN MAX VAR SD SE CV.
// GET /api/model/:model/run/:run/table/:name/calc/:calc/csv-id
// Dimension(s) returned as enum id's.
func runTableCalcIdCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableCalcGetCsvHandler(w, r, false, false)
}

// runTableCalcIdCsvBomGetHandler write into CSV response all output table expressions
// and for each expression additional measure: SUM AVG COUNT MIN MAX VAR SD SE CV.
// GET /api/model/:model/run/:run/table/:name/calc/:calc/csv-id-bom
// Dimension(s) returned as enum id's.
// Response starts from utf-8 BOM bytes.
func runTableCalcIdCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doTableCalcGetCsvHandler(w, r, false, true)
}

// doTableCalcGetCsvHandler write into CSV response all output table expressions
// and for each expression additional measure: SUM AVG COUNT MIN MAX VAR SD SE CV.
// It does read all output table values, not a "page" of values.
// Dimension(s) and enum-based parameters returned as enum codes or enum id's.
func doTableCalcGetCsvHandler(w http.ResponseWriter, r *http.Request, isCode, isBom bool) {

	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	rdsn := getRequestParam(r, "run")  // run digest-or-stamp-or-name
	name := getRequestParam(r, "name") // output table name
	calc := getRequestParam(r, "calc") // calculation function name: sum avg count min max var sd se cv

	// setup read layout and calculate layout
	// page size =0, read all values
	tableLt := db.ReadTableLayout{
		ReadLayout: db.ReadLayout{Name: name},
	}

	calcLt, ok := theCatalog.TableAllExprCalculateLayout(dn, name, calc)
	if !ok {
		http.Error(w, "Invalid calculation expression "+calc, http.StatusBadRequest)
		return
	}

	// get converter from cell list to csv rows []string
	hdr, cvtRow, _, runIds, ok := theCatalog.TableToCalcCsvConverter(dn, rdsn, isCode, name, nil)
	if !ok {
		http.Error(w, "Failed to create output table csv converter: "+name, http.StatusBadRequest)
		return
	}

	// set response headers: Content-Disposition: attachment; filename=name.csv
	csvSetHeaders(w, name)

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

	_, ok = theCatalog.ReadOutTableCalculateTo(dn, rdsn, &tableLt, calcLt, runIds, cvtWr)
	if !ok {
		http.Error(w, "Error at run output table read "+rdsn+": "+name, http.StatusBadRequest)
		return
	}
	csvWr.Flush() // flush csv to response
}

// runMicrodataCsvGetHandler read a microdata values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/microdata/:name/csv
// Enum-based microdata attributes returned as enum codes.
func runMicrodataCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	doMicrodataGetCsvHandler(w, r, true, false)
}

// runMicrodataCsvBomGetHandler read a microdata values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/microdata/:name/csv-bom
// Enum-based microdata attributes returned as enum codes.
// Response starts from utf-8 BOM bytes.
func runMicrodataCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doMicrodataGetCsvHandler(w, r, true, true)
}

// runMicrodataIdCsvGetHandler read a microdata values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/microdata/:name/csv-id
// Enum-based microdata attributes returned as enum id's.
func runMicrodataIdCsvGetHandler(w http.ResponseWriter, r *http.Request) {
	doMicrodataGetCsvHandler(w, r, false, false)
}

// runMicrodataIdCsvBomGetHandler read a microdata values from model run results and write it as csv response.
// GET /api/model/:model/run/:run/microdata/:name/csv-id-bom
// Enum-based microdata attributes returned as enum id's.
// Response starts from utf-8 BOM bytes.
func runMicrodataIdCsvBomGetHandler(w http.ResponseWriter, r *http.Request) {
	doMicrodataGetCsvHandler(w, r, false, true)
}

// doMicrodataGetCsvHandler read microdata values from model run and write it as csv response.
// It does read all microdata values, not a "page" of values.
// Enum-based microdata attributes returned as enum codes or enum id's.
func doMicrodataGetCsvHandler(w http.ResponseWriter, r *http.Request, isCode, isBom bool) {

	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	rdsn := getRequestParam(r, "run")  // run digest-or-stamp-or-name
	name := getRequestParam(r, "name") // entity name

	// return error if microdata disabled
	if !theCfg.isMicrodata {
		http.Error(w, "Error: microdata not allowed: "+dn+" "+rdsn, http.StatusBadRequest)
		return
	}

	// get converter from cell list to csv rows []string
	runId, genDigest, hdr, cvtRow, ok := theCatalog.MicrodataToCsvConverter(dn, isCode, rdsn, name)
	if !ok {
		http.Error(w, "Failed to create microdata csv converter: "+rdsn+": "+name, http.StatusBadRequest)
		return
	}

	// read microdata values, page size =0: read all values
	layout := db.ReadMicroLayout{
		ReadLayout: db.ReadLayout{
			Name:   name,
			FromId: runId,
		},
		GenDigest: genDigest,
	}

	// set response headers: Content-Disposition: attachment; filename=name.csv
	csvSetHeaders(w, name)

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

	_, ok = theCatalog.ReadMicrodataTo(dn, rdsn, &layout, cvtWr)
	if !ok {
		http.Error(w, "Error at microdata read: "+rdsn+": "+name, http.StatusBadRequest)
		return
	}
	csvWr.Flush() // flush csv to response
}
