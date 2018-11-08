// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// ParameterCellConverter return parameter value converter between code cell and id's cell.
// If isToId true then from code to id cell else other way around
func (mc *ModelCatalog) ParameterCellConverter(
	isToId bool, dn, src string, name string,
) (
	func(interface{}) (interface{}, error), bool,
) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return nil, false
	}

	// load model metadata and return index in model catalog
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	// lock catalog and search model parameter by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	if _, ok = mc.modelLst[idx].meta.ParamByName(name); !ok {
		omppLog.Log("Error: model parameter not found: ", dn, ": ", name)
		return nil, false
	}

	// create converter
	var cvt func(interface{}) (interface{}, error)
	var err error

	if isToId {
		var cell db.CellCodeParam
		cvt, err = cell.CodeToIdCell(mc.modelLst[idx].meta, name)
	} else {
		var cell db.CellParam
		cvt, err = cell.IdToCodeCell(mc.modelLst[idx].meta, name)
	}
	if err != nil {
		omppLog.Log("Failed to create parameter cell value converter: ", name, ": ", err.Error())
		return nil, false
	}

	return cvt, true
}

// TableToCodeCellConverter return output table value converter from id's cell into code cell.
func (mc *ModelCatalog) TableToCodeCellConverter(
	dn string, name string, isAcc, isAllAcc bool,
) (
	func(interface{}) (interface{}, error), bool,
) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return nil, false
	}

	// load model metadata and return index in model catalog
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	// lock catalog and search model output table by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	if _, ok = mc.modelLst[idx].meta.OutTableByName(name); !ok {
		omppLog.Log("Error: model output table not found: ", dn, ": ", name)
		return nil, false
	}

	// create converter
	var cvt func(interface{}) (interface{}, error)
	var err error

	switch {
	case isAllAcc:
		var cell db.CellAllAcc
		cvt, err = cell.IdToCodeCell(mc.modelLst[idx].meta, name)
	case isAcc:
		var cell db.CellAcc
		cvt, err = cell.IdToCodeCell(mc.modelLst[idx].meta, name)
	default:
		var cell db.CellExpr
		cvt, err = cell.IdToCodeCell(mc.modelLst[idx].meta, name)
	}
	if err != nil {
		omppLog.Log("Failed to create output table cell id's to code converter: ", name, ": ", err.Error())
		return nil, false
	}

	return cvt, true
}

// ParameterToCsvConverter return parameter csv converter and csv header as string array.
func (mc *ModelCatalog) ParameterToCsvConverter(
	dn string, isCode bool, name string,
) (
	[]string, func(interface{}, []string) error, bool,
) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []string{}, nil, false
	}

	// load model metadata and return index in model catalog
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []string{}, nil, false // return empty result: model not found or error
	}

	// lock catalog and search model parameter by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	if _, ok = mc.modelLst[idx].meta.ParamByName(name); !ok {
		omppLog.Log("Error: model parameter not found: ", dn, ": ", name)
		return []string{}, nil, false // return empty result: parameter not found or error
	}

	// make csv header
	var cell db.CellParam

	hdr, err := cell.CsvHeader(mc.modelLst[idx].meta, name, !isCode, "")
	if err != nil {
		omppLog.Log("Failed  to make parameter csv header: ", dn, ": ", name, ": ", err.Error())
		return []string{}, nil, false
	}

	// create converter from db cell into csv row []string
	var cvt func(interface{}, []string) error

	if isCode {
		cvt, err = cell.CsvToRow(mc.modelLst[idx].meta, name, doubleFmt, "")
	} else {
		cvt, err = cell.CsvToIdRow(mc.modelLst[idx].meta, name, doubleFmt, "")
	}
	if err != nil {
		omppLog.Log("Failed to create parameter converter to csv: ", dn, ": ", name, ": ", err.Error())
		return []string{}, nil, false
	}

	return hdr, cvt, true
}

// TableToCsvConverter return output table cell to csv converter and csv header as staring array.
func (mc *ModelCatalog) TableToCsvConverter(
	dn string, isCode bool, name string, isAcc, isAllAcc bool,
) (
	[]string, func(interface{}, []string) error, bool,
) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []string{}, nil, false
	}

	// load model metadata and return index in model catalog
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []string{}, nil, false // return empty result: model not found or error
	}

	// lock catalog and search model output table by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	if _, ok = mc.modelLst[idx].meta.OutTableByName(name); !ok {
		omppLog.Log("Error: model output table not found: ", dn, ": ", name)
		return []string{}, nil, false // return empty result: output table not found or error
	}

	// set cell conveter to csv
	var cell db.CsvConverter
	var ec db.CellExpr
	var ac db.CellAcc
	var alc db.CellAllAcc
	if !isAcc {
		cell = ec
	} else {
		if !isAllAcc {
			cell = ac
		} else {
			cell = alc
		}
	}

	// make csv header
	hdr, err := cell.CsvHeader(mc.modelLst[idx].meta, name, !isCode, "")
	if err != nil {
		omppLog.Log("Failed  to make output table csv header: ", dn, ": ", name, ": ", err.Error())
		return []string{}, nil, false
	}

	// create converter from db cell into csv row []string
	var cvt func(interface{}, []string) error

	if isCode {
		cvt, err = cell.CsvToRow(mc.modelLst[idx].meta, name, doubleFmt, "")
	} else {
		cvt, err = cell.CsvToIdRow(mc.modelLst[idx].meta, name, doubleFmt, "")
	}
	if err != nil {
		omppLog.Log("Failed to create output table converter to csv: ", dn, ": ", name, ": ", err.Error())
		return []string{}, nil, false
	}

	return hdr, cvt, true
}
