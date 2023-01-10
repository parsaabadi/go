// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// ParameterCellConverter return parameter value converter between code cell and id's cell.
// If isToId true then from code to id cell else other way around
func (mc *ModelCatalog) ParameterCellConverter(
	isToId bool, dn string, name string,
) (
	func(interface{}) (interface{}, error), bool,
) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return nil, false
	}

	// load model metadata and return index in model catalog
	if _, ok := mc.loadModelMeta(dn); !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	// lock catalog and search model parameter by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	if _, ok = mc.modelLst[idx].meta.ParamByName(name); !ok {
		omppLog.Log("Error: model parameter not found: ", dn, ": ", name)
		return nil, false
	}

	// create converter
	csvCvt := db.CellParamConverter{
		ModelDef:  mc.modelLst[idx].meta,
		Name:      name,
		DoubleFmt: theCfg.doubleFmt,
	}
	var cvt func(interface{}) (interface{}, error)
	var err error

	if isToId {
		cvt, err = csvCvt.CodeToIdCell(mc.modelLst[idx].meta, name)
	} else {
		cvt, err = csvCvt.IdToCodeCell(mc.modelLst[idx].meta, name)
	}
	if err != nil {
		omppLog.Log("Failed to create parameter cell value converter: ", name, ": ", err.Error())
		return nil, false
	}

	return cvt, true
}

// TableToCodeCellConverter return output table value converter from id's cell into code cell.
func (mc *ModelCatalog) TableToCodeCellConverter(dn string, name string, isAcc, isAllAcc bool) (func(interface{}) (interface{}, error), bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return nil, false
	}

	// load model metadata and return index in model catalog
	if _, ok := mc.loadModelMeta(dn); !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	// lock catalog and search model output table by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	if _, ok = mc.modelLst[idx].meta.OutTableByName(name); !ok {
		omppLog.Log("Error: model output table not found: ", dn, ": ", name)
		return nil, false
	}

	// create converter
	ctc := db.CellTableConverter{
		ModelDef: mc.modelLst[idx].meta,
		Name:     name,
	}
	var cvt func(interface{}) (interface{}, error)
	var err error

	switch {
	case isAcc && isAllAcc:
		csvCvt := db.CellAllAccConverter{
			CellTableConverter: ctc,
			IsIdCsv:            true,
			DoubleFmt:          theCfg.doubleFmt,
			ValueName:          "",
		}
		cvt, err = csvCvt.IdToCodeCell(mc.modelLst[idx].meta, name)
	case isAcc:
		csvCvt := db.CellAccConverter{
			CellTableConverter: ctc,
			IsIdCsv:            true,
			DoubleFmt:          theCfg.doubleFmt,
		}
		cvt, err = csvCvt.IdToCodeCell(mc.modelLst[idx].meta, name)
	default:
		csvCvt := db.CellExprConverter{
			CellTableConverter: ctc,
			IsIdCsv:            true,
			DoubleFmt:          theCfg.doubleFmt,
		}
		cvt, err = csvCvt.IdToCodeCell(mc.modelLst[idx].meta, name)
	}
	if err != nil {
		omppLog.Log("Failed to create output table cell id's to code converter: ", name, ": ", err.Error())
		return nil, false
	}

	return cvt, true
}

// ParameterToCsvConverter return parameter csv converter and csv header as string array.
func (mc *ModelCatalog) ParameterToCsvConverter(dn string, isCode bool, name string) ([]string, func(interface{}, []string) error, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []string{}, nil, false
	}

	// load model metadata and return index in model catalog
	if _, ok := mc.loadModelMeta(dn); !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []string{}, nil, false // return empty result: model not found or error
	}

	// lock catalog and search model parameter by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []string{}, nil, false // return empty result: model not found or error
	}

	if _, ok = mc.modelLst[idx].meta.ParamByName(name); !ok {
		omppLog.Log("Error: model parameter not found: ", dn, ": ", name)
		return []string{}, nil, false // return empty result: parameter not found or error
	}

	// make csv header
	csvCvt := db.CellParamConverter{
		ModelDef:  mc.modelLst[idx].meta,
		Name:      name,
		IsIdCsv:   !isCode,
		DoubleFmt: theCfg.doubleFmt,
	}

	hdr, err := csvCvt.CsvHeader()
	if err != nil {
		omppLog.Log("Failed to make parameter csv header: ", dn, ": ", name, ": ", err.Error())
		return []string{}, nil, false
	}

	// create converter from db cell into csv row []string
	var cvt func(interface{}, []string) error

	if isCode {
		cvt, err = csvCvt.ToCsvRow()
	} else {
		cvt, err = csvCvt.ToCsvIdRow()
	}
	if err != nil {
		omppLog.Log("Failed to create parameter converter to csv: ", dn, ": ", name, ": ", err.Error())
		return []string{}, nil, false
	}

	return hdr, cvt, true
}

// TableToCsvConverter return output table cell to csv converter and csv header as starting array.
func (mc *ModelCatalog) TableToCsvConverter(dn string, isCode bool, name string, isAcc, isAllAcc bool) ([]string, func(interface{}, []string) error, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []string{}, nil, false
	}

	// load model metadata and return index in model catalog
	if _, ok := mc.loadModelMeta(dn); !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []string{}, nil, false // return empty result: model not found or error
	}

	// lock catalog and search model output table by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []string{}, nil, false // return empty result: model not found or error
	}

	if _, ok = mc.modelLst[idx].meta.OutTableByName(name); !ok {
		omppLog.Log("Error: model output table not found: ", dn, ": ", name)
		return []string{}, nil, false // return empty result: output table not found or error
	}

	// set cell conveter to csv
	ctc := db.CellTableConverter{
		ModelDef: mc.modelLst[idx].meta,
		Name:     name,
	}
	var csvCvt db.CsvConverter

	switch {
	case isAcc && isAllAcc:
		csvCvt = &db.CellAllAccConverter{
			CellTableConverter: ctc,
			IsIdCsv:            !isCode,
			DoubleFmt:          theCfg.doubleFmt,
			ValueName:          "",
		}
	case isAcc:
		csvCvt = &db.CellAccConverter{
			CellTableConverter: ctc,
			IsIdCsv:            !isCode,
			DoubleFmt:          theCfg.doubleFmt,
		}
	default:
		csvCvt = &db.CellExprConverter{
			CellTableConverter: ctc,
			IsIdCsv:            !isCode,
			DoubleFmt:          theCfg.doubleFmt,
		}
	}

	// make csv header
	hdr, err := csvCvt.CsvHeader()
	if err != nil {
		omppLog.Log("Failed to make output table csv header: ", dn, ": ", name, ": ", err.Error())
		return []string{}, nil, false
	}

	// create converter from db cell into csv row []string
	var cvt func(interface{}, []string) error

	if isCode {
		cvt, err = csvCvt.ToCsvRow()
	} else {
		cvt, err = csvCvt.ToCsvIdRow()
	}
	if err != nil {
		omppLog.Log("Failed to create output table converter to csv: ", dn, ": ", name, ": ", err.Error())
		return []string{}, nil, false
	}

	return hdr, cvt, true
}

// MicrodataToCsvConverter return model run id, entity generation digest, microdata cell to csv converter and csv header as starting array.
func (mc *ModelCatalog) MicrodataToCsvConverter(dn string, isCode bool, rdsn, name string) (int, string, []string, func(interface{}, []string) error, bool) {

	// validate parameters and return empty results on empty input
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return 0, "", []string{}, nil, false
	}
	if rdsn == "" {
		omppLog.Log("Warning: invalid (empty) model run digest, stamp and name")
		return 0, "", []string{}, nil, false
	}
	if name == "" {
		omppLog.Log("Warning: invalid (empty) model entity name")
		return 0, "", []string{}, nil, false
	}

	// load model metadata and return index in model catalog
	if _, ok := mc.loadModelMeta(dn); !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return 0, "", []string{}, nil, false // return empty result: model not found or error
	}

	// lock catalog and search model output table by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return 0, "", []string{}, nil, false // return empty result: model not found or error
	}

	// find model entity by entity name
	eIdx, ok := mc.modelLst[idx].meta.EntityByName(name)
	if !ok {
		omppLog.Log("Warning: model entity not found: ", name)
		return 0, "", []string{}, nil, false
	}
	ent := &mc.modelLst[idx].meta.Entity[eIdx]

	// get run_lst db row by digest, stamp or run name
	r, err := db.GetRunByDigestOrStampOrName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdsn)
	if err != nil {
		omppLog.Log("Error at get run status: ", dn, ": ", rdsn, ": ", err.Error())
		return 0, "", []string{}, nil, false // return empty result: run select error
	}
	if r == nil {
		return 0, "", []string{}, nil, false // return empty result: run_lst row not found
	}
	if !db.IsRunCompleted(r.Status) {
		omppLog.Log("Warning: run is not completed: ", rdsn, ": ", r.Status)
		return r.RunId, "", []string{}, nil, false
	}

	// find entity generation by entity name
	egLst, err := db.GetEntityGenList(mc.modelLst[idx].dbConn, r.RunId)
	if err != nil {
		omppLog.Log("Error at get run entities: ", dn, ": ", rdsn, ": ", err.Error())
		return r.RunId, "", []string{}, nil, false
	}

	gIdx := -1
	for k := range egLst {

		if egLst[k].EntityId == ent.EntityId {
			gIdx = k
			break
		}
	}
	if gIdx < 0 {
		omppLog.Log("Error: model run entity generation not found: ", name, ": ", dn, ": ", rdsn)
		return r.RunId, "", []string{}, nil, false
	}
	entGen := egLst[gIdx]

	// make csv header
	cvtMicro := &db.CellMicroConverter{
		ModelDef:  mc.modelLst[idx].meta,
		Name:      name,
		EntityGen: &entGen,
		IsIdCsv:   !isCode,
		DoubleFmt: theCfg.doubleFmt,
	}

	hdr, err := cvtMicro.CsvHeader()
	if err != nil {
		omppLog.Log("Failed to make microdata csv header: ", dn, ": ", name, ": ", err.Error())
		return r.RunId, "", []string{}, nil, false
	}

	// create converter from db cell into csv row []string
	var cvt func(interface{}, []string) error

	if isCode {
		cvt, err = cvtMicro.ToCsvRow()
	} else {
		cvt, err = cvtMicro.ToCsvIdRow()
	}
	if err != nil {
		omppLog.Log("Failed to create microdata converter to csv: ", dn, ": ", name, ": ", err.Error())
		return r.RunId, "", []string{}, nil, false
	}

	return r.RunId, entGen.GenDigest, hdr, cvt, true
}
