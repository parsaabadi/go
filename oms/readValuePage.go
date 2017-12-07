// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// ReadParameter return "page" of parameter values from workset or model run.
// Parameter identified by model digest-or-name, run digest-or-name or set name, parameter name.
// Page of values is a rows from parameter value table started at zero based offset row
// and up to max page size rows, if page size <= 0 then all values returned.
// Parameter values can be read-only (select from run or read-only workset) or read-write (read-write workset).
// Rows can be filtered and ordered (see db.ReadParamLayout for details).
func (mc *ModelCatalog) ReadParameter(dn, src string, isCode bool, layout *db.ReadParamLayout) (*list.List, bool) {

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

	if _, ok = mc.modelLst[idx].meta.ParamByName(layout.Name); !ok {
		omppLog.Log("Warning: parameter not found: ", layout.Name)
		return nil, false // return empty result: parameter not found or error
	}

	// find workset id by name or run id by name-or-digest
	if layout.IsFromSet {

		if wst, ok := mc.loadWorksetByName(idx, src); ok {
			layout.FromId = wst.SetId // source workset id
		} else {
			return nil, false // return empty result: workset select error
		}
	} else {

		if rst, ok := mc.loadCompletedRunByDigestOrName(idx, src); ok {
			layout.FromId = rst.RunId // source run id
		} else {
			return nil, false // return empty result: run select error
		}
	}

	// read parameter page
	cLst, err := db.ReadParameter(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, layout)
	if err != nil || cLst == nil {
		omppLog.Log("Error at read parameter: ", dn, ": ", layout.Name, ": ", err.Error())
		return nil, false // return empty result: values select error
	}

	return cLst, true
}

// ReadOutTable return "page" of output table values from model run.
// Output table identified by model digest-or-name, run digest-or-name and output table name.
// Page of values is a rows from output table expression or accumulator table or all accumulators view.
// Page started at zero based offset row and up to max page size rows, if page size <= 0 then all values returned.
// Values can be from expression table, accumulator table or "all accumulators" view.
// Rows can be filtered and ordered (see db.ReadTableLayout for details).
func (mc *ModelCatalog) ReadOutTable(dn, src string, isCode bool, layout *db.ReadTableLayout) (*list.List, bool) {

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

	if _, ok = mc.modelLst[idx].meta.OutTableByName(layout.Name); !ok {
		omppLog.Log("Warning: output table not found: ", layout.Name)
		return nil, false // return empty result: output table not found or error
	}

	// find model run id by digest-or-name
	rst, ok := mc.loadCompletedRunByDigestOrName(idx, src)
	if !ok {
		return nil, false // return empty result: run select error
	}
	layout.FromId = rst.RunId // source run id

	// read output table page
	cLst, err := db.ReadOutputTable(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, layout)
	if err != nil || cLst == nil {
		omppLog.Log("Error at read output table: ", dn, ": ", layout.Name, ": ", err.Error())
		return nil, false // return empty result: values select error
	}

	return cLst, true
}

// loadWorksetByName select workset_lst db row by name and model index in model catalog.
// It can be used only inside of lock.
func (mc *ModelCatalog) loadWorksetByName(modelIdx int, wsn string) (*db.WorksetRow, bool) {

	if wsn == "" {
		omppLog.Log("Warning: invalid (empty) workset name")
		return nil, false
	}

	wst, err := db.GetWorksetByName(mc.modelLst[modelIdx].dbConn, mc.modelLst[modelIdx].meta.Model.ModelId, wsn)
	if err != nil {
		omppLog.Log("Workset not found or error at get workset status: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", wsn, ": ", err.Error())
		return nil, false // return empty result: workset select error
	}
	if wst == nil {
		omppLog.Log("Warning: workset not found: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", wsn)
		return nil, false // return empty result: workset_lst row not found
	}

	return wst, true
}

// loadCompletedRunByDigestOrName select run_lst db row by digest-or-name and model index in model catalog.
// Run must be completed, run status one of: s=success, x=exit, e=error.
// It can be used only inside of lock.
func (mc *ModelCatalog) loadCompletedRunByDigestOrName(modelIdx int, rdn string) (*db.RunRow, bool) {

	if rdn == "" {
		omppLog.Log("Warning: invalid (empty) run name or digest")
		return nil, false
	}

	// get run_lst db row by digest or run name
	rst, err := db.GetRunByDigest(mc.modelLst[modelIdx].dbConn, rdn)
	if err == nil && rst == nil {
		rst, err = db.GetRunByName(mc.modelLst[modelIdx].dbConn, mc.modelLst[modelIdx].meta.Model.ModelId, rdn)
	}
	if err != nil {
		omppLog.Log("Error at get run status: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", rdn, ": ", err.Error())
		return nil, false // return empty result: run select error
	}
	if rst == nil {
		omppLog.Log("Warning: run not found: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", rdn)
		return nil, false // return empty result: run_lst row not found
	}

	// run must be completed
	if !db.IsRunCompleted(rst.Status) {
		omppLog.Log("Warning: run is not completed: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", rdn, ": ", rst.Status)
		return nil, false // return empty result: run_lst row not found
	}

	return rst, true
}

// ParameterToCodeCellConverter return parameter value converter from id's cell into code cell.
func (mc *ModelCatalog) ParameterToCodeCellConverter(dn, src string, name string,
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
	var cell db.CellParam
	cvt, err := cell.IdToCodeCell(mc.modelLst[idx].meta, name)
	if err != nil {
		omppLog.Log("Failed to create parameter cell value converter: ", name, ": ", err.Error())
		return nil, false
	}

	return cvt, true
}

// TableToCodeCellConverter return output table value converter from id's cell into code cell.
func (mc *ModelCatalog) TableToCodeCellConverter(dn string, name string, isAcc, isAllAcc bool,
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

// ParameterToCsvConverter return parameter csv converter and csv header line.
func (mc *ModelCatalog) ParameterToCsvConverter(dn string, isCode bool, name string,
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
