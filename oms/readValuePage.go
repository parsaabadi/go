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

	// convert from id list into code list
	if isCode {
		if ok := mc.convertToCellCodeList(mc.modelLst[idx].meta, layout.Name, cLst); !ok {
			omppLog.Log("Failed to parameter value cells: ", dn, ": ", layout.Name, ": ", err.Error())
			return nil, false // fail to convert from id cell list into code cell list
		}
	}

	return cLst, true
}

// ReadOutTable return "page" of output table values from model run.
// Output table identified by model digest-or-name, run digest-or-name and output table name.
// Page of values is a rows from output table expression or accumulator table or all accumulators view.
// Page started at zero based offset row and up to max page size rows, if page size <= 0 then all values returned.
// Values can be from expression table, accumulator table or "all accumulators" view.
// Rows can be filtered and ordered (see db.ReadOutTableLayout for details).
func (mc *ModelCatalog) ReadOutTable(dn, src string, isCode bool, layout *db.ReadOutTableLayout) (*list.List, bool) {

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

	// convert from id list into code list
	if isCode {
		if ok := mc.convertToCellCodeList(mc.modelLst[idx].meta, layout.Name, cLst); !ok {
			omppLog.Log("Failed to convert table value cells: ", dn, ": ", layout.Name, ": ", err.Error())
			return nil, false // fail to convert from id cell list into code cell list
		}
	}

	return cLst, true
}

// convertToCellCodeList for eact cell list element convert it from id's cell into code cell.
// Parameter and output table values selected into list of id cells where dimension items are enum id's.
// Code cell contain enum codes for enum-based dimensions and string values of simple type dimensions.
// If this is enum type parameter then parameter value converted from id to enum code.
// It can be used only inside of lock.
func (mc *ModelCatalog) convertToCellCodeList(meta *db.ModelMeta, name string, cellLst *list.List) bool {

	if meta == nil {
		omppLog.Log("Invalid (empty) model metadata, fail to create cell value converter: ", name)
		return false // error: no model metadata or no cell list
	}
	if cellLst == nil || cellLst.Len() <= 0 {
		return true // exit on emprty list: nothing to do
	}

	// create converter
	var cvt func(interface{}) (interface{}, error)
	var err error

	if cellLst.Front() != nil {
		if cv, ok := cellLst.Front().Value.(db.CellToCodeConverter); ok {
			cvt, err = cv.IdToCodeCell(meta, name)
		}
	}
	if err != nil {
		omppLog.Log("Failed to create cell value converter: ", name, ": ", err.Error())
		return false // error: fail to get converter, may be front element of the list is not a cell
	}
	if cvt == nil {
		omppLog.Log("Invalid cell value list or front element: ", name)
		return false // error: front element of the list is not a cell
	}

	// convert all list elements from id's to code
	for el := cellLst.Front(); el != nil; el = el.Next() {

		c, err := cvt(el.Value) // convert from id's to codes
		if err != nil {
			omppLog.Log("Error at convert cell value of: ", name, ": ", err.Error())
			return false // error at conversion
		}
		el.Value = c
	}
	return true
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
