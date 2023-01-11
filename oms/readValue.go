// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// ReadParameterTo select "page" of parameter values from workset or model run and pass each row into cvtWr().
// Parameter identified by model digest-or-name, run digest-or-stamp-or-name or set name, parameter name.
// Page of values is a rows from parameter value table started at zero based offset row
// and up to max page size rows, if page size <= 0 then all values returned.
// Parameter values can be read-only (select from run or read-only workset) or read-write (read-write workset).
// Rows can be filtered and ordered (see db.ReadParamLayout for details).
func (mc *ModelCatalog) ReadParameterTo(dn, src string, layout *db.ReadParamLayout, cvtWr func(src interface{}) (bool, error)) (*db.ReadPageLayout, bool) {

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

		// get run_lst db row by digest, stamp or run name
		rst, err := db.GetRunByDigestOrStampOrName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, src)
		if err != nil {
			omppLog.Log("Error at get run status: ", mc.modelLst[idx].meta.Model.Name, ": ", src, ": ", err.Error())
			return nil, false // return empty result: run select error
		}
		if rst == nil {
			omppLog.Log("Warning: run not found: ", mc.modelLst[idx].meta.Model.Name, ": ", src)
			return nil, false // return empty result: run_lst row not found
		}

		layout.FromId = rst.RunId // source run id
	}

	// read parameter page
	lt, err := db.ReadParameterTo(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, layout, cvtWr)
	if err != nil {
		omppLog.Log("Error at read parameter: ", dn, ": ", layout.Name, ": ", err.Error())
		return nil, false // return empty result: values select error
	}

	return lt, true
}

// ReadOutTableTo select "page" of output table values from model run and pass each row into cvtWr().
// Output table identified by model digest-or-name, run digest-or-stamp-or-name and output table name.
// Page of values is a rows from output table expression or accumulator table or all accumulators view.
// Page started at zero based offset row and up to max page size rows, if page size <= 0 then all values returned.
// Values can be from expression table, accumulator table or "all accumulators" view.
// Rows can be filtered and ordered (see db.ReadTableLayout for details).
func (mc *ModelCatalog) ReadOutTableTo(dn, rdsn string, layout *db.ReadTableLayout, cvtWr func(src interface{}) (bool, error)) (*db.ReadPageLayout, bool) {

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

	if _, ok = mc.modelLst[idx].meta.OutTableByName(layout.Name); !ok {
		omppLog.Log("Warning: output table not found: ", layout.Name)
		return nil, false // return empty result: output table not found or error
	}

	// find model run id by digest-or-stamp-or-name
	rst, ok := mc.loadCompletedRunByDigestOrStampOrName(idx, rdsn)
	if !ok {
		return nil, false // return empty result: run select error
	}
	layout.FromId = rst.RunId // source run id

	// read output table page
	lt, err := db.ReadOutputTableTo(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, layout, cvtWr)
	if err != nil {
		omppLog.Log("Error at read output table: ", dn, ": ", layout.Name, ": ", err.Error())
		return nil, false // return empty result: values select error
	}

	return lt, true
}

// ReadMicrodataTo select "page" of microdata values from model run and pass each row into cvtWr().
// Microdata identified by model digest-or-name, run digest-or-stamp-or-name and entity name.
// Page of values is a rows from microdata value table started at zero based offset row
// and up to max page size rows, if page size <= 0 then all values returned.
// Rows can be filtered and ordered (see db.ReadMicroLayout for details).
func (mc *ModelCatalog) ReadMicrodataTo(dn, rdsn string, layout *db.ReadMicroLayout, cvtWr func(src interface{}) (bool, error)) (*db.ReadPageLayout, bool) {

	// validate parameters and return empty results on empty input
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return nil, false
	}
	if layout.Name == "" {
		omppLog.Log("Warning: invalid (empty) model entity name")
		return nil, false
	}

	// load model metadata and return index in model catalog
	if _, ok := mc.loadModelMeta(dn); !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	// lock catalog and search model entity by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	if _, ok := mc.modelLst[idx].meta.EntityByName(layout.Name); !ok {
		omppLog.Log("Warning: model entity not found: ", layout.Name)
		return nil, false
	}

	// if run id not defiened then find model run id by digest-or-stamp-or-name
	if layout.FromId <= 0 {

		rst, ok := mc.loadCompletedRunByDigestOrStampOrName(idx, rdsn)
		if !ok {
			return nil, false // return empty result: run select error
		}
		layout.FromId = rst.RunId // source run id
	}

	// if generation digest undefined then find entity generation by entity name and run id
	if layout.GenDigest == "" {

		entGen, ok := mc.loadEntityGenByName(idx, layout.FromId, layout.Name)
		if !ok {
			return nil, false // entity generation not found
		}
		layout.GenDigest = entGen.GenDigest
	}

	// read microdata values page
	lt, err := db.ReadMicrodataTo(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, layout, cvtWr)
	if err != nil {
		omppLog.Log("Error at read microdata: ", dn, ": ", layout.Name, ": ", layout.GenDigest, ": ", err.Error())
		return nil, false // return empty result: values select error
	}

	return lt, true
}
