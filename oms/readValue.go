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

		if rst, ok := mc.loadCompletedRunByDigestOrStampOrName(idx, src); ok {
			layout.FromId = rst.RunId // source run id
		} else {
			return nil, false // return empty result: run select error
		}
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
func (mc *ModelCatalog) ReadOutTableTo(dn, src string, layout *db.ReadTableLayout, cvtWr func(src interface{}) (bool, error)) (*db.ReadPageLayout, bool) {

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
	rst, ok := mc.loadCompletedRunByDigestOrStampOrName(idx, src)
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
