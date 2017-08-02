// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// ReadParameter return "page" of parameter values from workset or model run.
// Parameter identified by model digest-or-name, run digest-or-name or set name, parameter digest-or-name.
// Page of values is a rows from parameter value table started at zero based offset row
// and up to max page size rows, if page size <= 0 then all values returned.
// Parameter values can be read-only (select from run or read-only workset) or read-write (read-write workset).
// Rows can be filtered and ordered (see db.ReadLayout for details).
func (mc *ModelCatalog) ReadParameter(dn, src string, layout *db.ReadLayout) (*list.List, bool) {

	// if model digest-or-name or workset name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return nil, false
	}

	// load model metadata in order to convert to "public"
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	// lock catalog and search model parameter by name or digest
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	pIdx, ok := mc.paramIndexByDigestOrName(idx, layout.Name)
	if !ok {
		omppLog.Log("Warning: parameter digest or name not found: ", layout.Name)
		return nil, false // return empty result: parameter not found or error
	}
	layout.Name = mc.modelLst[idx].meta.Param[pIdx].Name // parameter name

	// find model run or workset id
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
		return nil, false // return empty result: workset select error
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
		omppLog.Log("Warning workset status not found: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", wsn)
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
		omppLog.Log("Warning run status not found: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", rdn)
		return nil, false // return empty result: run_lst row not found
	}

	// run must be completed
	if !db.IsRunCompleted(rst.Status) {
		omppLog.Log("Warning run is not completed: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", rdn, ": ", rst.Status)
		return nil, false // return empty result: run_lst row not found
	}

	return rst, true
}
