// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
	"golang.org/x/text/language"
)

// RunStatusByDigestOrName return run_lst db row by model digest-or-name and run digest-or-name.
func (mc *ModelCatalog) RunStatus(dn, rdn string) (*db.RunRow, bool) {

	// if model digest-or-name or run digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.RunRow{}, false
	}
	if rdn == "" {
		omppLog.Log("Warning: invalid (empty) run digest and name")
		return &db.RunRow{}, false
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.RunRow{}, false // return empty result: model not found or error
	}

	// get run_lst db row by digest or run name
	r, err := db.GetRunByDigest(mc.modelLst[idx].dbConn, rdn)
	if err == nil && r == nil {
		r, err = db.GetRunByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdn)
	}
	if err != nil {
		omppLog.Log("Error at get run status: ", dn, ": ", rdn, ": ", err.Error())
		return &db.RunRow{}, false // return empty result: run select error
	}
	if r == nil {
		omppLog.Log("Warning run status not found: ", dn, ": ", rdn)
		return &db.RunRow{}, false // return empty result: run_lst row not found
	}

	return r, true
}

// FirstOrLastRunStatus return first or last run_lst db row by model digest-or-name.
func (mc *ModelCatalog) FirstOrLastRunStatus(dn string, isFisrt bool) (*db.RunRow, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.RunRow{}, false
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.RunRow{}, false // return empty result: model not found or error
	}

	// get first or last run_lst db row
	rst := &db.RunRow{}
	var err error

	if isFisrt {
		rst, err = db.GetFirstRun(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	} else {
		rst, err = db.GetLastRun(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	}

	if err != nil {
		omppLog.Log("Error at get run status: ", dn, ": ", err.Error())
		return &db.RunRow{}, false // return empty result: run select error
	}
	if rst == nil {
		// omppLog.Log("Warning: there is no run status not found for the model: ", dn)
		return &db.RunRow{}, false // return empty result: run_lst row not found
	}

	return rst, true
}

// LastCompletedRunText return last compeleted run_lst and run_txt db rows by model digest-or-name.
// Run completed if run status one of: s=success, x=exit, e=error.
// Text (description and notes) are in prefered language or empty if text in such language exists.
func (mc *ModelCatalog) LastCompletedRunText(dn string, preferedLang []language.Tag) (*db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.RunPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.RunPub{}, false // return empty result: model not found or error
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// get last completed run_lst db row
	r, err := db.GetLastCompletedRun(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	if err != nil {
		omppLog.Log("Error at get last completed run: ", dn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run select error
	}
	if r == nil {
		// omppLog.Log("Warning: there is no completed run not found for the model: ", dn)
		return &db.RunPub{}, false // return empty result: run_lst row not found
	}

	// get run_txt db row for that run using matched prefered languag
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langLst[np].LangCode

	rt, err := db.GetRunText(mc.modelLst[idx].dbConn, r.RunId, lc)
	if err != nil {
		omppLog.Log("Error at get run text of last completed run: ", dn, ": ", r.RunId, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run select error
	}

	// convert to "public" model run format
	rp, err := (&db.RunMeta{Run: *r, Txt: rt}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at last completed run conversion: ", dn, ": ", r.Name, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: conversion error
	}

	return rp, true
}

// RunTextList return list of run_lst and run_txt db rows by model digest-or-name.
// Text (description and notes) are in prefered language or empty if text in such language exists.
func (mc *ModelCatalog) RunListText(dn string, preferedLang []language.Tag) ([]db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []db.RunPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []db.RunPub{}, false // return empty result: model not found or error
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// get run_txt db row for each run_lst using matched prefered languag
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langLst[np].LangCode

	rl, rt, err := db.GetRunList(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, lc)
	if err != nil {
		omppLog.Log("Error at get run list: ", dn, ": ", err.Error())
		return []db.RunPub{}, false // return empty result: run select error
	}
	if len(rl) <= 0 {
		// omppLog.Log("Warning: there is no runs found for the model: ", dn)
		return []db.RunPub{}, false // return empty result: run_lst rows not found for that model
	}

	// for each run_lst find run_txt row if exist and convert to "public" run format
	rpl := make([]db.RunPub, len(rl))

	nt := 0
	for ni := range rl {

		// find text row for current master row by run id
		isFound := false
		for ; nt < len(rt); nt++ {
			isFound = rt[nt].RunId == rl[ni].RunId
			if rt[nt].RunId >= rl[ni].RunId {
				break // text found or text missing: text run id ahead of master run id
			}
		}

		// convert to "public" format
		var p *db.RunPub
		var err error

		if isFound && nt < len(rt) {
			p, err = (&db.RunMeta{Run: rl[ni], Txt: []db.RunTxtRow{rt[nt]}}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		} else {
			p, err = (&db.RunMeta{Run: rl[ni]}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		}
		if err != nil {
			omppLog.Log("Error at run conversion: ", dn, ": ", err.Error())
			return []db.RunPub{}, false // return empty result: conversion error
		}
		if p != nil {
			rpl[ni] = *p
		}
	}

	return rpl, true
}

// RunTextFull return full run metadata by model digest-or-name and digest-or-name.
// It does not return non-completed runs (run in progress).
// Run completed if run status one of: s=success, x=exit, e=error.
// Text (description and notes) are in prefered language or empty if text in such language exists.
func (mc *ModelCatalog) RunTextFull(dn, rdn string, preferedLang []language.Tag) (*db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.RunPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.RunPub{}, false // return empty result: model not found or error
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// get run_lst db row by digest or run name
	r, err := db.GetRunByDigest(mc.modelLst[idx].dbConn, rdn)
	if err == nil && r == nil {
		r, err = db.GetRunByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdn)
	}
	if err != nil {
		omppLog.Log("Error at get run status: ", dn, ": ", rdn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run select error
	}
	if r == nil {
		omppLog.Log("Warning run status not found: ", dn, ": ", rdn)
		return &db.RunPub{}, false // return empty result: run_lst row not found
	}
	if r.Status != db.DoneRunStatus && r.Status != db.ErrorRunStatus && r.Status != db.ExitRunStatus {
		omppLog.Log("Warning run is not completed: ", dn, ": ", rdn, ": ", r.Status)
		return &db.RunPub{}, false // return empty result: run not completed
	}

	// get full metadata db rows using matched prefered languag
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langLst[np].LangCode

	rm, err := db.GetRunFull(mc.modelLst[idx].dbConn, r, lc)
	if err != nil {
		omppLog.Log("Error at get run text: ", dn, ": ", r.Name, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run select error
	}

	// convert to "public" model run format
	rp, err := rm.ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at completed run conversion: ", dn, ": ", r.Name, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: conversion error
	}

	return rp, true
}
