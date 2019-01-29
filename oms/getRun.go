// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
	"golang.org/x/text/language"
)

// RunStatusByDigestOrName return run_lst db row by model digest-or-name and run digest-or-name.
func (mc *ModelCatalog) RunStatus(dn, rdsn string) (*db.RunPub, bool) {

	// if model digest-or-name or run digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.RunPub{}, false
	}
	if rdsn == "" {
		omppLog.Log("Warning: invalid (empty) run digest or stamp or name")
		return &db.RunPub{}, false
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.RunPub{}, false // return empty result: model not found or error
	}

	// get run_lst db row by digest, stamp or run name
	r, err := db.GetRunByDigestOrStampOrName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdsn)
	if err != nil {
		omppLog.Log("Error at get run status: ", dn, ": ", rdsn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run select error
	}
	if r == nil {
		omppLog.Log("Warning run status not found: ", dn, ": ", rdsn)
		return &db.RunPub{}, false // return empty result: run_lst row not found
	}

	// get run sub-values progress for that run id
	rpRs, err := db.GetRunProgress(mc.modelLst[idx].dbConn, r.RunId)
	if err != nil {
		omppLog.Log("Error at get run progress: ", dn, ": ", rdsn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run progress select error
	}

	// convert to "public" format
	rp, err := (&db.RunMeta{Run: *r, Progress: rpRs}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at run status conversion: ", dn, ": ", rdsn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result
	}

	return rp, true
}

// FirstOrLastRunStatus return first or last or last completed run_lst db row and run_progress db rows by model digest-or-name.
func (mc *ModelCatalog) FirstOrLastRunStatus(dn string, isFisrt, isCompleted bool) (*db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.RunPub{}, false
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.RunPub{}, false // return empty result: model not found or error
	}

	// get first or last or last completed run_lst db row
	rst := &db.RunRow{}
	var err error

	if isFisrt {
		rst, err = db.GetFirstRun(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	} else {
		if !isCompleted {
			rst, err = db.GetLastRun(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
		} else {
			rst, err = db.GetLastCompletedRun(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
		}
	}
	if err != nil {
		omppLog.Log("Error at get run status: ", dn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run select error
	}
	if rst == nil {
		// omppLog.Log("Warning: there is no run status not found for the model: ", dn)
		return &db.RunPub{}, false // return empty result: run_lst row not found
	}

	// get run sub-values progress for that run id
	rpRs, err := db.GetRunProgress(mc.modelLst[idx].dbConn, rst.RunId)
	if err != nil {
		omppLog.Log("Error at get run progress: ", dn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run progress select error
	}

	// convert to "public" format
	rp, err := (&db.RunMeta{Run: *rst, Progress: rpRs}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at run status conversion: ", dn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result
	}

	return rp, true
}

// LastCompletedRunText return last compeleted run_lst and run_txt db rows by model digest-or-name.
// Run completed if run status one of: s=success, x=exit, e=error.
// Text (description and notes) can be in prefered language or all languages.
// If prefered language requested and it is not found in db then return empty text results.
func (mc *ModelCatalog) LastCompletedRunText(dn string, isAllLang bool, preferedLang []language.Tag) (*db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.RunPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
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

	// get run_txt db row for that run using matched prefered language or all languages
	lc := ""
	if !isAllLang {
		_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
		lc = mc.modelLst[idx].langCodes[np]
	}

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

// RunList return list of run_lst db rows by model digest-or-name.
// No text info returned (no description and notes).
func (mc *ModelCatalog) RunList(dn string) ([]db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []db.RunPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []db.RunPub{}, false // return empty result: model not found or error
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	rl, err := db.GetRunList(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	if err != nil {
		omppLog.Log("Error at get run list: ", dn, ": ", err.Error())
		return []db.RunPub{}, false // return empty result: run select error
	}
	if len(rl) <= 0 {
		return []db.RunPub{}, true // return empty result: run_lst rows not found for that model
	}

	// for each run_lst convert it to "public" run format
	rpl := make([]db.RunPub, len(rl))

	for ni := range rl {

		p, err := (&db.RunMeta{Run: rl[ni]}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
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

// RunListText return list of run_lst and run_txt db rows by model digest-or-name.
// Text (description and notes) are in prefered language or if text in such language exists.
func (mc *ModelCatalog) RunListText(dn string, preferedLang []language.Tag) ([]db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []db.RunPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []db.RunPub{}, false // return empty result: model not found or error
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// get run_txt db row for each run_lst using matched prefered language
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langCodes[np]

	rl, rt, err := db.GetRunListText(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, lc)
	if err != nil {
		omppLog.Log("Error at get run list: ", dn, ": ", err.Error())
		return []db.RunPub{}, false // return empty result: run select error
	}
	if len(rl) <= 0 {
		return []db.RunPub{}, true // return empty result: run_lst rows not found for that model
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

// RunTextFull return full run metadata by model digest-or-name and run digest-or-stamp-name.
// It does not return non-completed runs (run in progress).
// Run completed if run status one of: s=success, x=exit, e=error.
// Text (description and notes) can be in prefered language or all languages.
// If prefered language requested and it is not found in db then return empty text results.
func (mc *ModelCatalog) RunTextFull(dn, rdsn string, isAllLang bool, preferedLang []language.Tag) (*db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.RunPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.RunPub{}, false // return empty result: model not found or error
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// get run_lst db row by digest, stamp or run name
	r, err := db.GetRunByDigestOrStampOrName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdsn)
	if err != nil {
		omppLog.Log("Error at get run status: ", dn, ": ", rdsn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run select error
	}
	if r == nil {
		omppLog.Log("Warning run status not found: ", dn, ": ", rdsn)
		return &db.RunPub{}, false // return empty result: run_lst row not found
	}
	if !db.IsRunCompleted(r.Status) {
		omppLog.Log("Warning run is not completed: ", dn, ": ", rdsn, ": ", r.Status)
		return &db.RunPub{}, false // return empty result: run not completed
	}

	// get full metadata db rows using matched prefered language or in all languages
	lc := ""
	if !isAllLang {
		_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
		lc = mc.modelLst[idx].langCodes[np]
	}

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
