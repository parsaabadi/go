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

	// get run_txt db row for that run
	// match prefered languages and model languages
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

	// get run_lst and run_txt db rows
	// match prefered languages and model languages
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

	ti := 0
	for ri := range rl {

		// find text row for current master ro by run id
		isFound := false
		for ; ti < len(rt); ti++ {
			isFound = rt[ti].RunId == rl[ri].RunId
			if rt[ti].RunId >= rl[ri].RunId {
				break // text found or text missing: text run id ahead of master run id
			}
		}

		// convert to "public" format
		var p *db.RunPub
		var err error

		if isFound && ti < len(rt) {
			p, err = (&db.RunMeta{Run: rl[ri], Txt: []db.RunTxtRow{rt[ti]}}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		} else {
			p, err = (&db.RunMeta{Run: rl[ri]}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		}
		if err != nil {
			omppLog.Log("Error at run conversion: ", dn, ": ", err.Error())
			return []db.RunPub{}, false // return empty result: conversion error
		}
		if p != nil {
			rpl[ri] = *p
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

	// get full metadata db rows
	// match prefered languages and model languages
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

/*
*
* Alterantive version: different text matching
*
// LastCompletedRunText return last compeleted run_lst and run_txt db rows by model digest-or-name.
// Run completed if run status one of: s=success, x=exit, e=error
// It can be in prefered language, default model language or empty if no completed runs exist.
func (mc *ModelCatalog) LastCompletedRunText(dn string, preferedLang []language.Tag) (*db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.RunPub{}, false
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.RunPub{}, false // return empty result: model not found or error
	}

	// get last completed run_lst db row
	r, err := db.GetLastCompletedRun(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	if err != nil {
		omppLog.Log("Error at get last completed run: ", dn, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run select error
	}
	if r == nil {
		omppLog.Log("Warning: there is no completed run not found for the model: ", dn)
		return &db.RunPub{}, false // return empty result: run_lst row not found
	}

	// get run_txt db row for that run
	rt, err := db.GetRunText(mc.modelLst[idx].dbConn, r.RunId, "")
	if err != nil {
		omppLog.Log("Error at get run text of last completed run: ", dn, ": ", r.RunId, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: run select error
	}

	// match prefered languages and model languages
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langLst[np].LangCode
	lcd := mc.modelLst[idx].meta.Model.DefaultLangCode

	rm := db.RunMeta{Run: *r}

	// set run_txt row by language
	if len(rt) > 0 {

		var nf, i int
		for ; i < len(rt); i++ {
			if rt[i].LangCode == lc {
				break // language match
			}
			if rt[i].LangCode == lcd {
				nf = i // index of default language
			}
		}
		if i >= len(rt) {
			i = nf // use default language or zero index row
		}
		rm.Txt = append(rm.Txt, rt[i])
	}

	// convert to "public" model run format
	rp, err := rm.ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at last completed run conversion: ", dn, ": ", r.Name, ": ", err.Error())
		return &db.RunPub{}, false // return empty result: conversion error
	}

	return rp, true
}

// RunTextList return list of run_lst and run_txt db rows by model digest-or-name.
// It can be in prefered language, default model language or empty if no completed runs exist.
func (mc *ModelCatalog) RunTextList(dn string, preferedLang []language.Tag) ([]db.RunPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []db.RunPub{}, false
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []db.RunPub{}, false // return empty result: model not found or error
	}

	// get run_lst and run_txt db rows
	rl, rt, err := db.GetRunList(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, "")
	if err != nil {
		omppLog.Log("Error at get run list: ", dn, ": ", err.Error())
		return []db.RunPub{}, false // return empty result: run select error
	}
	if len(rl) <= 0 {
		omppLog.Log("Warning: there is no runs found for the model: ", dn)
		return []db.RunPub{}, false // return empty result: run_lst rows not found for that model
	}

	// match prefered languages and model languages
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langLst[np].LangCode
	lcd := mc.modelLst[idx].meta.Model.DefaultLangCode

	// find run_txt row by language and convert to "public" run format
	rpl := make([]db.RunPub, len(rl))

	var isKey, isFound, isMatch bool
	var nf, ni, ri, ti int

	for ; ti < len(rt); ti++ {

		if ri >= len(rl) {
			break // done with master rows
		}

		// check if keys are equal
		isKey = rt[ti].RunId == rl[ri].RunId

		// start of next key: set "paublic" value
		if !isKey && isFound {

			if !isMatch { // if no match then use default
				ni = nf
			}

			p, err := (&db.RunMeta{Run: rl[ri], Txt: []db.RunTxtRow{rt[ni]}}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
			if err != nil {
				omppLog.Log("Error at run conversion: ", dn, ": ", err.Error())
				return []db.RunPub{}, false // return empty result: conversion error
			}
			rpl[ri] = *p

			// reset to start next search
			isFound = false
			isMatch = false
			ri++ // move to next master row
			ti-- // repeat current text row
			continue
		}

		// inside of key
		if isKey {

			if !isFound {
				isFound = true // first key found
				nf = ti
			}
			// match the language
			isMatch = rt[ti].LangCode == lc
			if isMatch {
				ni = ti // perefred language match
			}
			if rt[ti].LangCode == lcd {
				nf = ti // index of default language
			}
		}

		// if keys not equal and master key behind text key
		// then append "public" with empty text
		// and move to next master row and repeat current text row
		if !isKey && rt[ri].RunId > rl[ri].RunId {

			p, err := (&db.RunMeta{Run: rl[ri]}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
			if err != nil {
				omppLog.Log("Error at run conversion: ", dn, ": ", err.Error())
				return []db.RunPub{}, false // return empty result: conversion error
			}
			rpl[ri] = *p

			ri++ // move to master type
			ti-- // repeat current text row
			continue
		}
	} // for

	// last row found
	if isFound && ri < len(rl) {

		if !isMatch { // if no match then use default
			ni = nf
		}

		var p *db.RunPub
		var err error

		if ni < len(rt) {
			p, err = (&db.RunMeta{Run: rl[ri], Txt: []db.RunTxtRow{rt[ni]}}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		} else {
			p, err = (&db.RunMeta{Run: rl[ri]}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		}
		if err != nil {
			omppLog.Log("Error at run conversion: ", dn, ": ", err.Error())
			return []db.RunPub{}, false // return empty result: conversion error
		}
		if p != nil {
			rpl[ri] = *p
		}
		ri++ // next master row
	}

	// convert the rest of master rows to "public" with empty text
	for ; ri < len(rl); ri++ {
		p, err := (&db.RunMeta{Run: rl[ri]}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		if err != nil {
			omppLog.Log("Error at run conversion: ", dn, ": ", err.Error())
			return []db.RunPub{}, false // return empty result: conversion error
		}
		rpl[ri] = *p
	}

	return rpl, true
}
*/
