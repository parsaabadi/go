// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
	"golang.org/x/text/language"
)

// RunStatusByDigestOrName return run_lst db row by model digest-or-name and run digest-or-name.
func (mc *ModelCatalog) RunStatusByDigestOrName(dn, rdn string) (*db.RunRow, bool) {

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
	rst, err := db.GetRunByDigest(mc.modelLst[idx].dbConn, rdn)
	if err != nil {
		omppLog.Log("Error at get run status: ", dn, ": ", rdn, ": ", err.Error())
		return &db.RunRow{}, false // return empty result: run select error
	}
	if rst == nil {
		rst, err = db.GetRunByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdn)
		if err != nil {
			omppLog.Log("Error at get run status: ", dn, ": ", rdn, ": ", err.Error())
			return &db.RunRow{}, false // return empty result: run select error
		}
	}
	if rst == nil {
		omppLog.Log("Warning run status not found: ", dn, ": ", rdn)
		return &db.RunRow{}, false // return empty result: run_lst row not found
	}

	return rst, true
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
		omppLog.Log("Warning: there is no run status not found for the model: ", dn)
		return &db.RunRow{}, false // return empty result: run_lst row not found
	}

	return rst, true
}

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
		return &db.RunPub{}, false // return empty result: run select error
	}

	return rp, true
}
