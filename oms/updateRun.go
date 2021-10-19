// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// DeleteRun do delete  model run including output table values and run input parameters.
func (mc *ModelCatalog) DeleteRun(dn, rdsn string) (bool, error) {

	// if model digest-or-name or run digest-or-name name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false, nil
	}
	if rdsn == "" {
		omppLog.Log("Warning: invalid (empty) model run digest, stamp and name")
		return false, nil
	}

	// if model metadata not loaded then read it from database
	if _, ok := mc.loadModelMeta(dn); !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, nil // return empty result: model not found or error
	}

	// lock catalog and delete model run
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, nil // return empty result: model not found or error
	}

	// find model run by digest, stamp or run name
	r, err := db.GetRunByDigestOrStampOrName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdsn)
	if err != nil {
		omppLog.Log("Error at get model run: ", dn, ": ", rdsn, ": ", err.Error())
		return false, err
	}
	if r == nil {
		return false, nil // return OK: model run not found
	}

	// delete run from database
	err = db.DeleteRun(mc.modelLst[idx].dbConn, r.RunId)
	if err != nil {
		omppLog.Log("Error at delete model run: ", dn, ": ", rdsn, ": ", err.Error())
		return false, err
	}

	return true, nil
}

// UpdateRunText do merge run text (run description and notes) and run parameter notes.
func (mc *ModelCatalog) UpdateRunText(rp *db.RunPub) (bool, string, string, error) {

	// validate parameters
	if rp == nil {
		omppLog.Log("Error: invalid (empty) model run data")
		return false, "", "", errors.New("Error: invalid (empty) model run data")
	}

	// if model digest-or-name or run digest-or-name is empty then return empty results
	dn := rp.ModelDigest
	if dn == "" {
		dn = rp.ModelName
	}
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false, "", "", nil
	}

	rdsn := rp.RunDigest
	if rdsn == "" {
		rdsn = rp.RunStamp
	}
	if rdsn == "" {
		rdsn = rp.Name
	}
	if rdsn == "" {
		omppLog.Log("Warning: invalid (empty) model run digest, stamp and name")
		return false, "", "", nil
	}

	// if model metadata not loaded then read it from database
	if _, ok := mc.loadModelMeta(dn); !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, dn, rdsn, nil // return empty result: model not found or error
	}

	// lock catalog and update model run
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, dn, rdsn, nil // return empty result: model not found or error
	}

	// find model run by digest, stamp or run name
	r, err := db.GetRunByDigestOrStampOrName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdsn)
	if err != nil {
		omppLog.Log("Error at get model run: ", dn, ": ", rdsn, ": ", err.Error())
		return false, dn, rdsn, err
	}
	if r == nil {
		return false, dn, rdsn, nil // return OK: model run not found
	}

	// validate: model run must be completed
	if !db.IsRunCompleted(r.Status) {
		omppLog.Log("Failed to update model run, it is not completed: ", dn, ": ", rdsn)
		return false, dn, rdsn, errors.New("Failed to update model run, it is not completed: " + dn + ": " + rdsn)
	}

	// convert run from "public" into db rows
	rm, err := rp.FromPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at model run conversion: ", dn, ": ", rdsn, ": ", err.Error())
		return false, dn, rdsn, err
	}

	// match languages from request into model languages
	for k := range rm.Txt {
		lc := mc.languageMatch(idx, rm.Txt[k].LangCode)
		if lc != "" {
			rm.Txt[k].LangCode = lc
		}
	}
	for k := range rm.Param {
		for j := range rm.Param[k].Txt {
			lc := mc.languageMatch(idx, rm.Param[k].Txt[j].LangCode)
			if lc != "" {
				rm.Param[k].Txt[j].LangCode = lc
			}
		}
	}

	// update model run text and run parameter notes
	err = rm.UpdateRunText(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, r.RunId, mc.modelLst[idx].langMeta)
	if err != nil {
		omppLog.Log("Error at update model run: ", dn, ": ", rdsn, ": ", err.Error())
		return false, dn, rdsn, err
	}

	return true, dn, rdsn, nil
}

// UpdateRunParameterText do merge (insert or update) parameters run value notes.
func (mc *ModelCatalog) UpdateRunParameterText(dn, rdsn string, pvtLst []db.ParamRunSetTxtPub) (bool, error) {

	// validate parameters
	if pvtLst == nil || len(pvtLst) <= 0 {
		omppLog.Log("Warning: empty list of run parameters to update value notes")
		return false, nil
	}
	if dn == "" {
		return false, errors.New("Error: invalid (empty) model digest or name")
	}
	if rdsn == "" {
		return false, errors.New("Error: invalid (empty) model run digest or stamp or name")
	}

	// if model metadata not loaded then read it from database
	if _, ok := mc.loadModelMeta(dn); !ok {
		return false, errors.New("Error: model digest or name not found: " + dn)
	}

	// lock catalog and update model run
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		return false, errors.New("Error: model digest or name not found: " + dn)
	}

	// validate parameters by name: it must be model parameter
	for k := range pvtLst {
		if _, ok = mc.modelLst[idx].meta.ParamByName(pvtLst[k].Name); !ok {
			return false, errors.New("Model parameter not found: " + dn + ": " + pvtLst[k].Name)
		}
	}

	// find model run by digest, stamp or run name
	r, err := db.GetRunByDigestOrStampOrName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdsn)
	if err != nil {
		return false, errors.New("Model run not found: " + dn + ": " + rdsn + ": " + err.Error())
	}
	if r == nil {
		return false, errors.New("Model run not found: " + dn + ": " + rdsn)
	}

	// validate: model run must be completed
	if !db.IsRunCompleted(r.Status) {
		return false, errors.New("Failed to update model run, it is not completed: " + dn + ": " + rdsn)
	}

	// match languages from request into model languages
	for j := range pvtLst {
		for k := range pvtLst[j].Txt {
			lc := mc.languageMatch(idx, pvtLst[j].Txt[k].LangCode)
			if lc != "" {
				pvtLst[j].Txt[k].LangCode = lc
			}
		}
	}

	// update run parameter notes
	err = db.UpdateRunParameterText(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, r.RunId, pvtLst, mc.modelLst[idx].langMeta)
	if err != nil {
		return false, errors.New("Error at update run parameter notes: " + dn + ": " + rdsn + ": " + err.Error())
	}

	return true, nil
}
