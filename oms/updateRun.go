// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// DeleteRun do delete  model run including output table values and run input parameters.
func (mc *ModelCatalog) DeleteRun(dn, rdn string) (bool, error) {

	// if model digest-or-name or run digest-or-name name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false, nil
	}
	if rdn == "" {
		omppLog.Log("Warning: invalid (empty) model run digest and name")
		return false, nil
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, nil // return empty result: model not found or error
	}

	// lock catalog and delete model run
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// find model run by digest or if not found then by name
	r, err := db.GetRunByDigest(mc.modelLst[idx].dbConn, rdn)
	if err == nil && r == nil {
		r, err = db.GetRunByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdn)
	}
	if err != nil {
		omppLog.Log("Error at get model run: ", dn, ": ", rdn, ": ", err.Error())
		return false, err
	}
	if r == nil {
		return false, nil // return OK: model run not found
	}

	// delete run from database
	err = db.DeleteRun(mc.modelLst[idx].dbConn, r.RunId)
	if err != nil {
		omppLog.Log("Error at delete model run: ", dn, ": ", rdn, ": ", err.Error())
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

	rdn := rp.Digest
	if rdn == "" {
		rdn = rp.Name
	}
	if rdn == "" {
		omppLog.Log("Warning: invalid (empty) model run digest and name")
		return false, "", "", nil
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, dn, rdn, nil // return empty result: model not found or error
	}

	// lock catalog and update model run
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// find model run by digest or if not found then by name
	r, err := db.GetRunByDigest(mc.modelLst[idx].dbConn, rdn)
	if err == nil && r == nil {
		r, err = db.GetRunByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, rdn)
	}
	if err != nil {
		omppLog.Log("Error at get model run: ", dn, ": ", rdn, ": ", err.Error())
		return false, dn, rdn, err
	}
	if r == nil {
		return false, dn, rdn, nil // return OK: model run not found
	}

	// validate: model run must be completed
	if !db.IsRunCompleted(r.Status) {
		omppLog.Log("Failed to update model run, it is not completed: ", dn, ": ", rdn)
		return false, dn, rdn, errors.New("Failed to update model run, it is not completed: " + dn + ": " + rdn)
	}

	// convert run from "public" into db rows
	rm, err := rp.FromPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at model run conversion: ", dn, ": ", rdn, ": ", err.Error())
		return false, dn, rdn, err
	}

	// update model run text and run parameter notes
	err = rm.UpdateRunText(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, r.RunId, mc.modelLst[idx].langMeta)
	if err != nil {
		omppLog.Log("Error at update model run: ", dn, ": ", rdn, ": ", err.Error())
		return false, dn, rdn, err
	}

	return true, dn, rdn, nil
}
