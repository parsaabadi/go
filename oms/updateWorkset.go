// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// UpdateWorksetReadonly update workset read-only status by model digest-or-name and workset name.
func (mc *ModelCatalog) UpdateWorksetReadonly(dn, wsn string, isReadonly bool) (string, *db.WorksetRow, bool) {

	// if model digest-or-name or workset name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return "", &db.WorksetRow{}, false
	}
	if wsn == "" {
		omppLog.Log("Warning: invalid (empty) workset name")
		return "", &db.WorksetRow{}, false
	}

	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return "", &db.WorksetRow{}, false // return empty result: model not found or error
	}

	// lock catalog and if model metadata not loaded then read it from database
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// find workset in database
	w, err := db.GetWorksetByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, wsn)
	if err != nil {
		omppLog.Log("Error at get workset status: ", dn, ": ", wsn, ": ", err.Error())
		return "", &db.WorksetRow{}, false // return empty result: workset select error
	}
	if w == nil {
		omppLog.Log("Warning workset status not found: ", dn, ": ", wsn)
		return "", &db.WorksetRow{}, false // return empty result: workset_lst row not found
	}

	// update workset readonly status
	err = db.UpdateWorksetReadonly(mc.modelLst[idx].dbConn, w.SetId, isReadonly)
	if err != nil {
		omppLog.Log("Error at update workset status: ", dn, ": ", wsn, ": ", err.Error())
		return "", &db.WorksetRow{}, false // return empty result: workset select error
	}

	// get workset status
	w, err = db.GetWorkset(mc.modelLst[idx].dbConn, w.SetId)
	if err != nil {
		omppLog.Log("Error at get workset status: ", dn, ": ", w.SetId, ": ", err.Error())
		return "", &db.WorksetRow{}, false // return empty result: workset select error
	}
	if w == nil {
		omppLog.Log("Warning workset status not found: ", dn, ": ", wsn)
		return "", &db.WorksetRow{}, false // return empty result: workset_lst row not found
	}

	return mc.modelLst[idx].meta.Model.Digest, w, true
}

// UpdateWorkset update workset metadata or create new workset.
func (mc *ModelCatalog) UpdateWorkset(wp *db.WorksetPub) (bool, error) {

	// if model digest-or-name or workset name is empty then return empty results
	dn := wp.ModelDigest
	if dn == "" {
		dn = wp.ModelName
	}
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false, nil
	}
	if wp.Name == "" {
		omppLog.Log("Warning: invalid (empty) workset name")
		return false, nil
	}

	// if model metadata not loaded then read it from database
	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, nil // return empty result: model not found or error
	}

	// lock catalog and update workset
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// convert workset from "public" into db rows
	wm, err := wp.FromPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at workset json conversion: ", dn, ": ", wp.Name, ": ", err.Error())
		return false, err
	}

	// update workset metadata
	err = wm.UpdateWorkset(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, mc.modelLst[idx].langMeta)
	if err != nil {
		omppLog.Log("Error at update workset: ", dn, ": ", wp.Name, ": ", err.Error())
		return false, err // return empty result: workset select error
	}

	return true, nil
}
