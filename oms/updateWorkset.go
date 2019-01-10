// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"encoding/csv"
	"errors"
	"io"

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

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return "", &db.WorksetRow{}, false // return empty result: model not found or error
	}

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

// UpdateWorkset update workset metadata: create new workset, replace existsing or merge metadata.
func (mc *ModelCatalog) UpdateWorkset(isReplace bool, wp *db.WorksetPub) (bool, bool, error) {

	// if model digest-or-name or workset name is empty then return empty results
	dn := wp.ModelDigest
	if dn == "" {
		dn = wp.ModelName
	}
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false, false, nil
	}
	if wp.Name == "" {
		omppLog.Log("Warning: invalid (empty) workset name")
		return false, false, nil
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Error: model digest or name not found: ", dn)
		return false, false, errors.New("Error: model digest or name not found: " + dn)
	}

	// lock catalog and update workset
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// find workset in database: it must be read-write if exists
	w, err := db.GetWorksetByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, wp.Name)
	if err != nil {
		omppLog.Log("Error at get workset status: ", dn, ": ", wp.Name, ": ", err.Error())
		return false, false, err
	}
	if w != nil && w.IsReadonly {
		omppLog.Log("Failed to update read-only workset: ", dn, ": ", wp.Name)
		return false, false, errors.New("Failed to update read-only workset: " + dn + ": " + wp.Name)
	}

	// if workset does not exist then clean paramters list to create empty workset
	isEraseParam := w == nil && len(wp.Param) > 0
	if isEraseParam {
		wp.Param = []db.ParamRunSetPub{}
		omppLog.Log("Warning: existing workset not found, create new empty workset (without parameters): ", wp.Name)
	}

	// convert workset from "public" into db rows
	wm, err := wp.FromPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at workset json conversion: ", dn, ": ", wp.Name, ": ", err.Error())
		return false, isEraseParam, err
	}

	// update workset metadata
	err = wm.UpdateWorkset(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, isReplace, mc.modelLst[idx].langMeta)
	if err != nil {
		omppLog.Log("Error at update workset: ", dn, ": ", wp.Name, ": ", err.Error())
		return false, isEraseParam, err
	}

	return true, isEraseParam, nil
}

// DeleteWorkset do delete workset, including parameter values from database.
func (mc *ModelCatalog) DeleteWorkset(dn, wsn string) (bool, error) {

	// if model digest-or-name or workset name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false, nil
	}
	if wsn == "" {
		omppLog.Log("Warning: invalid (empty) workset name")
		return false, nil
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, nil // return empty result: model not found or error
	}

	// lock catalog and delete workset
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// find workset in database
	w, err := db.GetWorksetByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, wsn)
	if err != nil {
		omppLog.Log("Error at get workset status: ", dn, ": ", wsn, ": ", err.Error())
		return false, err
	}
	if w == nil {
		return false, nil // return OK: workset not found
	}

	// delete workset from database
	err = db.DeleteWorkset(mc.modelLst[idx].dbConn, w.SetId)
	if err != nil {
		omppLog.Log("Error at delete workset: ", dn, ": ", wsn, ": ", err.Error())
		return false, err
	}

	return true, nil
}

// UpdateWorksetParameter replace or merge parameter metadata into workset and replace parameter values from csv reader.
func (mc *ModelCatalog) UpdateWorksetParameter(
	isReplace bool, wp *db.WorksetPub, param *db.ParamRunSetPub, csvRd *csv.Reader,
) (
	bool, error) {

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
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Error: model digest or name not found: ", dn)
		return false, errors.New("Error: model digest or name not found: " + dn)
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

	// if csv file exist then read csv file and convert and append lines into cell list
	cLst := list.New()

	if csvRd != nil {

		// converter from csv row []string to db cell
		var cell db.CellParam
		cvt, err := cell.CsvToCell(mc.modelLst[idx].meta, param.Name, param.SubCount, "")
		if err != nil {
			return false, errors.New("invalid converter from csv row: " + err.Error())
		}

		isFirst := true
	ReadFor:
		for {
			row, err := csvRd.Read()
			switch {
			case err == io.EOF:
				break ReadFor
			case err != nil:
				return false, errors.New("Failed to read csv parameter values " + param.Name)
			}

			// skip header line
			if isFirst {
				isFirst = false
				continue
			}

			// convert and append cell to cell list
			c, err := cvt(row)
			if err != nil {
				return false, errors.New("Failed to convert csv parameter values " + param.Name)
			}
			cLst.PushBack(c)
		}
		if cLst.Len() <= 0 {
			return false, errors.New("workset: " + wp.Name + " parameter empty: " + param.Name)
		}
	}

	// update workset parameter metadata and parameter values
	hId, err := wm.UpdateWorksetParameter(
		mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, isReplace, param, cLst, mc.modelLst[idx].langMeta)
	if err != nil {
		omppLog.Log("Error at update workset: ", dn, ": ", wp.Name, ": ", err.Error())
		return false, err
	}
	return hId > 0, nil // return success and true if parameter was found
}

// DeleteWorksetParameter do delete workset parameter metadata and values from database.
func (mc *ModelCatalog) DeleteWorksetParameter(dn, wsn, name string) (bool, error) {

	// if model digest-or-name or workset name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false, nil
	}
	if wsn == "" {
		omppLog.Log("Warning: invalid (empty) workset name")
		return false, nil
	}
	if name == "" {
		omppLog.Log("Warning: invalid (empty) workset parameter name")
		return false, nil
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, nil // return empty result: model not found or error
	}

	// lock catalog and update workset
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// delete workset from database
	hId, err := db.DeleteWorksetParameter(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, wsn, name)
	if err != nil {
		omppLog.Log("Error at update workset: ", dn, ": ", wsn, ": ", err.Error())
		return false, err
	}
	return hId > 0, nil // return success and true if parameter was found
}

// UpdateWorksetParameterPage merge "page" of parameter values into workset.
// Parameter must be already in workset and identified by model digest-or-name, set name, parameter name.
func (mc *ModelCatalog) UpdateWorksetParameterPage(dn, wsn, name string, cellLst *list.List) error {

	// if model digest-or-name, set name or paramete name is empty then return empty results
	if dn == "" {
		return errors.New("Invalid (empty) model digest and name")
	}
	if wsn == "" {
		return errors.New("Invalid (empty) workset name. Model: " + dn)
	}
	if name == "" {
		return errors.New("Invalid (empty) parameter name. Model: " + dn + " workset: " + wsn)
	}

	// load model metadata and return index in model catalog
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		return errors.New("Model digest or name not found: " + dn)
	}

	// lock catalog and search model parameter by name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	pHid := 0
	if k, ok := mc.modelLst[idx].meta.ParamByName(name); ok {
		pHid = mc.modelLst[idx].meta.Param[k].ParamHid
	} else {
		return errors.New("Parameter " + name + " not found in model: " + dn)
	}

	// find workset id by name
	wst, ok := mc.loadWorksetByName(idx, wsn)
	if !ok {
		return errors.New("Workset " + wsn + " not found in model: " + dn)
	}
	layout := db.WriteParamLayout{
		WriteLayout: db.WriteLayout{Name: name, ToId: wst.SetId},
		IsToRun:     false,
		IsPage:      true,
		DoubleFmt:   doubleFmt,
	}

	// parameter must be in workset already
	hIds, nSubs, err := db.GetWorksetParamList(mc.modelLst[idx].dbConn, wst.SetId)
	if err != nil {
		return err
	}
	for k := range hIds {
		ok = hIds[k] == pHid
		if ok {
			layout.SubCount = nSubs[k]
			break
		}
	}
	if !ok {
		return errors.New("Workset: " + wsn + " must contain parameter: " + name)
	}

	// write parameter values
	return db.WriteParameter(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, &layout, cellLst)
}

// CopyParameterToWsFromRun copy parameter metadata and values into workset from model run.
// If parameter already exist in destination workset then error returned.
// Destination workset must be in read-write state.
// Source model run must be completed, run status one of: s=success, x=exit, e=error.
func (mc *ModelCatalog) CopyParameterToWsFromRun(dn, wsn, name, rdn string) error {

	// validate parameters
	if dn == "" {
		return errors.New("Parameter copy failed: invalid (empty) model digest and name")
	}
	if wsn == "" {
		return errors.New("Parameter copy failed: invalid (empty) workset name")
	}
	if name == "" {
		return errors.New("Parameter copy failed: invalid (empty) parameter name")
	}
	if rdn == "" {
		return errors.New("Parameter copy failed: invalid (empty) model run name")
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		return errors.New("Model digest or name not found: " + dn)
	}

	// lock catalog to update workset
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// search model parameter by name
	pHid := 0
	if k, ok := mc.modelLst[idx].meta.ParamByName(name); ok {
		pHid = mc.modelLst[idx].meta.Param[k].ParamHid
	} else {
		return errors.New("Parameter " + name + " not found in model: " + dn)
	}

	// find workset by name: it must be read-write
	wst, ok := mc.loadWorksetByName(idx, wsn)
	if !ok || wst == nil {
		return errors.New("Workset not found or error at get workset status: " + dn + ": " + wsn)
	}
	if wst.IsReadonly {
		return errors.New("Parameter copy failed, destination workset is read-only: " + wsn + ": " + name)
	}

	// if parameter already in the workset then return error
	hIds, _, err := db.GetWorksetParamList(mc.modelLst[idx].dbConn, wst.SetId)
	if err != nil {
		return errors.New("Error at getting workset parameters list: " + wsn + ": " + err.Error())
	}
	for _, h := range hIds {
		if h == pHid {
			return errors.New("Parameter copy failed, workset already contains parameter: " + wsn + ": " + name)
		}
	}

	// find run by digest or name: it must be completed
	rst, ok := mc.loadCompletedRunByDigestOrName(idx, rdn)
	if !ok || rst == nil {
		return errors.New("Model not found or not completed: " + dn + ": " + rdn)
	}

	// copy parameter into workset from model run
	err = db.CopyParameterFromRun(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, wst, name, rst)
	if err != nil {
		return errors.New("Parameter copy failed: " + wsn + ": " + name + ": " + err.Error())
	}
	return nil
}

// CopyParameterBetweenWs copy parameter metadata and values into workset from other workset.
// If parameter already exist in destination workset then error returned.
// Destination workset must be in read-write state, source workset must be read-only.
// Source model run must be completed, run status one of: s=success, x=exit, e=error.
func (mc *ModelCatalog) CopyParameterBetweenWs(dn, dstWsName, name, srcWsName string) error {

	// validate parameters
	if dn == "" {
		return errors.New("Parameter copy failed: invalid (empty) model digest and name")
	}
	if dstWsName == "" {
		return errors.New("Parameter copy failed: invalid (empty) destination workset name")
	}
	if name == "" {
		return errors.New("Parameter copy failed: invalid (empty) parameter name")
	}
	if srcWsName == "" {
		return errors.New("Parameter copy failed: invalid (empty) source workset name")
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		return errors.New("Model digest or name not found: " + dn)
	}

	// lock catalog to update workset
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// search model parameter by name
	pHid := 0
	if k, ok := mc.modelLst[idx].meta.ParamByName(name); ok {
		pHid = mc.modelLst[idx].meta.Param[k].ParamHid
	} else {
		return errors.New("Parameter " + name + " not found in model: " + dn)
	}

	// find destination workset by name: it must be read-write
	dstWs, ok := mc.loadWorksetByName(idx, dstWsName)
	if !ok || dstWs == nil {
		return errors.New("Workset not found or error at get workset status: " + dn + ": " + dstWsName)
	}
	if dstWs.IsReadonly {
		return errors.New("Parameter copy failed, destination workset is read-only: " + dstWsName + ": " + name)
	}

	// if parameter already in the workset then return error
	hIds, _, err := db.GetWorksetParamList(mc.modelLst[idx].dbConn, dstWs.SetId)
	if err != nil {
		return errors.New("Error at getting workset parameters list: " + dstWsName + ": " + err.Error())
	}
	for _, h := range hIds {
		if h == pHid {
			return errors.New("Parameter copy failed, workset already contains parameter: " + dstWsName + ": " + name)
		}
	}

	// find source workset by name: it must be read-only
	srcWs, ok := mc.loadWorksetByName(idx, srcWsName)
	if !ok || srcWs == nil {
		return errors.New("Workset not found or error at get workset status: " + dn + ": " + srcWsName)
	}
	if !srcWs.IsReadonly {
		return errors.New("Parameter copy failed, source workset must be read-only: " + srcWsName + ": " + name)
	}

	// copy parameter from one workset to another
	err = db.CopyParameterFromWorkset(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, dstWs, name, srcWs)
	if err != nil {
		return errors.New("Parameter copy failed: " + dstWsName + ": " + name + ": " + err.Error())
	}
	return nil
}
