// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
	"golang.org/x/text/language"
)

// WorksetStatus return workset_lst db row by model digest-or-name and workset name.
func (mc *ModelCatalog) WorksetStatus(dn, name string) (*db.WorksetRow, bool) {

	// if model digest-or-name or workset name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.WorksetRow{}, false
	}
	if name == "" {
		omppLog.Log("Warning: invalid (empty) workset name")
		return &db.WorksetRow{}, false
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.WorksetRow{}, false // return empty result: model not found or error
	}

	// get workset_lst db row by name
	w, err := db.GetWorksetByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, name)
	if err != nil {
		omppLog.Log("Error at get workset status: ", dn, ": ", name, ": ", err.Error())
		return &db.WorksetRow{}, false // return empty result: workset select error
	}
	if w == nil {
		omppLog.Log("Warning workset status not found: ", dn, ": ", name)
		return &db.WorksetRow{}, false // return empty result: workset_lst row not found
	}

	return w, true
}

// WorksetDefaultStatus return workset_lst db row of default workset by model digest-or-name.
func (mc *ModelCatalog) WorksetDefaultStatus(dn string) (*db.WorksetRow, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.WorksetRow{}, false
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.WorksetRow{}, false // return empty result: model not found or error
	}

	// get workset_lst db row for default workset
	w, err := db.GetDefaultWorkset(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	if err != nil {
		omppLog.Log("Error at get default workset status: ", dn, ": ", err.Error())
		return &db.WorksetRow{}, false // return empty result: workset select error
	}
	if w == nil {
		omppLog.Log("Warning default workset status not found: ", dn)
		return &db.WorksetRow{}, false // return empty result: workset_lst row not found
	}

	return w, true
}

// WorksetList return list of workset_lst db rows by model digest-or-name.
// No text info returned (no description and notes).
func (mc *ModelCatalog) WorksetList(dn string) ([]db.WorksetPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []db.WorksetPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []db.WorksetPub{}, false // return empty result: model not found or error
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	wl, err := db.GetWorksetList(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	if err != nil {
		omppLog.Log("Error at get workset list: ", dn, ": ", err.Error())
		return []db.WorksetPub{}, false // return empty result: workset select error
	}
	if len(wl) <= 0 {
		// omppLog.Log("Warning: there is no any worksets found for the model: ", dn)
		return []db.WorksetPub{}, false // return empty result: workset_lst rows not found for that model
	}

	// for each workset_lst convert it to "public" workset format
	wpl := make([]db.WorksetPub, len(wl))

	for ni := range wl {

		p, err := (&db.WorksetMeta{Set: wl[ni]}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		if err != nil {
			omppLog.Log("Error at workset conversion: ", dn, ": ", err.Error())
			return []db.WorksetPub{}, false // return empty result: conversion error
		}
		if p != nil {
			wpl[ni] = *p
		}
	}

	return wpl, true
}

// WorksetListText return list of workset_lst and workset_txt db rows by model digest-or-name.
// Text (description and notes) are in prefered language or empty if text in such language exists.
func (mc *ModelCatalog) WorksetListText(dn string, preferedLang []language.Tag) ([]db.WorksetPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []db.WorksetPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []db.WorksetPub{}, false // return empty result: model not found or error
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// get workset_txt db row for each workset_lst using matched prefered language
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langLst[np].LangCode

	wl, wt, err := db.GetWorksetListText(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, lc)
	if err != nil {
		omppLog.Log("Error at get workset list: ", dn, ": ", err.Error())
		return []db.WorksetPub{}, false // return empty result: workset select error
	}
	if len(wl) <= 0 {
		// omppLog.Log("Warning: there is no any worksets found for the model: ", dn)
		return []db.WorksetPub{}, false // return empty result: workset_lst rows not found for that model
	}

	// for each workset_lst find workset_txt row if exist and convert to "public" workset format
	wpl := make([]db.WorksetPub, len(wl))

	nt := 0
	for ni := range wl {

		// find text row for current master row by set id
		isFound := false
		for ; nt < len(wt); nt++ {
			isFound = wt[nt].SetId == wl[ni].SetId
			if wt[nt].SetId >= wl[ni].SetId {
				break // text found or text missing: text set id ahead of master set id
			}
		}

		// convert to "public" format
		var p *db.WorksetPub
		var err error

		if isFound && nt < len(wt) {
			p, err = (&db.WorksetMeta{Set: wl[ni], Txt: []db.WorksetTxtRow{wt[nt]}}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		} else {
			p, err = (&db.WorksetMeta{Set: wl[ni]}).ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
		}
		if err != nil {
			omppLog.Log("Error at workset conversion: ", dn, ": ", err.Error())
			return []db.WorksetPub{}, false // return empty result: conversion error
		}
		if p != nil {
			wpl[ni] = *p
		}
	}

	return wpl, true
}

// WorksetText return full workset metadata by model digest-or-name and workset name.
// Text (description and notes) can be in prefered language or all languages.
// If prefered language requested and it is not found in db then return empty text results.
func (mc *ModelCatalog) WorksetText(dn, name string, isAllLang bool, preferedLang []language.Tag) (*db.WorksetPub, bool) {

	// if model digest-or-name or workset name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.WorksetPub{}, false
	}
	if name == "" {
		omppLog.Log("Warning: invalid (empty) workset name")
		return &db.WorksetPub{}, false
	}

	// load model metadata in order to convert to "public"
	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.WorksetPub{}, false // return empty result: model not found or error
	}

	// lock catalog and find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// get workset_lst db row by name
	w, err := db.GetWorksetByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, name)
	if err != nil {
		omppLog.Log("Error at get workset status: ", dn, ": ", name, ": ", err.Error())
		return &db.WorksetPub{}, false // return empty result: workset select error
	}
	if w == nil {
		omppLog.Log("Warning workset status not found: ", dn, ": ", name)
		return &db.WorksetPub{}, false // return empty result: workset_lst row not found
	}

	// get full workset metadata using matched prefered language or in all languages
	lc := ""
	if !isAllLang {
		_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
		lc = mc.modelLst[idx].langLst[np].LangCode
	}

	wm, err := db.GetWorksetFull(mc.modelLst[idx].dbConn, w, lc)
	if err != nil {
		omppLog.Log("Error at get workset metadata: ", dn, ": ", w.Name, ": ", err.Error())
		return &db.WorksetPub{}, false // return empty result: workset select error
	}

	// convert to "public" model workset format
	wp, err := wm.ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at workset conversion: ", dn, ": ", w.Name, ": ", err.Error())
		return &db.WorksetPub{}, false // return empty result: conversion error
	}

	return wp, true
}
