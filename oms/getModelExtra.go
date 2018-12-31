// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
	"golang.org/x/text/language"
)

// LangListByDigestOrName return lang_lst db rows by model digest or name.
// First language is model default language.
// It may contain more languages than model actually have.
func (mc *ModelCatalog) LangListByDigestOrName(dn string) ([]db.LangLstRow, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []db.LangLstRow{}, false
	}

	// lock model catalog and return copy of langauge list
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []db.LangLstRow{}, false
	}

	ls := make([]db.LangLstRow, len(mc.modelLst[idx].langMeta.Lang))
	for k := range mc.modelLst[idx].langMeta.Lang {
		ls[k] = mc.modelLst[idx].langMeta.Lang[k].LangLstRow
	}

	return ls, true
}

// ModelProfileByName return model profile db rows by model digest-or-name and by profile name.
func (mc *ModelCatalog) ModelProfileByName(dn, profile string) (*db.ProfileMeta, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.ProfileMeta{}, false
	}
	if profile == "" {
		omppLog.Log("Warning: invalid (empty) profile name")
		return &db.ProfileMeta{}, false
	}

	// find model index by digest
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.ProfileMeta{}, false // return empty result: model not found or error
	}

	// read profile from database
	p, err := db.GetProfile(mc.modelLst[idx].dbConn, profile)
	if err != nil {
		omppLog.Log("Error at get profile: ", dn, ": ", profile, ": ", err.Error())
		return &db.ProfileMeta{}, false // return empty result: model not found or error
	}

	return p, true
}

// WordListByDigestOrName return model "words" by model digest and prefered language tags.
// Model "words" are arrays of rows from lang_word and model_word db tables.
// It can be in prefered language, default model language or empty if no lang_word or model_word rows exist.
func (mc *ModelCatalog) WordListByDigestOrName(dn string, preferedLang []language.Tag) (*ModelLangWord, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &ModelLangWord{}, false
	}

	// if model_word rows not loaded then read it from database
	idx := mc.loadModelWord(dn)
	if idx < 0 {
		return &ModelLangWord{}, false // return empty result: model not found or error
	}

	// lock model catalog
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// match prefered languages and model languages
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langCodes[np]
	lcd := mc.modelLst[idx].meta.Model.DefaultLangCode

	// find lang_word rows in prefered or model default language
	mlw := ModelLangWord{
		ModelName:   mc.modelLst[idx].meta.Model.Name,
		ModelDigest: mc.modelLst[idx].meta.Model.Digest}

	var nd, i int
	for i = 0; i < len(mc.modelLst[idx].langMeta.Lang); i++ {
		if mc.modelLst[idx].langMeta.Lang[i].LangCode == lc {
			break // language match
		}
		if mc.modelLst[idx].langMeta.Lang[i].LangCode == lcd {
			nd = i // index of default language
		}
	}
	if i >= len(mc.modelLst[idx].langMeta.Lang) {
		i = nd // use default language or zero index row
	}

	// copy lang_word rows, if exist for that language index
	if i < len(mc.modelLst[idx].langMeta.Lang) {
		mlw.LangCode = mc.modelLst[idx].langMeta.Lang[i].LangCode // actual language of result
		for c, v := range mc.modelLst[idx].langMeta.Lang[i].Words {
			mlw.LangWords = append(mlw.LangWords, codeLabel{Code: c, Label: v})
		}
	}

	// find model_word rows in prefered or model default language
	nd = 0
	for i = 0; i < len(mc.modelLst[idx].modelWord.ModelWord); i++ {
		if mc.modelLst[idx].modelWord.ModelWord[i].LangCode == lc {
			break // language match
		}
		if mc.modelLst[idx].modelWord.ModelWord[i].LangCode == lcd {
			nd = i // index of default language
		}
	}
	if i >= len(mc.modelLst[idx].modelWord.ModelWord) {
		i = nd // use default language or zero index row
	}

	// copy model_word rows, if exist for that language index
	if i < len(mc.modelLst[idx].modelWord.ModelWord) {
		mlw.ModelLangCode = mc.modelLst[idx].modelWord.ModelWord[i].LangCode // actual language of result
		for c, v := range mc.modelLst[idx].modelWord.ModelWord[i].Words {
			mlw.ModelWords = append(mlw.ModelWords, codeLabel{Code: c, Label: v})
		}
	}

	return &mlw, true
}

// loadModelWord reads model_word table rows from db by digest or name.
// If model words already loaded then skip db reading and return index in model list.
// Return index in model list or < 0 on error or if model digest not found.
func (mc *ModelCatalog) loadModelWord(dn string) int {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return -1
	}

	// find model index by digest-or-name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return idx // model not found, index is negative
	}
	if mc.modelLst[idx].modelWord != nil { // exit if model_word already loaded
		return idx
	}

	// read model_word from database
	w, err := db.GetModelWord(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, "")
	if err != nil {
		omppLog.Log("Error at get model language-specific stirngs: ", dn, ": ", err.Error())
		return -1
	}

	// store model words
	mc.modelLst[idx].modelWord = w
	return idx
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
		omppLog.Log("Warning: workset not found: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", wsn)
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
		omppLog.Log("Warning: run not found: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", rdn)
		return nil, false // return empty result: run_lst row not found
	}

	// run must be completed
	if !db.IsRunCompleted(rst.Status) {
		omppLog.Log("Warning: run is not completed: ", mc.modelLst[modelIdx].meta.Model.Name, ": ", rdn, ": ", rst.Status)
		return nil, false // return empty result: run_lst row not found
	}

	return rst, true
}
