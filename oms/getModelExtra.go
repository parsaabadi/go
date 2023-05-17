// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
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

	// get database connection
	_, dbConn, ok := mc.modelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &db.ProfileMeta{}, false
	}

	// read profile from database
	p, err := db.GetProfile(dbConn, profile)
	if err != nil {
		omppLog.Log("Error at get profile: ", dn, ": ", profile, ": ", err.Error())
		return &db.ProfileMeta{}, false // return empty result: model not found or error
	}

	return p, true
}

// ProfileNamesByDigestOrName return list of profile(s) names by model digest-or-name.
// This is a list of profiles from model database, it is not a "model" profile(s).
// There is no explicit link between profile and model, profile can be applicable to multiple models.
func (mc *ModelCatalog) ProfileNamesByDigestOrName(dn string) ([]string, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return []string{}, false
	}

	// get database connection
	_, dbConn, ok := mc.modelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []string{}, false // return empty result: model not found or error
	}

	// read profile names from database
	nameLst, err := db.GetProfileList(dbConn)
	if err != nil {
		omppLog.Log("Error at get profile list from model database: ", dn, ": ", err.Error())
		return []string{}, false
	}

	return nameLst, true
}

// WordListByDigestOrName return model "words" by model digest and preferred language tags.
// Model "words" are arrays of rows from lang_word and model_word db tables.
// It can be in preferred language, default model language or empty if no lang_word or model_word rows exist.
func (mc *ModelCatalog) WordListByDigestOrName(dn string, preferredLang []language.Tag) (*ModelLangWord, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &ModelLangWord{}, false
	}

	// match preferred languages and model languages
	lc := mc.languageTagMatch(dn, preferredLang)
	lcd, _, _ := mc.modelLangs(dn)
	if lc == "" && lcd == "" {
		omppLog.Log("Error: invalid (empty) model default language: ", dn)
		return &ModelLangWord{}, false
	}

	// lock model catalog
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		return &ModelLangWord{}, false // return empty result: model not found or error
	}

	// find lang_word rows in preferred or model default language
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

	// find model_word rows in preferred or model default language
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

// EntityGenByName select entity_get and entity_gen_attr db row by entity name, run id and model digest or name.
func (mc *ModelCatalog) EntityGenByName(dn string, runId int, entityName string) (*db.EntityGenMeta, bool) {

	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return nil, false
	}
	if entityName == "" {
		omppLog.Log("Warning: invalid (empty) entity name")
		return nil, false
	}
	meta, dbConn, ok := mc.modelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return nil, false // return empty result: model not found or error
	}

	// find model entity by entity name
	eIdx, ok := meta.EntityByName(entityName)
	if !ok {
		omppLog.Log("Warning: model entity not found: ", entityName)
		return nil, false
	}
	ent := &meta.Entity[eIdx]

	// get list of entity generations for that model run
	egLst, err := db.GetEntityGenList(dbConn, runId)
	if err != nil {
		omppLog.Log("Error at get run entities: ", entityName, ": ", runId, ": ", err.Error())
		return nil, false
	}

	// find entity generation by entity name
	gIdx := -1
	for k := range egLst {

		if egLst[k].EntityId == ent.EntityId {
			gIdx = k
			break
		}
	}
	if gIdx < 0 {
		omppLog.Log("Error: model run entity generation not found: ", entityName, ": ", runId)
		return nil, false
	}

	return &egLst[gIdx], true
}
