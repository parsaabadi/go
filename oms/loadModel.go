// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"path/filepath"

	"golang.org/x/text/language"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// RefreshSqlite open db-connection to model.sqlite files in model directory and read model_dic row for each model.
// If multiple version of the same model (equal by digest) exist in different files then only one is used.
// All previously opened db connections are closed.
func (mc *ModelCatalog) RefreshSqlite(mDir string) error {

	// model directory must exist
	isDir := mDir != "" && mDir != "."
	if isDir {
		isDir = isDirExist(mDir) == nil
	}
	if !isDir {
		return errors.New("Error: model directory not exist or not accesible: " + mDir)
	}

	// get list of model/dir/*.sqlite files
	pathLst, err := filepath.Glob(filepath.Join(mDir, "*.sqlite"))
	if err != nil {
		omppLog.Log("Error: fail to list model directory: ", err.Error())
		return errors.New("Error: fail to list model directory")
	}

	// make list of models from model.sqlite files:
	// open db connection to model.sqlite and read list of model_dic rows.
	// if model exist in multiple sqlite files then only one is used.
	var mLst []modelDef
	for _, fp := range pathLst {

		// open db connection and check version of openM++ database
		dbc, _, err := db.Open(db.MakeSqliteDefault(fp), db.SQLiteDbDriver, false)
		if err != nil {
			omppLog.Log("Error: ", fp, " : ", err.Error())
			continue
		}
		nv, err := db.OpenmppSchemaVersion(dbc)
		if err != nil || nv < db.MinSchemaVersion {
			omppLog.Log("Error: invalid database, likely not an openM++ database", fp)
			dbc.Close()
		}

		// read list of models: model_dic rows
		dicLst, err := db.GetModelList(dbc)
		if err != nil || len(dicLst) <= 0 {
			omppLog.Log("Warning: empty database, no models found: ", fp)
			dbc.Close()
			continue // skip this database
		}

		ls, err := db.GetLanguages(dbc)
		if err != nil || ls == nil {
			omppLog.Log("Warning: no languages found in database: ", fp)
			dbc.Close()
			continue // skip this database
		}

		// append to list of models if not already exist
	dicLoop:
		for idx := range dicLst {

			// skip model if same digest already exist in model list
			for k := range mLst {
				if dicLst[idx].Digest == mLst[k].meta.Model.Digest {
					omppLog.Log("Skip: model already exist in other database: ", dicLst[idx].Name, " ", dicLst[idx].Digest)
					continue dicLoop
				}
			}

			// make model languages list, starting from default language
			ml := []db.LangLstRow{}
			lt := []language.Tag{}

			for k := range ls.Lang {
				if ls.Lang[k].LangCode == dicLst[idx].DefaultLangCode {
					ml = append([]db.LangLstRow{ls.Lang[k].LangLstRow}, ml...)
					lt = append([]language.Tag{language.Make(ls.Lang[k].LangCode)}, lt...)
				} else {
					ml = append(ml, ls.Lang[k].LangLstRow)
					lt = append(lt, language.Make(ls.Lang[k].LangCode))
				}
			}

			// append to model list
			mLst = append(mLst, modelDef{
				dbConn:     dbc,
				isMetaFull: false,
				meta:       &db.ModelMeta{Model: dicLst[idx]},
				langLst:    ml,
				langTags:   lt,
				matcher:    language.NewMatcher(lt)})
		}
	}

	// lock and update model catalog
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// update model directory
	theCatalog.modelDir = mDir
	theCatalog.isDirEnabled = isDir

	// close existing connections and store updated list of models and db connections
	for k := range mc.modelLst {
		if err := mc.modelLst[k].dbConn.Close(); err != nil {
			omppLog.Log("Error: close db connection error: " + err.Error())
		}
	}

	mc.modelLst = mLst // set new list of the models
	return nil
}

// getModelDir return model directory
func (mc *ModelCatalog) getModelDir() (string, bool) {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()
	return mc.modelDir, mc.isDirEnabled
}

// indexByDigest return index of model by digest or -1 if not found. It can be used only inside of lock.
func (mc *ModelCatalog) indexByDigest(digest string) int {
	for k := range mc.modelLst {
		if mc.modelLst[k].meta.Model.Digest == digest {
			return k
		}
	}
	return -1
}

// indexByDigestOrName return index of model by digest or by name. It can be used only inside of lock.
// If digest exist in model list then return index by digest else first index of name.
// Return -1 if no digest or name found.
func (mc *ModelCatalog) indexByDigestOrName(dn string) int {
	ni := -1
	for k := range mc.modelLst {
		if mc.modelLst[k].meta.Model.Digest == dn {
			return k
		}
		if ni < 0 && mc.modelLst[k].meta.Model.Name == dn {
			ni = k
		}
	}
	return ni
}

// AllModelDigests return copy of all model digests.
func (mc *ModelCatalog) AllModelDigests() []string {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	ds := []string{}
	for idx := range mc.modelLst {
		ds = append(ds, mc.modelLst[idx].meta.Model.Digest)
	}
	return ds
}

// ModelDicByDigest return model_dic db row by model digest.
func (mc *ModelCatalog) ModelDicByDigest(digest string) (db.ModelDicRow, bool) {

	// if model digest is empty then return empty results
	if digest == "" {
		omppLog.Log("Warning: invalid (empty) model digest")
		return db.ModelDicRow{}, false
	}

	// find model index by digest
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigest(digest)
	if idx < 0 {
		return db.ModelDicRow{}, false // model not found, empty result
	}

	return mc.modelLst[idx].meta.Model, true
}

// ModelMetaByDigestOrName return model_dic db row by model digest or name.
// TODO: ? return deep copy rather pointer to catalog ?
func (mc *ModelCatalog) ModelMetaByDigestOrName(dn string) (db.ModelMeta, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return db.ModelMeta{}, false
	}

	// if model metadata not loaded then read it from database
	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		return db.ModelMeta{}, false // return empty result: model not found or error
	}

	// lock model catalog and return copy of model metadata
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	return *mc.modelLst[idx].meta, true
}

// loadModelMeta read language-independent model metadata from db.
// If metadata already loaded then do skip db reading and return index in model list.
// It search model by digest or name if digest not found.
// Return index in model list or < 0 on error or if model not found.
func (mc *ModelCatalog) loadModelMeta(dn string) int {

	// if model digest-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return -1
	}

	// find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return idx // model not found, index is negative
	}
	if mc.modelLst[idx].meta != nil && mc.modelLst[idx].isMetaFull { // exit if model metadata already loaded
		return idx
	}

	// read metadata from database
	m, err := db.GetModelById(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	if err != nil {
		omppLog.Log("Error at get model metadata: ", dn, ": ", err.Error())
		return -1
	}

	// store model metadata
	mc.modelLst[idx].isMetaFull = true
	mc.modelLst[idx].meta = m

	return idx
}

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

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []db.LangLstRow{}, false
	}

	return append([]db.LangLstRow{}, mc.modelLst[idx].langLst...), true
}

// GroupsByDigestOrName return parameter and output table groups,
// language-neutral part of metadata by model digest or name.
func (mc *ModelCatalog) GroupsByDigestOrName(dn string) (*GroupMeta, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &GroupMeta{}, false
	}

	// if groups not loaded then read it from database
	idx := mc.loadModelGroups(dn)
	if idx < 0 {
		return &GroupMeta{}, false // return empty result: model not found or error
	}

	// lock model catalog and copy of language-neutral metadata
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	gm := GroupMeta{
		Group:   append([]db.GroupLstRow{}, mc.modelLst[idx].groupLst.GroupLst...),
		GroupPc: append([]db.GroupPcRow{}, mc.modelLst[idx].groupLst.GroupPc...)}

	return &gm, true
}

// loadModelGroups reads parameter and output table groups from db by digest or name.
// If groups already loaded then skip db reading and return index in model list.
// Return index in model list or < 0 on error or if model digest not found.
func (mc *ModelCatalog) loadModelGroups(dn string) int {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return -1
	}

	// find model index by digest-or-name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return idx // model not found, index is negative
	}
	if mc.modelLst[idx].groupLst != nil { // exit if groups already loaded
		return idx
	}

	// read groups from database
	g, err := db.GetModelGroup(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, "")
	if err != nil {
		omppLog.Log("Error at get parameter and output table groups: ", dn, ": ", err.Error())
		return -1
	}

	// store model groups
	mc.modelLst[idx].groupLst = g
	return idx
}

// ModelProfileByName return model profile db rows by digest and by profile name.
func (mc *ModelCatalog) ModelProfileByName(digest, profile string) (*db.ProfileMeta, bool) {

	// if model digest is empty or profile name is empty then return empty results
	if digest == "" {
		omppLog.Log("Warning: invalid (empty) model digest")
		return &db.ProfileMeta{}, false
	}
	if profile == "" {
		omppLog.Log("Warning: invalid (empty) profile name")
		return &db.ProfileMeta{}, false
	}

	// find model index by digest
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigest(digest)
	if idx < 0 {
		omppLog.Log("Warning: model digest: ", digest)
		return &db.ProfileMeta{}, false // return empty result: model not found or error
	}

	// read groups from database
	p, err := db.GetProfile(mc.modelLst[idx].dbConn, profile)
	if err != nil {
		omppLog.Log("Error at get profile: ", digest, ": ", profile, ": ", err.Error())
		return &db.ProfileMeta{}, false // return empty result: model not found or error
	}

	return p, true
}
