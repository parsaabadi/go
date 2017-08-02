// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

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

	idx, ok := mc.indexByDigest(digest)
	if !ok {
		return db.ModelDicRow{}, false // model not found, empty result
	}

	return mc.modelLst[idx].meta.Model, true
}

// ModelMetaByDigestOrName return model_dic db row by model digest or name.
func (mc *ModelCatalog) ModelMetaByDigestOrName(dn string) (*db.ModelMeta, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.ModelMeta{}, false
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		return &db.ModelMeta{}, false // return empty result: model not found or error
	}

	// lock model catalog and return copy of model metadata
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	m, err := mc.modelLst[idx].meta.Clone()
	if err != nil {
		omppLog.Log("Error at model metadata clone: ", dn, ": ", err.Error())
		return &db.ModelMeta{}, false
	}

	return m, true
}

// loadModelMeta read language-neutral model metadata from db.
// If metadata already loaded then do skip db reading and return index in model list.
// It search model by digest or name if digest not found.
// Return index in model list or < 0 on error or if model not found.
func (mc *ModelCatalog) loadModelMeta(dn string) (int, bool) {

	// if model digest-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return 0, false
	}

	// find model index by digest or name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return 0, false // model not found, index is negative
	}
	if mc.modelLst[idx].meta != nil && mc.modelLst[idx].isMetaFull { // exit if model metadata already loaded
		return idx, true
	}

	// read metadata from database
	m, err := db.GetModelById(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId)
	if err != nil {
		omppLog.Log("Error at get model metadata: ", dn, ": ", err.Error())
		return 0, false
	}

	// store model metadata
	mc.modelLst[idx].isMetaFull = true
	mc.modelLst[idx].meta = m

	return idx, true
}
