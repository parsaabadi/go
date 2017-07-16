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
