// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
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

// ModelDicByDigestOrName return model_dic db row by model digest or name.
func (mc *ModelCatalog) ModelDicByDigestOrName(dn string) (db.ModelDicRow, bool) {

	// if model digest is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return db.ModelDicRow{}, false
	}

	// find model index by digest
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		return db.ModelDicRow{}, false // model not found, empty result
	}

	return mc.modelLst[idx].meta.Model, true
}

// ModelMetaByDigestOrName return copy of model metadata by model digest or name.
func (mc *ModelCatalog) ModelMetaByDigestOrName(dn string) (*db.ModelMeta, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.ModelMeta{}, false
	}

	// lock model catalog and return copy of model metadata
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		return &db.ModelMeta{}, false // return empty result: model not found or error
	}

	m, err := mc.modelLst[idx].meta.Clone()
	if err != nil {
		omppLog.Log("Error at model metadata clone: ", dn, ": ", err.Error())
		return &db.ModelMeta{}, false
	}

	return m, true
}

// ModelTextByDigestOrName return copy of model text metadata by model digest or name.
func (mc *ModelCatalog) ModelTextByDigestOrName(dn string) (*db.ModelTxtMeta, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.ModelTxtMeta{}, false
	}

	// lock model catalog and return copy of model metadata
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigestOrName(dn)
	if !ok {
		return &db.ModelTxtMeta{}, false // return empty result: model not found or error
	}

	txt, err := mc.modelLst[idx].txtMeta.Clone()
	if err != nil {
		omppLog.Log("Error at model text metadata clone: ", dn, ": ", err.Error())
		return &db.ModelTxtMeta{}, false
	}

	return txt, true
}
