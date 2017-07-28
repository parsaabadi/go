// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
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

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return []db.LangLstRow{}, false
	}

	ls := make([]db.LangLstRow, len(mc.modelLst[idx].langMeta.Lang))
	for k := range mc.modelLst[idx].langMeta.Lang {
		ls[k] = mc.modelLst[idx].langMeta.Lang[k].LangLstRow
	}

	return ls, true
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
		omppLog.Log("Warning: model digest not found: ", digest)
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
