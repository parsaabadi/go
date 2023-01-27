// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"github.com/openmpp/go/ompp/db"
	"golang.org/x/text/language"
)

// get "public" configuration of model catalog
func (mc *ModelCatalog) toPublicConfig() ModelCatalogConfig {

	// lock run catalog and return results
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	return ModelCatalogConfig{
		ModelDir:        mc.modelDir,
		ModelLogDir:     mc.modelLogDir,
		IsLogDirEnabled: mc.isLogDirEnabled,
		LastTimeStamp:   mc.lastTimeStamp,
	}
}

// getModelDir return model directory
func (mc *ModelCatalog) getModelDir() (string, bool) {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()
	return mc.modelDir, mc.isDirEnabled
}

// getModelLogDir return default model directory and true if that directory exist
func (mc *ModelCatalog) getModelLogDir() (string, bool) {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()
	return mc.modelLogDir, mc.isLogDirEnabled
}

// indexByDigest return index of model by digest.
// It can be used only inside of lock.
func (mc *ModelCatalog) indexByDigest(digest string) (int, bool) {
	for k := range mc.modelLst {
		if mc.modelLst[k].meta.Model.Digest == digest {
			return k, true
		}
	}
	return 0, false
}

// indexByDigestOrName return index of model by digest or by name.
// It can be used only inside of lock.
// If digest exist in model list then return index by digest else first index of name.
// If not found then return false flag.
func (mc *ModelCatalog) indexByDigestOrName(dn string) (int, bool) {
	n := -1
	for k := range mc.modelLst {
		if mc.modelLst[k].meta.Model.Digest == dn {
			return k, true // return: digest found
		}
		if n < 0 && mc.modelLst[k].meta.Model.Name == dn {
			n = k
		}
	}
	if n >= 0 {
		return n, true // return: name found
	}
	return 0, false // not found
}

// languageMatch return model language matched to request language, e.g. EN from en-CA.
// if there is no such match then return is empty "" language code.
// It can be used only inside of lock.
func (mc *ModelCatalog) languageMatch(modelIdx int, langCode string) string {

	if modelIdx >= 0 && modelIdx < len(mc.modelLst) && langCode != "" {

		_, np, _ := mc.modelLst[modelIdx].matcher.Match(language.Make(langCode))

		if np >= 0 && np < len(mc.modelLst[modelIdx].langCodes) {
			return mc.modelLst[modelIdx].langCodes[np]
		}
	}
	return ""
}

// allModelDigests return digests for all models.
func (mc *ModelCatalog) allModelDigests() []string {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	ds := make([]string, len(mc.modelLst))
	for idx := range mc.modelLst {
		ds[idx] = mc.modelLst[idx].meta.Model.Digest
	}
	return ds
}

// modelBasicByDigest return basic info from catalog about model by digest.
func (mc *ModelCatalog) modelBasicByDigest(digest string) (modelBasic, bool) {
	return mc.findModelBasic(true, digest)
}

// modelBasicByDigestOrName return basic info from catalog about model by digest or model name.
func (mc *ModelCatalog) modelBasicByDigestOrName(dn string) (modelBasic, bool) {
	return mc.findModelBasic(false, dn)
}

// findModelBasic return basic info from catalog about model by  or model name.
func (mc *ModelCatalog) findModelBasic(isByDigestOnly bool, dn string) (modelBasic, bool) {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := 0
	ok := false
	if isByDigestOnly {
		idx, ok = mc.indexByDigest(dn)
	} else {
		idx, ok = mc.indexByDigestOrName(dn)
	}
	if !ok {
		return modelBasic{}, false // model not found, empty result
	}
	return modelBasic{
			name:     mc.modelLst[idx].meta.Model.Name,
			digest:   mc.modelLst[idx].meta.Model.Digest,
			binDir:   mc.modelLst[idx].binDir,
			logDir:   mc.modelLst[idx].logDir,
			isLogDir: mc.modelLst[idx].isLogDir,
			dbPath:   mc.modelLst[idx].dbPath,
			relDir:   mc.modelLst[idx].relDir,
			extra:    mc.modelLst[idx].extra,
		},
		true
}

// allModels return basic info from catalog about all models: name, digest, files location.
func (mc *ModelCatalog) allModels() []modelBasic {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	mbs := make([]modelBasic, len(mc.modelLst))
	for idx := range mc.modelLst {
		mbs[idx] = modelBasic{
			name:     mc.modelLst[idx].meta.Model.Name,
			digest:   mc.modelLst[idx].meta.Model.Digest,
			binDir:   mc.modelLst[idx].binDir,
			logDir:   mc.modelLst[idx].logDir,
			isLogDir: mc.modelLst[idx].isLogDir,
			dbPath:   mc.modelLst[idx].dbPath,
			relDir:   mc.modelLst[idx].relDir,
			extra:    mc.modelLst[idx].extra,
		}
	}
	return mbs
}

// modelEntityAttrs return model entities and attributesfrom catalog about model by digest.
func (mc *ModelCatalog) entityAttrsByDigest(digest string) []db.EntityMeta {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigest(digest)
	if !ok {
		return []db.EntityMeta{} // model not found, empty result
	}

	// copy model entity db rows
	em := make([]db.EntityMeta, len(mc.modelLst[idx].meta.Entity))

	for k := range mc.modelLst[idx].meta.Entity {

		em[k] = mc.modelLst[idx].meta.Entity[k]
		em[k].Attr = make([]db.EntityAttrRow, len(mc.modelLst[idx].meta.Entity[k].Attr))
		copy(em[k].Attr, mc.modelLst[idx].meta.Entity[k].Attr)
	}
	return em
}
