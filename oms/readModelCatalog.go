// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

// get "public" configuration of model catalog
func (mc *ModelCatalog) toPublicConfig() *ModelCatalogConfig {

	// lock run catalog and return results
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	mp := ModelCatalogConfig{
		ModelDir:        mc.modelDir,
		ModelLogDir:     mc.modelLogDir,
		IsLogDirEnabled: mc.isLogDirEnabled,
		LastTimeStamp:   mc.lastTimeStamp,
	}
	return &mp
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

// binDirectoryByDigest return model bin directory where model.exe expected to be located.
func (mc *ModelCatalog) modelBasicByDigest(digest string) (modelBasic, bool) {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigest(digest)
	if !ok {
		return modelBasic{}, false // model not found, empty result
	}
	return modelBasic{
			name:     mc.modelLst[idx].meta.Model.Name,
			digest:   mc.modelLst[idx].meta.Model.Digest,
			binDir:   mc.modelLst[idx].binDir,
			logDir:   mc.modelLst[idx].logDir,
			isLogDir: mc.modelLst[idx].isLogDir,
		},
		true
}

// allModels return basic info about all models: name, digest, files location.
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
		}
	}
	return mbs
}
