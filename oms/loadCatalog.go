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
			ml := []string{}
			lt := []language.Tag{}

			for k := range ls.Lang {
				if ls.Lang[k].LangCode == dicLst[idx].DefaultLangCode {
					ml = append([]string{ls.Lang[k].LangCode}, ml...)
					lt = append([]language.Tag{language.Make(ls.Lang[k].LangCode)}, lt...)
				} else {
					ml = append(ml, ls.Lang[k].LangCode)
					lt = append(lt, language.Make(ls.Lang[k].LangCode))
				}
			}

			// sort and clone model languages
			m := &db.LangMeta{}
			for k := range ml {
				for j := range ls.Lang {
					if ls.Lang[j].LangCode == ml[k] {
						m.Lang = append(m.Lang, ls.Lang[j])
						break
					}
				}
			}
			cl, err := m.Clone()
			if err != nil || ls == nil {
				omppLog.Log("Warning: language clone failed for database: ", fp)
				dbc.Close()
				continue // skip this database
			}

			// append to model list
			mLst = append(mLst, modelDef{
				dbConn:     dbc,
				isMetaFull: false,
				meta:       &db.ModelMeta{Model: dicLst[idx]},
				langCodes:  ml,
				langMeta:   cl,
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
	n := -1
	for k := range mc.modelLst {
		if mc.modelLst[k].meta.Model.Digest == dn {
			return k
		}
		if n < 0 && mc.modelLst[k].meta.Model.Name == dn {
			n = k
		}
	}
	return n
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
