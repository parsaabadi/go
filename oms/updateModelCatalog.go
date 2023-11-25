// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"golang.org/x/text/language"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// RefreshSqlite open db-connection to model.sqlite files in model directory and read model_dic row for each model.
// If multiple version of the same model (equal by digest) exist in different files then only one is used.
// All previously opened db connections are closed.
func (mc *ModelCatalog) refreshSqlite(modelDir, modelLogDir string) error {

	// model directory must exist
	isDir := modelDir != "" && modelDir != "."
	if isDir {
		isDir = dirExist(modelDir)
	}
	if !isDir {
		return errors.New("Error: model directory not exist or not accesible: " + modelDir)
	}

	// model log directory is optional, if empty or not exists then model log disabled
	isLogDir := modelLogDir != "" && modelLogDir != "."
	if isLogDir {
		isLogDir = dirExist(modelLogDir)
	}

	// get list of model/dir/*.sqlite files
	pathLst := []string{}
	err := filepath.Walk(modelDir, func(src string, info os.FileInfo, err error) error {
		if err != nil {
			if err != filepath.SkipDir {
				omppLog.Log("Error at refresh model catalog, path: ", src, " : ", err.Error())
			}
			return err
		}
		if strings.EqualFold(filepath.Ext(src), ".sqlite") {
			pathLst = append(pathLst, src)
		}
		return nil
	})
	if err != nil {
		omppLog.Log("Error: fail to list model directory: ", err.Error())
		return errors.New("Error: fail to list model directory")
	}
	sort.Strings(pathLst) // sort by path to model.sqlite: same as sort by model name in default case

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
		if err := db.CheckOpenmppSchemaVersion(dbc); err != nil {
			omppLog.Log("Error: invalid database, likely not an openM++ database: ", fp)
			dbc.Close()
			continue
		}
		dbDir := filepath.Dir(fp)

		dbPath, err := filepath.Abs(fp)
		if err != nil {
			omppLog.Log("Error: ", fp, " : ", err.Error())
			continue
		}
		dbRel, err := filepath.Rel(modelDir, dbDir)
		if err != nil {
			omppLog.Log("Error: ", fp, " : ", err.Error())
			continue
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

			// read metadata from database
			meta, err := db.GetModelById(dbc, dicLst[idx].ModelId)
			if err != nil {
				omppLog.Log("Error at get model metadata: ", dicLst[idx].Name, " ", dicLst[idx].Digest, ": ", err.Error())
				dbc.Close()
				continue dicLoop // skip this database
			}

			// read model_dic_txt rows from database
			txt, err := db.GetModelTextRowById(dbc, dicLst[idx].ModelId, "")
			if err != nil {
				omppLog.Log("Error at get model_dic_txt: ", dicLst[idx].Name, " ", dicLst[idx].Digest, ": ", err.Error())
				dbc.Close()
				continue dicLoop // skip this database
			}
			// partial initialization of model text metadata: only model_dic_txt rows
			mt := &db.ModelTxtMeta{
				ModelName:   meta.Model.Name,
				ModelDigest: meta.Model.Digest,
				ModelTxt:    txt}

			// read model_word from database
			w, err := db.GetModelWord(dbc, dicLst[idx].ModelId, "")
			if err != nil {
				omppLog.Log("Error at get model language-specific stirngs: ", dicLst[idx].Name, " ", dicLst[idx].Digest, ": ", err.Error())
				dbc.Close()
				continue dicLoop // skip this database
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

			// read model extra content from models/bin/dir/model.extra.json
			me := ""
			if bt, err := os.ReadFile(filepath.Join(dbDir, dicLst[idx].Name+".extra.json")); err == nil {
				me = string(bt)
			}

			// append to model list
			mLst = append(mLst, modelDef{
				dbConn:        dbc,
				binDir:        dbDir,
				dbPath:        dbPath,
				relDir:        filepath.ToSlash(dbRel),
				logDir:        modelLogDir,
				isLogDir:      isLogDir,
				meta:          meta,
				isTxtMetaFull: false,
				txtMeta:       mt,
				langCodes:     ml,
				langMeta:      ls,
				matcher:       language.NewMatcher(lt),
				modelWord:     w,
				extra:         me})
		}
	}

	// lock and update model catalog
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// update model directories
	mc.modelDir = modelDir
	mc.isDirEnabled = isDir
	mc.modelLogDir = modelLogDir
	mc.isLogDirEnabled = isLogDir

	// close existing connections and store updated list of models and db connections
	for k := range mc.modelLst {
		if err := mc.modelLst[k].dbConn.Close(); err != nil {
			omppLog.Log("Error: close db connection error: " + err.Error())
		}
	}

	mc.modelLst = mLst // set new list of the models
	return nil
}

// close all db-connection to model.sqlite files and clear model list.
func (mc *ModelCatalog) close() error {

	// lock and update model catalog
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// close existing db connections
	var firstErr error
	for k := range mc.modelLst {
		if err := mc.modelLst[k].dbConn.Close(); err != nil {
			omppLog.Log("Error: close db connection error: " + err.Error())
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// clear model list
	mc.modelLst = []modelDef{}
	return firstErr
}

// getNewTimeStamp return new unique timestamp and source time of it.
func (mc *ModelCatalog) getNewTimeStamp() (string, time.Time) {
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	tNow := time.Now()
	ts := helper.MakeTimeStamp(tNow)
	if ts == mc.lastTimeStamp {
		time.Sleep(2 * time.Millisecond)
		tNow = time.Now()
		ts = helper.MakeTimeStamp(tNow)
	}
	mc.lastTimeStamp = ts
	return ts, tNow
}

// Update model text metadata in catalog:
// Set boolean flag to indicate if text metadata fully loaded from database and ModelTxtMeta itself.
// Return false if model digest not found in catalog.
func (mc *ModelCatalog) setModelTextMeta(digest string, isFull bool, txtMeta *db.ModelTxtMeta) bool {

	if txtMeta == nil {
		return false // model text is empty
	}

	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx, ok := mc.indexByDigest(digest)
	if !ok {
		return false // model not found, empty result
	}

	mc.modelLst[idx].isTxtMetaFull = isFull
	mc.modelLst[idx].txtMeta = txtMeta
	return true
}
