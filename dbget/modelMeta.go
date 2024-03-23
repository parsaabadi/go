// Copyright OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"

	_ "github.com/mattn/go-sqlite3"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// write model list from database into text csv or json file
func modelList(srcDb *sql.DB, runOpts *config.RunOptions) error {

	// get model list
	mLst, err := db.GetModelList(srcDb)
	if err != nil {
		return err
	}
	if len(mLst) <= 0 {
		omppLog.Log("Database is empty, models not found")
		return nil
	}

	// use specified file name or make default
	fp := ""
	if !theCfg.isNoFile {
		fp = theCfg.fileName
		if fp == "" {
			if theCfg.isJson {
				fp = "model_dic.json"
			} else {
				fp = "model_dic.csv"
			}
		}
		fp = filepath.Join(theCfg.dir, fp)
	}

	if theCfg.isNoFile {
		omppLog.Log("Do model-list")
	} else {
		omppLog.Log("Do model-list: " + fp)
	}

	// write json output into file and/or console
	if theCfg.isJson {
		return toJsonOutput(theCfg.isConsole, fp, mLst)
	}
	// else write csv output into file and/or console

	// write model master row into csv
	row := make([]string, 7)

	idx := 0
	err = toCsvOutput(
		theCfg.isConsole,
		fp,
		[]string{"model_id", "model_name", "model_digest", "model_type", "model_ver", "create_dt", "default_lang_code"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(mLst) {
				row[0] = strconv.Itoa(mLst[idx].ModelId)
				row[1] = mLst[idx].Name
				row[2] = mLst[idx].Digest
				row[3] = strconv.Itoa(mLst[idx].Type)
				row[4] = mLst[idx].Version
				row[5] = mLst[idx].CreateDateTime
				row[6] = mLst[idx].DefaultLangCode
				idx++
				return false, row, nil
			}
			return true, row, nil // end of model_dic rows
		})
	if err != nil {
		return errors.New("failed to write model into csv " + err.Error())
	}

	return nil
}
