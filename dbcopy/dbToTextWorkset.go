// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// copy workset from database into text json and csv files
func dbToTextWorkset(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// get workset name and id
	setName := runOpts.String(setNameArgKey)
	setId := runOpts.Int(setIdArgKey, 0)

	// conflicting options: use set id if positive else use set name
	if runOpts.IsExist(setNameArgKey) && runOpts.IsExist(setIdArgKey) {
		if setId > 0 {
			omppLog.Log("dbcopy options conflict. Using set id: ", setId, " ignore set name: ", setName)
			setName = ""
		} else {
			omppLog.Log("dbcopy options conflict. Using set name: ", setName, " ignore set id: ", setId)
			setId = 0
		}
	}

	if setId < 0 || setId == 0 && setName == "" {
		return errors.New("dbcopy invalid argument(s) for set id: " + runOpts.String(setIdArgKey) + " and/or set name: " + runOpts.String(setNameArgKey))
	}

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(dbConnStrArgKey), runOpts.String(dbDriverArgKey))
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	nv, err := db.OpenmppSchemaVersion(srcDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid database, likely not an openM++ database")
	}

	// get model metadata
	modelDef, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = modelDef.Model.Name // set model name: it can be empty and only model digest specified

	// "root" directory for workset metadata
	// later this "root" combined with modelName.set.name or modelName.set.id
	outDir := ""
	if runOpts.IsExist(paramDirArgKey) {
		outDir = filepath.Clean(runOpts.String(paramDirArgKey))
	} else {
		if setId > 0 {
			outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".set."+strconv.Itoa(setId))
		} else {
			outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".set."+setName)
		}
	}

	// get workset metadata by id or name
	var wsRow *db.WorksetRow
	if setId > 0 {
		if wsRow, err = db.GetWorkset(srcDb, setId); err != nil {
			return err
		}
		if wsRow == nil {
			return errors.New("workset not found, set id: " + strconv.Itoa(setId))
		}
	} else {
		if wsRow, err = db.GetWorksetByName(srcDb, modelDef.Model.ModelId, setName); err != nil {
			return err
		}
		if wsRow == nil {
			return errors.New("workset not found: " + setName)
		}
	}

	wm, err := db.GetWorksetFull(srcDb, wsRow, "") // get full workset metadata
	if err != nil {
		return err
	}

	// check: workset must be readonly
	if !wm.Set.IsReadonly {
		return errors.New("workset must be readonly: " + strconv.Itoa(wsRow.SetId) + " " + wsRow.Name)
	}

	// create new output directory for workset metadata
	err = os.MkdirAll(outDir, 0750)
	if err != nil {
		return err
	}

	// write workset metadata into json and parameter values into csv files
	dblFmt := runOpts.String(doubleFormatArgKey)
	isIdCsv := runOpts.Bool(useIdCsvArgKey)
	if err = toWorksetText(srcDb, modelDef, wm, outDir, dblFmt, isIdCsv); err != nil {
		return err
	}

	// pack worksets metadata json and csv files into zip
	if runOpts.Bool(zipArgKey) {
		zipPath, err := helper.PackZip(outDir, "")
		if err != nil {
			return err
		}
		omppLog.Log("Packed ", zipPath)
	}

	return nil
}

// toWorksetListText write all readonly worksets into csv files, each set in separate subdirectory
func toWorksetListText(
	dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, doubleFmt string, isIdCsv bool) error {

	// get all readonly worksets
	wl, err := db.GetWorksetFullList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// read all workset parameters and dump it into csv files
	for k := range wl {
		err = toWorksetText(dbConn, modelDef, &wl[k], outDir, doubleFmt, isIdCsv)
		if err != nil {
			return err
		}
	}
	return nil
}

// toWorksetText write workset into csv file, in separate subdirectory
func toWorksetText(
	dbConn *sql.DB, modelDef *db.ModelMeta, meta *db.WorksetMeta, outDir string, doubleFmt string, isIdCsv bool) error {

	// convert db rows into "public" format
	setId := meta.Set.SetId
	omppLog.Log("Workset ", setId, " ", meta.Set.Name)

	pub, err := meta.ToPublic(dbConn, modelDef)
	if err != nil {
		return err
	}

	// create workset subdir under output dir
	csvName := "set." + strconv.Itoa(setId) + "." + helper.ToAlphaNumeric(pub.Name)
	csvDir := filepath.Join(outDir, csvName)

	err = os.MkdirAll(csvDir, 0750)
	if err != nil {
		return err
	}

	layout := &db.ReadLayout{FromId: setId, IsFromSet: true}

	// write parameter into csv file
	for j := range pub.Param {

		layout.Name = pub.Param[j].Name

		cLst, err := db.ReadParameter(dbConn, modelDef, layout)
		if err != nil {
			return err
		}
		if cLst.Len() <= 0 { // parameter data must exist for all parameters
			return errors.New("missing workset parameter values " + layout.Name + " set id: " + strconv.Itoa(layout.FromId))
		}

		var pc db.CellValue
		err = toCsvCellFile(csvDir, modelDef, layout.Name, pc, cLst, doubleFmt, isIdCsv, "")
		if err != nil {
			return err
		}
	}

	// save model workset metadata into json
	if err := helper.ToJsonFile(filepath.Join(outDir, modelDef.Model.Name+"."+csvName+".json"), pub); err != nil {
		return err
	}
	return nil
}
