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

	// use of run and set id's in directory names:
	// do this by default or if use id name = true
	// only if use id name = false then do not use id's in directory names
	isUseIdNames := !runOpts.IsExist(useIdNamesArgKey) || runOpts.Bool(useIdNamesArgKey)

	// write workset metadata into json and parameter values into csv files
	dblFmt := runOpts.String(doubleFormatArgKey)
	isIdCsv := runOpts.Bool(useIdCsvArgKey)
	isWriteUtf8bom := runOpts.Bool(useUtf8CsvArgKey)

	if err = toWorksetText(srcDb, modelDef, wm, outDir, dblFmt, isIdCsv, isWriteUtf8bom, isUseIdNames); err != nil {
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
	dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, doubleFmt string, isIdCsv bool, isWriteUtf8bom bool, isUseIdNames bool) error {

	// get all readonly worksets
	wl, err := db.GetWorksetFullList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// read all workset parameters and dump it into csv files
	for k := range wl {
		err = toWorksetText(dbConn, modelDef, &wl[k], outDir, doubleFmt, isIdCsv, isWriteUtf8bom, isUseIdNames)
		if err != nil {
			return err
		}
	}
	return nil
}

// toWorksetText write workset into csv file, in separate subdirectory
// by default file name and directory name include set id: modelName.set.1234.SetName
// user can explicitly disable it by IdNames=false
func toWorksetText(
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	meta *db.WorksetMeta,
	outDir string,
	doubleFmt string,
	isIdCsv bool,
	isWriteUtf8bom bool,
	isUseIdNames bool) error {

	// convert db rows into "public" format
	setId := meta.Set.SetId
	omppLog.Log("Workset ", setId, " ", meta.Set.Name)

	pub, err := meta.ToPublic(dbConn, modelDef)
	if err != nil {
		return err
	}

	// create workset subdir under output dir
	var csvName string
	if !isUseIdNames {
		csvName = "set." + helper.ToAlphaNumeric(pub.Name)
	} else {
		csvName = "set." + strconv.Itoa(setId) + "." + helper.ToAlphaNumeric(pub.Name)
	}
	csvDir := filepath.Join(outDir, csvName)

	err = os.MkdirAll(csvDir, 0750)
	if err != nil {
		return err
	}

	paramLt := &db.ReadParamLayout{ReadLayout: db.ReadLayout{FromId: setId}, IsFromSet: true}

	// write parameter into csv file
	for j := range pub.Param {

		paramLt.Name = pub.Param[j].Name

		cLst, _, err := db.ReadParameter(dbConn, modelDef, paramLt)
		if err != nil {
			return err
		}
		if cLst.Len() <= 0 { // parameter data must exist for all parameters
			return errors.New("missing workset parameter values " + paramLt.Name + " set id: " + strconv.Itoa(paramLt.FromId))
		}

		var pc db.CellParam
		err = toCsvCellFile(csvDir, modelDef, paramLt.Name, pc, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom)
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
