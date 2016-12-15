// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"database/sql"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// copy model from database into text json and csv files
func dbToText(modelName string, modelDigest string, runOpts *config.RunOptions) error {

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

	// create new output directory, use modelName subdirectory
	outDir := filepath.Join(runOpts.String(outputDirArgKey), modelName)
	err = os.MkdirAll(outDir, 0750)
	if err != nil {
		return err
	}

	// write model definition to json file
	if err = toModelJson(srcDb, modelDef, outDir); err != nil {
		return err
	}

	// write all model run data into csv files: parameters, output expressions and accumulators
	dblFmt := runOpts.String(doubleFormatArgKey)
	isIdCsv := runOpts.Bool(useIdCsvArgKey)
	if err = toRunListText(srcDb, modelDef, outDir, dblFmt, isIdCsv); err != nil {
		return err
	}

	// write all readonly workset data into csv files: input parameters
	if err = toWorksetListText(srcDb, modelDef, outDir, dblFmt, isIdCsv); err != nil {
		return err
	}

	// write all modeling tasks and task run history to json files
	if err = toTaskListJson(srcDb, modelDef, outDir); err != nil {
		return err
	}

	// pack model metadata, run results and worksets into zip
	if runOpts.Bool(zipArgKey) {
		zipPath, err := helper.PackZip(outDir, "")
		if err != nil {
			return err
		}
		omppLog.Log("Packed ", zipPath)
	}

	return nil
}

// toModelJson convert model metadata to json and write into json files.
func toModelJson(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string) error {

	// get list of languages
	langDef, err := db.GetLanguages(dbConn)
	if err != nil {
		return err
	}

	// get model text (description and notes) in all languages
	modelTxt, err := db.GetModelText(dbConn, modelDef.Model.ModelId, "")
	if err != nil {
		return err
	}

	// get model language-specific strings in all languages
	mwDef, err := db.GetModelWord(dbConn, modelDef.Model.ModelId, "")
	if err != nil {
		return err
	}

	// get model parameter and output table groups and group text (description and notes) in all languages
	modelGroup, err := db.GetModelGroup(dbConn, modelDef.Model.ModelId, "")
	if err != nil {
		return err
	}

	// get model profile: default model profile is profile where name = model name
	modelName := modelDef.Model.Name
	modelProfile, err := db.GetProfile(dbConn, modelName)
	if err != nil {
		return err
	}

	// save into model json files
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".model.json"), &modelDef); err != nil {
		return err
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".lang.json"), &langDef); err != nil {
		return err
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".text.json"), &modelTxt); err != nil {
		return err
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".word.json"), &mwDef); err != nil {
		return err
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".group.json"), &modelGroup); err != nil {
		return err
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".profile.json"), &modelProfile); err != nil {
		return err
	}
	return nil
}

// toCsvCellFile convert parameter or output table values and write into csvDir/fileName.csv file.
// if isIdCsv is true then csv contains enum id's, default: enum code
func toCsvCellFile(
	csvDir string, modelDef *db.ModelMeta, name string, cell db.CsvConverter, cellLst *list.List, doubleFmt string, isIdCsv bool) error {

	// converter from db cell to csv row []string
	var cvt func(interface{}, []string) error
	var err error
	if !isIdCsv {
		cvt, err = cell.CsvToRow(modelDef, name, doubleFmt)
	} else {
		cvt, err = cell.CsvToIdRow(modelDef, name, doubleFmt)

	}
	if err != nil {
		return err
	}

	// create csv file
	fn, err := cell.CsvFileName(modelDef, name)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(filepath.Join(csvDir, fn), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	wr := csv.NewWriter(f)

	// write header line: column names
	cs, err := cell.CsvHeader(modelDef, name, isIdCsv)
	if err != nil {
		return err
	}
	if err = wr.Write(cs); err != nil {
		return err
	}

	for c := cellLst.Front(); c != nil; c = c.Next() {

		// write cell line: dimension(s) and value
		if err := cvt(c.Value, cs); err != nil {
			return err
		}
		if err := wr.Write(cs); err != nil {
			return err
		}
	}

	// flush and return error, if any
	wr.Flush()
	return wr.Error()
}
