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

	// use of run and set id's in directory names:
	// do this by default or if use id name = true
	// only if use id name = false then do not use id's in directory names
	isUseIdNames := !runOpts.IsExist(useIdNamesArgKey) || runOpts.Bool(useIdNamesArgKey)

	// write all model run data into csv files: parameters, output expressions and accumulators
	dblFmt := runOpts.String(doubleFormatArgKey)
	isIdCsv := runOpts.Bool(useIdCsvArgKey)
	isWriteUtf8bom := runOpts.Bool(useUtf8CsvArgKey)

	if err = toRunListText(srcDb, modelDef, outDir, dblFmt, isIdCsv, isWriteUtf8bom, isUseIdNames); err != nil {
		return err
	}

	// write all readonly workset data into csv files: input parameters
	if err = toWorksetListText(srcDb, modelDef, outDir, dblFmt, isIdCsv, isWriteUtf8bom, isUseIdNames); err != nil {
		return err
	}

	// write all modeling tasks and task run history to json files
	if err = toTaskListJson(srcDb, modelDef, outDir, isUseIdNames); err != nil {
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
	csvDir string,
	modelDef *db.ModelMeta,
	name string,
	isAppend bool,
	cell db.CsvConverter,
	cellLst *list.List,
	doubleFmt string,
	isIdCsv bool,
	valueName string,
	isWriteUtf8bom bool,
	extraFirstName string,
	extraFirstValue string) error {

	// converter from db cell to csv row []string
	var cvt func(interface{}, []string) error
	var err error
	if !isIdCsv {
		cvt, err = cell.CsvToRow(modelDef, name, doubleFmt, valueName)
	} else {
		cvt, err = cell.CsvToIdRow(modelDef, name, doubleFmt, valueName)

	}
	if err != nil {
		return err
	}

	// create csv file or open existing for append
	fn, err := cell.CsvFileName(modelDef, name)
	if err != nil {
		return err
	}

	flag := os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	if isAppend {
		flag = os.O_APPEND | os.O_WRONLY
	}

	f, err := os.OpenFile(filepath.Join(csvDir, fn), flag, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if isWriteUtf8bom { // if required then write utf-8 bom
		if _, err = f.Write(helper.Utf8bom); err != nil {
			return err
		}
	}

	wr := csv.NewWriter(f)

	// if not append to already existing csv file then write header line: column names
	cs, err := cell.CsvHeader(modelDef, name, isIdCsv, valueName)
	if err != nil {
		return err
	}
	if extraFirstName != "" {
		cs = append([]string{extraFirstName}, cs...) // if this is all-in-one then prepend first column name
	}
	if !isAppend {
		if err = wr.Write(cs); err != nil {
			return err
		}
	}
	if extraFirstValue != "" {
		cs[0] = extraFirstValue // if this is all-in-one then first column value is run id (or name or set id set name)
	}

	for c := cellLst.Front(); c != nil; c = c.Next() {

		// write cell line: dimension(s) and value
		// if "all-in-one" then prepend first value, e.g.: run id
		if extraFirstValue == "" {
			if err := cvt(c.Value, cs); err != nil {
				return err
			}
		} else {
			if err := cvt(c.Value, cs[1:]); err != nil {
				return err
			}
		}
		if err := wr.Write(cs); err != nil {
			return err
		}
	}

	// flush and return error, if any
	wr.Flush()
	return wr.Error()
}
