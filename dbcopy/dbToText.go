// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// copy model from database into text json and csv files
func dbToText(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefaultReadOnly(modelName, runOpts.String(fromSqliteArgKey), runOpts.String(dbConnStrArgKey), runOpts.String(dbDriverArgKey))

	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	if err := db.CheckOpenmppSchemaVersion(srcDb); err != nil {
		return err
	}

	// get model metadata
	modelDef, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = modelDef.Model.Name // set model name: it can be empty and only model digest specified

	// create new output directory, use modelName subdirectory
	outDir := filepath.Join(runOpts.String(outputDirArgKey), modelName)

	if !theCfg.isKeepOutputDir {
		if ok := dirDeleteAndLog(outDir); !ok {
			return errors.New("Error: unable to delete: " + outDir)
		}
	}
	if err = os.MkdirAll(outDir, 0750); err != nil {
		return err
	}
	fileCreated := make(map[string]bool)

	// write model definition to json file
	if err = toModelJson(srcDb, modelDef, outDir); err != nil {
		return err
	}

	// use of run and set id's in directory names:
	// if true then always use id's in the names, false never use it
	// by default: only if name conflict
	doUseIdNames := defaultUseIdNames
	if runOpts.IsExist(useIdNamesArgKey) {
		if runOpts.Bool(useIdNamesArgKey) {
			doUseIdNames = yesUseIdNames
		} else {
			doUseIdNames = noUseIdNames
		}
	}
	isIdNames := false

	// write all model run data into csv files: parameters, output expressions and accumulators
	isIdCsv := runOpts.Bool(useIdCsvArgKey)

	if isIdNames, err = toRunListText(srcDb, modelDef, outDir, fileCreated, isIdCsv, doUseIdNames); err != nil {
		return err
	}

	// write all readonly workset data into csv files: input parameters
	if err = toWorksetListText(srcDb, modelDef, outDir, fileCreated, isIdCsv, isIdNames); err != nil {
		return err
	}

	// write all modeling tasks and task run history to json files
	if err = toTaskListJson(srcDb, modelDef, outDir, isIdNames); err != nil {
		return err
	}

	// pack model metadata, run results and worksets into zip
	if runOpts.Bool(zipArgKey) {
		zipPath, err := helper.PackZip(outDir, !theCfg.isKeepOutputDir, "")
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
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".profile.json"), &modelProfile); err != nil {
		return err
	}
	return nil
}

// toCellCsvFile convert parameter, output table values or microdata and write into csvDir/fileName.csv file.
// if IsIdCsv is true then csv contains enum id's, default: enum code
func toCellCsvFile(
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	readLayout interface{},
	csvCvt db.CsvConverter,
	fileCreated map[string]bool,
	csvDir string,
	extraFirstName string,
	extraFirstValue string) error {

	// converter from db cell to csv row []string
	var cvtRow func(interface{}, []string) error
	var err error
	if !csvCvt.IsUseEnumId() {
		cvtRow, err = csvCvt.ToCsvRow()
	} else {
		cvtRow, err = csvCvt.ToCsvIdRow()
	}
	if err != nil {
		return err
	}

	// create csv file or open existing for append
	fn, err := csvCvt.CsvFileName()
	if err != nil {
		return err
	}
	p := filepath.Join(csvDir, fn)
	_, isAppend := fileCreated[p]

	flag := os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	if isAppend {
		flag = os.O_APPEND | os.O_WRONLY
	}

	f, err := os.OpenFile(p, flag, 0644)
	if err != nil {
		return err
	}
	fileCreated[p] = true
	defer f.Close()

	if theCfg.isWriteUtf8Bom { // if required then write utf-8 bom
		if _, err = f.Write(helper.Utf8bom); err != nil {
			return err
		}
	}

	wr := csv.NewWriter(f)

	// if not append to already existing csv file then write header line: column names
	cs, err := csvCvt.CsvHeader()
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

	// convert cell into []string and write line into csv file
	cvtWr := func(src interface{}) (bool, error) {

		// write cell line: dimension(s) and value
		// if "all-in-one" then prepend first value, e.g.: run id
		if extraFirstValue == "" {
			if err := cvtRow(src, cs); err != nil {
				return false, err
			}
		} else {
			if err := cvtRow(src, cs[1:]); err != nil {
				return false, err
			}
		}
		if err := wr.Write(cs); err != nil {
			return false, err
		}
		return true, nil
	}

	// select parameter rows, output table rows or microdata rows and write into csv file
	switch lt := readLayout.(type) {
	case db.ReadParamLayout:
		_, err = db.ReadParameterTo(dbConn, modelDef, &lt, cvtWr)
	case db.ReadTableLayout:
		_, err = db.ReadOutputTableTo(dbConn, modelDef, &lt, cvtWr)
	case db.ReadMicroLayout:
		_, err = db.ReadMicrodataTo(dbConn, modelDef, &lt, cvtWr)
	default:
		err = errors.New("fail to write from database into CSV: layout type is unknown")
	}
	if err != nil {
		return err
	}

	// flush and return error, if any
	wr.Flush()
	return wr.Error()
}

// toDotMdFile write parameter value notes or output table values notes into .md file, for example into csvDir/ageSex.FR.md file.
func toDotMdFile(
	csvDir string,
	name string,
	text string) error {

	f, err := os.OpenFile(filepath.Join(csvDir, name+".md"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// if required then write utf-8 bom
	if theCfg.isWriteUtf8Bom {
		if _, err = f.Write(helper.Utf8bom); err != nil {
			return err
		}
	}

	// write file content
	if _, err = f.WriteString(text); err != nil {
		return err
	}
	return f.Sync()
}
