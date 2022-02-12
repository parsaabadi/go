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
	isWriteAccum := !runOpts.Bool(noAccumCsv)

	if err = toRunListText(srcDb, modelDef, outDir, dblFmt, isIdCsv, isWriteUtf8bom, isUseIdNames, isWriteAccum); err != nil {
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

// toCellCsvFile convert parameter or output table values and write into csvDir/fileName.csv file.
// if isIdCsv is true then csv contains enum id's, default: enum code
// The readLayout argument is db.ReadParamLayout if isParam is true else it is db.ReadTableLayout
func toCellCsvFile(
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	name string,
	isParam bool,
	readLayout interface{},
	csvCvt db.CsvConverter,
	isAppend bool,
	csvDir string,
	isIdCsv bool,
	isWriteUtf8bom bool,
	extraFirstName string,
	extraFirstValue string) error {

	// converter from db cell to csv row []string
	var cvtRow func(interface{}, []string) error
	var err error
	if !isIdCsv {
		cvtRow, err = csvCvt.CsvToRow(modelDef, name)
	} else {
		cvtRow, err = csvCvt.CsvToIdRow(modelDef, name)
	}
	if err != nil {
		return err
	}

	// create csv file or open existing for append
	fn, err := csvCvt.CsvFileName(modelDef, name, isIdCsv)
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
	cs, err := csvCvt.CsvHeader(modelDef, name)
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

	// convert output table cell into []string and write line into csv file
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

	// select parameter or output table rows and write into csv file
	if isParam {
		lt, ok := readLayout.(db.ReadParamLayout)
		if !ok {
			return errors.New("invalid type, expected: ReadParamLayout (internal error)")
		}
		_, err = db.ReadParameterTo(dbConn, modelDef, &lt, cvtWr)
		if err != nil {
			return err
		}
	} else {
		lt, ok := readLayout.(db.ReadTableLayout)
		if !ok {
			return errors.New("invalid type, expected: ReadTableLayout (internal error)")
		}
		_, err = db.ReadOutputTableTo(dbConn, modelDef, &lt, cvtWr)
		if err != nil {
			return err
		}
	}

	// flush and return error, if any
	wr.Flush()
	return wr.Error()
}

// toMdFile write parameter value notes or output table values notes into Md file, for example into csvDir/ageSex.FR.md file.
func toMdFile(
	csvDir string,
	name string,
	isWriteUtf8bom bool,
	text string) error {

	f, err := os.OpenFile(filepath.Join(csvDir, name+".md"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// if required then write utf-8 bom
	if isWriteUtf8bom {
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
