// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"database/sql"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// copy model from text json and csv files into database
func textToDb(modelName string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// get connection string and driver name
	cs := runOpts.String(toDbConnStrArgKey)
	// use OpenM options if DBCopy ouput database not defined
	//	if cs == "" && runOpts.IsExist(dbConnStrArgKey) {
	//		cs = runOpts.String(dbConnStrArgKey)
	//	}

	dn := runOpts.String(toDbDriverArgKey)
	if dn == "" && runOpts.IsExist(dbDriverArgKey) {
		dn = runOpts.String(dbDriverArgKey)
	}

	cs, dn = db.IfEmptyMakeDefault(modelName, cs, dn)

	// open destination database and check is it valid
	dstDb, dbFacet, err := db.Open(cs, dn, true)
	if err != nil {
		return err
	}
	defer dstDb.Close()

	nv, err := db.OpenmppSchemaVersion(dstDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid database, likely not an openM++ database")
	}

	// use modelName as subdirectory inside of input and output directories or as name of model.zip file
	inpDir := runOpts.String(inputDirArgKey)

	if !runOpts.Bool(zipArgKey) {
		inpDir = filepath.Join(inpDir, modelName) // json and csv files located in modelName subdir
	} else {
		omppLog.Log("Unpack ", modelName, ".zip")

		outDir := runOpts.String(outputDirArgKey)
		if err = helper.UnpackZip(filepath.Join(inpDir, modelName+".zip"), outDir); err != nil {
			return err
		}
		inpDir = filepath.Join(outDir, modelName)
	}

	// insert model metadata from json file into database
	modelDef, err := fromModelJsonToDb(dstDb, dbFacet, inpDir, modelName)
	if err != nil {
		return err
	}

	// insert languages and model text metadata from json file into database
	langDef, err := fromLangTextJsonToDb(dstDb, modelDef, inpDir)
	if err != nil {
		return err
	}

	// insert model runs data from csv into database:
	// parameters, output expressions and accumulators
	encName := runOpts.String(encodingArgKey)

	if err = fromRunTextListToDb(dstDb, modelDef, langDef, inpDir, runOpts.String(doubleFormatArgKey), encName); err != nil {
		return err
	}

	// insert model workset data from csv into database: input parameters
	if err = fromWorksetTextListToDb(dstDb, modelDef, langDef, inpDir, encName); err != nil {
		return err
	}

	// insert modeling tasks and tasks run history from json file into database
	if err = fromTaskListJsonToDb(dstDb, modelDef, langDef, inpDir); err != nil {
		return err
	}
	return nil
}

// fromModelJsonToDb reads model metadata from json file and insert it into database.
func fromModelJsonToDb(dbConn *sql.DB, dbFacet db.Facet, inpDir string, modelName string) (*db.ModelMeta, error) {

	// restore  model metadta from json
	js, err := helper.FileToUtf8(filepath.Join(inpDir, modelName+".model.json"), "")
	if err != nil {
		return nil, err
	}
	modelDef := &db.ModelMeta{}

	isExist, err := modelDef.FromJson([]byte(js))
	if err != nil {
		return nil, err
	}
	if !isExist {
		return nil, errors.New("model not found: " + modelName)
	}
	if modelDef.Model.Name != modelName {
		return nil, errors.New("model name: " + modelName + " not found in .json file")
	}

	// insert model metadata into destination database if not exists
	if err = db.UpdateModel(dbConn, dbFacet, modelDef); err != nil {
		return nil, err
	}

	// insert, update or delete model default profile
	var modelProfile db.ProfileMeta
	isExist, err = helper.FromJsonFile(filepath.Join(inpDir, modelName+".profile.json"), &modelProfile)
	if err != nil {
		return nil, err
	}
	if isExist && modelProfile.Name == modelName { // if this is profile default model profile then do update
		if err = db.UpdateProfile(dbConn, &modelProfile); err != nil {
			return nil, err
		}
	}

	return modelDef, nil
}

// fromLangTextJsonToDb reads languages, model text and model groups from json file and insert it into database.
func fromLangTextJsonToDb(dbConn *sql.DB, modelDef *db.ModelMeta, inpDir string) (*db.LangMeta, error) {

	// restore language list from json and if exist then update db tables
	js, err := helper.FileToUtf8(filepath.Join(inpDir, modelDef.Model.Name+".lang.json"), "")
	if err != nil {
		return nil, err
	}
	langDef := &db.LangMeta{}

	isExist, err := langDef.FromJson([]byte(js))
	if err != nil {
		return nil, err
	}
	if isExist {
		if err = db.UpdateLanguage(dbConn, langDef); err != nil {
			return nil, err
		}
	}

	// get full list of languages
	langDef, err = db.GetLanguages(dbConn)
	if err != nil {
		return nil, err
	}

	// restore text data from json and if exist then update db tables
	var modelTxt db.ModelTxtMeta
	isExist, err = helper.FromJsonFile(filepath.Join(inpDir, modelDef.Model.Name+".text.json"), &modelTxt)
	if err != nil {
		return nil, err
	}
	if isExist {
		if err = db.UpdateModelText(dbConn, modelDef, langDef, &modelTxt); err != nil {
			return nil, err
		}
	}

	// restore model language-specific strings from json and if exist then update db table
	var mwDef db.ModelWordMeta
	isExist, err = helper.FromJsonFile(filepath.Join(inpDir, modelDef.Model.Name+".word.json"), &mwDef)
	if err != nil {
		return nil, err
	}
	if isExist {
		if err = db.UpdateModelWord(dbConn, modelDef, langDef, &mwDef); err != nil {
			return nil, err
		}
	}

	// restore model groups and groups text (description, notes) from json and if exist then update db tables
	var modelGroup db.GroupMeta
	isExist, err = helper.FromJsonFile(filepath.Join(inpDir, modelDef.Model.Name+".group.json"), &modelGroup)
	if err != nil {
		return nil, err
	}
	if isExist {
		if err = db.UpdateModelGroup(dbConn, modelDef, langDef, &modelGroup); err != nil {
			return nil, err
		}
	}

	return langDef, nil
}

// fromCsvFile read parameter or output table csv file and convert it to list of db cells
func fromCsvFile(
	csvDir string, modelDef *db.ModelMeta, name string, subCount int, cell db.CsvConverter, encodingName string) (*list.List, error) {

	// converter from csv row []string to db cell
	cvt, err := cell.CsvToCell(modelDef, name, subCount, "")
	if err != nil {
		return nil, errors.New("invalid converter from csv row: " + err.Error())
	}

	// open csv file, convert to utf-8 and parse csv into db cells
	fn, err := cell.CsvFileName(modelDef, name)
	if err != nil {
		return nil, errors.New("invalid csv file name: " + err.Error())
	}

	f, err := os.Open(filepath.Join(csvDir, fn))
	if err != nil {
		return nil, errors.New("csv file open error: " + err.Error())
	}
	defer f.Close()

	uRd, err := helper.Utf8Reader(f, encodingName)
	if err != nil {
		return nil, errors.New("fail to create utf-8 converter: " + err.Error())
	}

	rd := csv.NewReader(uRd)
	rd.TrimLeadingSpace = true

	// read csv file and convert and append lines into cell list
	cLst := list.New()
	isFirst := true
ReadFor:
	for {
		row, err := rd.Read()
		switch {
		case err == io.EOF:
			break ReadFor
		case err != nil:
			return nil, errors.New("csv file read error: " + err.Error())
		}

		// skip header line
		if isFirst {
			isFirst = false
			continue
		}

		// convert and append cell to cell list
		c, err := cvt(row)
		if err != nil {
			return nil, errors.New("csv file row convert error: " + err.Error())
		}
		cLst.PushBack(c)
	}

	return cLst, nil
}
