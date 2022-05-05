// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// copy model from text json and csv files into database
func textToDb(modelName string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// open source database connection and check is it valid
	dn := runOpts.String(toDbDriverArgKey)
	if dn == "" && runOpts.IsExist(dbDriverArgKey) {
		dn = runOpts.String(dbDriverArgKey)
	}
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(toSqliteArgKey), runOpts.String(toDbConnStrArgKey), dn)

	dstDb, dbFacet, err := db.Open(cs, dn, true)
	if err != nil {
		return err
	}
	defer dstDb.Close()

	if err := db.CheckOpenmppSchemaVersion(dstDb); err != nil {
		return err
	}

	// use modelName as subdirectory inside of input and output directories or as name of model.zip file
	inpDir := runOpts.String(inputDirArgKey)

	if !runOpts.Bool(zipArgKey) {
		inpDir = filepath.Join(inpDir, modelName) // json and csv files located in modelName subdir
	} else {
		omppLog.Log("Unpack ", modelName, ".zip")

		outDir := runOpts.String(outputDirArgKey)
		if outDir == "" {
			outDir = filepath.Dir(inpDir)
		}
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
	dblFmt := runOpts.String(doubleFormatArgKey)
	encName := runOpts.String(encodingArgKey)
	isNoModelDigestCheck := runOpts.Bool(noDigestCheck)

	if err = fromRunTextListToDb(dstDb, modelDef, langDef, inpDir, isNoModelDigestCheck, dblFmt, encName); err != nil {
		return err
	}

	// insert model workset data from csv into database: input parameters
	if err = fromWorksetTextListToDb(dstDb, modelDef, langDef, inpDir, isNoModelDigestCheck, dblFmt, encName); err != nil {
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
	if _, err = db.UpdateModel(dbConn, dbFacet, modelDef); err != nil {
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

// fromLangTextJsonToDb reads languages and model text from json file and insert it into database.
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

	return langDef, nil
}

// fromTableCsvFile read output table csv files (accumulators and expressions) and write it into db output tables.
func fromTableCsvFile(
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	layout db.WriteTableLayout,
	csvDir string,
	cvtExpr db.CellExprConverter,
	cvtAcc db.CellAccConverter,
	encodingName string) error {

	// accumulator converter from csv row []string to db cell
	aToCell, err := cvtAcc.CsvToCell(modelDef, layout.Name, layout.SubCount)
	if err != nil {
		return errors.New("invalid converter from accumulators csv row: " + err.Error())
	}

	// open accumulators csv file
	aFn, err := cvtAcc.CsvFileName(modelDef, layout.Name, false)
	if err != nil {
		return errors.New("invalid accumulators csv file name: " + err.Error())
	}
	ahs, err := cvtAcc.CsvHeader(modelDef, layout.Name)
	if err != nil {
		return errors.New("Error at building csv accumulators header " + layout.Name + ": " + err.Error())
	}
	ah := strings.Join(ahs, ",")

	accFile, err := os.Open(filepath.Join(csvDir, aFn))
	if err != nil {
		return errors.New("accumulators csv file open error: " + err.Error())
	}
	defer accFile.Close()

	accFrom, err := makeFromCsvReader(aFn, accFile, encodingName, ah, aToCell)
	if err != nil {
		return errors.New("fail to create expressions csv reader: " + err.Error())
	}

	// expression converter from csv row []string to db cell
	eToCell, err := cvtExpr.CsvToCell(modelDef, layout.Name, layout.SubCount)
	if err != nil {
		return errors.New("invalid converter from expressions csv row: " + err.Error())
	}

	// open expressions csv file
	eFn, err := cvtExpr.CsvFileName(modelDef, layout.Name, false)
	if err != nil {
		return errors.New("invalid expressions csv file name: " + err.Error())
	}
	ehs, err := cvtExpr.CsvHeader(modelDef, layout.Name)
	if err != nil {
		return errors.New("Error at building csv expressions header " + layout.Name + ": " + err.Error())
	}
	eh := strings.Join(ehs, ",")

	exprFile, err := os.Open(filepath.Join(csvDir, eFn))
	if err != nil {
		return errors.New("expressions csv file open error: " + err.Error())
	}
	defer exprFile.Close()

	exprFrom, err := makeFromCsvReader(eFn, exprFile, encodingName, eh, eToCell)
	if err != nil {
		return errors.New("fail to create expressions csv reader: " + err.Error())
	}

	// write each accumulator(s) csv rows into accumulator(s) output table
	// write each expression(s) csv rows into expression(s) output table
	err = db.WriteOutputTableFrom(dbConn, modelDef, &layout, accFrom, exprFrom)
	if err != nil {
		return err
	}

	return nil
}

// return closure to iterate over csv file rows
func makeFromCsvReader(
	fileName string, csvFile *os.File, encodingName string, csvHeader string, csvToCell func(row []string) (interface{}, error),
) (func() (interface{}, error), error) {

	// create csv reader from utf-8 line
	uRd, err := helper.Utf8Reader(csvFile, encodingName)
	if err != nil {
		return nil, errors.New("fail to create utf-8 converter: " + err.Error())
	}

	csvRd := csv.NewReader(uRd)
	csvRd.TrimLeadingSpace = true
	csvRd.ReuseRecord = true

	// skip header line
	fhs, e := csvRd.Read()
	switch {
	case e == io.EOF:
		return nil, errors.New("invalid (empty) csv file: " + fileName)
	case err != nil:
		return nil, errors.New("csv file read error: " + fileName + ": " + err.Error())
	}
	fh := strings.Join(fhs, ",")
	if strings.HasPrefix(fh, string(helper.Utf8bom)) {
		fh = fh[len(helper.Utf8bom):]
	}
	if fh != csvHeader {
		return nil, errors.New("Invalid csv file header " + fileName + ": " + fh + " expected: " + csvHeader)
	}

	// convert each csv line into cell (id cell)
	// reading from .id.csv files not supported by converters
	from := func() (interface{}, error) {
		row, err := csvRd.Read()
		switch {
		case err == io.EOF:
			return nil, nil // eof
		case err != nil:
			return nil, errors.New("csv file read error: " + fileName + ": " + err.Error())
		}

		// convert csv line to cell and return from reader
		c, err := csvToCell(row)
		if err != nil {
			return nil, errors.New("csv file row convert error: " + fileName + ": " + err.Error())
		}
		return c, nil
	}
	return from, nil
}
