// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// copy model run from text json and csv files into database
func textToDbRun(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// get model run name and id
	runName := runOpts.String(runNameArgKey)
	runId := runOpts.Int(runIdArgKey, 0)

	if runId < 0 || runId == 0 && runName == "" {
		return errors.New("dbcopy invalid argument(s) for model run id: " + runOpts.String(runIdArgKey) + " and/or name: " + runOpts.String(runNameArgKey))
	}

	// root for run data: input directory or name of input.zip
	// it is input directory/modelName.run.id or input directory/modelName.run.runName
	// for csv files this "root" combined subdirectory: root/run.id.runName
	inpDir := ""
	if runId > 0 {
		inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".run."+strconv.Itoa(runId))
	} else {
		inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".run."+runName)
	}

	// unzip if required and use unzipped directory as "root" input diretory
	if runOpts.Bool(zipArgKey) {
		base := filepath.Base(inpDir)
		omppLog.Log("Unpack ", base, ".zip")

		outDir := runOpts.String(outputDirArgKey)
		if err := helper.UnpackZip(inpDir+".zip", outDir); err != nil {
			return err
		}
		inpDir = filepath.Join(outDir, base)
	}

	// get model run metadata json path and csv directory by run id or run name or both
	var metaPath string
	var csvDir string

	if runOpts.IsExist(runNameArgKey) && runOpts.IsExist(runIdArgKey) { // both: run id and name

		metaPath = filepath.Join(inpDir,
			modelName+".run."+strconv.Itoa(runId)+"."+helper.ToAlphaNumeric(runName)+".json")
		csvDir = filepath.Join(inpDir,
			"run."+strconv.Itoa(runId)+"."+helper.ToAlphaNumeric(runName))

	} else { // run id or run name only

		// make path search patterns for metadata json and csv directory
		var cp string
		if runOpts.IsExist(runNameArgKey) && !runOpts.IsExist(runIdArgKey) { // run name only
			cp = "run.[0-9]*." + helper.ToAlphaNumeric(runName)
		}
		if !runOpts.IsExist(runNameArgKey) && runOpts.IsExist(runIdArgKey) { // run id only
			cp = "run." + strconv.Itoa(runId) + ".*"
		}
		mp := modelName + "." + cp + ".json"

		// find path to metadata json by pattern
		fl, err := filepath.Glob(inpDir + "/" + mp)
		if err != nil {
			return err
		}
		if len(fl) <= 0 {
			return errors.New("no metadata json file found for model run: " + strconv.Itoa(runId) + " " + runName)
		}
		metaPath = fl[0]
		if len(fl) > 1 {
			omppLog.Log("found multiple model run metadata json files, using: " + filepath.Base(metaPath))
		}

		// csv directory: check if csv directory exist for that json file
		re := regexp.MustCompile("\\.run\\.([0-9]+)\\.((_|[0-9A-Za-z])+)\\.json")
		s := re.FindString(filepath.Base(metaPath))

		if len(s) > 6 { // expected match string: .run.4.q.json, csv directory: run.4.q
			csvDir = filepath.Join(inpDir, s[1:len(s)-5])
		}
	}

	// check results: metadata json file or csv directory must exist
	if metaPath == "" {
		return errors.New("no metadata json file found for model run: " + strconv.Itoa(runId) + " " + runName)
	}
	if csvDir == "" {
		return errors.New("no csv directory found for model run: " + strconv.Itoa(runId) + " " + runName)
	}
	if _, err := os.Stat(metaPath); err != nil {
		return errors.New("no metadata json file found for model run: " + strconv.Itoa(runId) + " " + runName)
	}
	if _, err := os.Stat(csvDir); err != nil {
		return errors.New("no csv directory found for model run: " + strconv.Itoa(runId) + " " + runName)
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
	dstDb, _, err := db.Open(cs, dn, true)
	if err != nil {
		return err
	}
	defer dstDb.Close()

	nv, err := db.OpenmppSchemaVersion(dstDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid database, likely not an openM++ database")
	}

	// get model metadata
	modelDef, err := db.GetModel(dstDb, modelName, modelDigest)
	if err != nil {
		return err
	}

	// get full list of languages
	langDef, err := db.GetLanguages(dstDb)
	if err != nil {
		return err
	}

	// read from metadata json and csv files and update target database
	encName := runOpts.String(encodingArgKey)

	srcId, _, err := fromRunTextToDb(dstDb, modelDef, langDef, runName, runId, metaPath, csvDir, encName)
	if err != nil {
		return err
	}
	if srcId <= 0 {
		return errors.New("model run not found or empty: " + strconv.Itoa(runId) + " " + runName)
	}

	return nil
}

// fromRunTextListToDb read all model runs (metadata, parameters, output tables)
// from csv and json files, convert it to db cells and insert into database.
// Double format is used for float model types digest calculation, if non-empty format supplied
func fromRunTextListToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, inpDir string, doubleFmt string, encodingName string,
) error {

	// get list of model run json files
	fl, err := filepath.Glob(inpDir + "/" + modelDef.Model.Name + ".run.[0-9]*.*.json")
	if err != nil {
		return err
	}
	if len(fl) <= 0 {
		return nil // no model runs
	}

	// for each file:
	// read model run metadata, update model in target database
	// read csv files from run csv subdir, update run parameters values and output tables values
	// update model run digest
	for k := range fl {

		_, _, err := fromRunTextToDb(dbConn, modelDef, langDef, "", 0, fl[k], doubleFmt, encodingName)
		if err != nil {
			return err
		}
	}

	return nil
}

// fromRunTextToDb read model run metadata from json file,
// read from csv files parameter values, output tables values convert it to db cells and insert into database,
// and finally update model run digest.
// Double format is used for float model types digest calculation, if non-empty format supplied
// it return source run id (run id from metadata json file) and destination run id
func fromRunTextToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, srcName string, srcId int, metaPath string, doubleFmt string, encodingName string,
) (int, int, error) {

	// if no metadata file then exit: nothing to do
	if metaPath == "" {
		return 0, 0, nil // no model run metadata
	}

	// get model run metadata
	// model name and set name must be specified as parameter or inside of metadata json
	var pub db.RunPub
	isExist, err := helper.FromJsonFile(metaPath, &pub)
	if err != nil {
		return 0, 0, err
	}
	if !isExist {
		return 0, 0, nil // no model run
	}

	// run id: parse json file name to get source run id
	if srcId <= 0 {
		re := regexp.MustCompile("\\.run\\.([0-9]+)\\.")
		s2 := re.FindStringSubmatch(filepath.Base(metaPath))
		if len(s2) >= 2 {
			srcId, _ = strconv.Atoi(s2[1]) // if any error source run id remain default zero
		}
	}

	// run name: use run name from json metadata, if empty
	if pub.Name != "" && srcName != pub.Name {
		srcName = pub.Name
	}

	// check if run subdir exist
	csvDir := filepath.Join(filepath.Dir(metaPath), "run."+strconv.Itoa(srcId)+"."+helper.ToAlphaNumeric(pub.Name))

	if _, err := os.Stat(csvDir); err != nil {
		return 0, 0, errors.New("run directory not found: " + strconv.Itoa(srcId) + " " + pub.Name)
	}

	// destination: convert from "public" format into destination db rows
	meta, err := pub.FromPublic(dbConn, modelDef)
	if err != nil {
		return 0, 0, err
	}

	// save model run
	isExist, err = meta.UpdateRun(dbConn, modelDef, langDef)
	if err != nil {
		return 0, 0, err
	}
	dstId := meta.Run.RunId
	if isExist { // exit if model run already exist
		omppLog.Log("Model run ", srcId, " ", srcName, " already exists as ", dstId)
		return srcId, dstId, nil
	}

	omppLog.Log("Model run from ", srcId, " ", srcName, " to ", dstId)

	// restore run parameters: all model parameters must be included in the run
	layout := db.WriteLayout{ToId: meta.Run.RunId, IsToRun: true}

	for j := range modelDef.Param {

		// read parameter values from csv file
		var cell db.Cell
		cLst, err := fromCsvFile(csvDir, modelDef, modelDef.Param[j].Name, &cell, encodingName)
		if err != nil {
			return 0, 0, err
		}
		if cLst == nil || cLst.Len() <= 0 {
			return 0, 0, errors.New("run: " + strconv.Itoa(srcId) + " " + meta.Run.Name + " parameter empty: " + modelDef.Param[j].Name)
		}

		// insert parameter values in model run
		layout.Name = modelDef.Param[j].Name

		if err = db.WriteParameter(dbConn, modelDef, &layout, cLst, doubleFmt); err != nil {
			return 0, 0, err
		}
	}

	// restore run output tables accumulators and expressions
	for j := range modelDef.Table {

		// read output table accumulator(s) values from csv file
		var ca db.CellAcc
		acLst, err := fromCsvFile(csvDir, modelDef, modelDef.Table[j].Name, &ca, encodingName)
		if err != nil {
			return 0, 0, err
		}

		// read output table expression(s) values from csv file
		var ce db.CellExpr
		ecLst, err := fromCsvFile(csvDir, modelDef, modelDef.Table[j].Name, &ce, encodingName)
		if err != nil {
			return 0, 0, err
		}

		// insert output table values (accumulators and expressions) in model run
		layout.Name = modelDef.Table[j].Name
		if err = db.WriteOutputTable(dbConn, modelDef, &layout, acLst, ecLst, doubleFmt); err != nil {
			return 0, 0, err
		}
	}

	// update model run digest
	if meta.Run.Digest == "" {

		sd, err := db.UpdateRunDigest(dbConn, dstId)
		if err != nil {
			return 0, 0, err
		}
		meta.Run.Digest = sd
	}

	return srcId, dstId, nil
}
