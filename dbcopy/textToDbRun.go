// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// copy model run from text json and csv files into database
func textToDbRun(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// get model run id and/or digest and/or name
	runId := runOpts.Int(runIdArgKey, 0)
	runDigest := runOpts.String(runDigestArgKey)
	runName := runOpts.String(runNameArgKey)
	if runId < 0 || runId == 0 && runDigest == "" && runName == "" {
		return errors.New("dbcopy invalid argument(s) for model run: " + runOpts.String(runIdArgKey) + " " + runOpts.String(runNameArgKey) + " " + runOpts.String(runDigestArgKey))
	}

	// root directory of run data is input directory or name of input.zip, result is one of:
	// input/modelName.run.id
	// input/modelName.run.runName
	// input/modelName.run.runDigest
	// for csv files this "root" combined subdirectory: root/run.id.runName or root/run.runName
	inpDir := ""
	switch {
	case runId > 0:
		inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".run."+strconv.Itoa(runId))
	case runDigest != "":
		inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".run."+helper.CleanPath(runDigest))
	default:
		// if not run id and not digest then run name
		inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".run."+helper.CleanPath(runName))
	}

	// unzip if required and use unzipped directory as "root" input directory
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

	if runOpts.IsExist(runNameArgKey) && runOpts.IsExist(runIdArgKey) { // both: run id and name

		metaPath = filepath.Join(inpDir,
			modelName+".run."+strconv.Itoa(runId)+"."+helper.CleanPath(runName)+".json")

	} else { // only run id or run name and/or run digest

		// make path search patterns for metadata json and csv directory
		var mp string
		switch {
		case runOpts.IsExist(runNameArgKey) && !runOpts.IsExist(runIdArgKey): // run name and not run id
			mp = modelName + ".run.*" + helper.CleanPath(runName) + ".json"
		case !runOpts.IsExist(runNameArgKey) && runOpts.IsExist(runIdArgKey): // run id and not run name
			mp = modelName + ".run." + strconv.Itoa(runId) + ".*.json"
		default:
			// run digest and no run name or run id
			mp = modelName + ".run.*.json"
		}

		// find path to metadata json by pattern
		fl, err := filepath.Glob(inpDir + "/" + mp)
		if err != nil {
			return err
		}
		if len(fl) <= 0 {
			return errors.New("no metadata json file found for model run: " + strconv.Itoa(runId) + " " + runName + " " + runDigest)
		}
		metaPath = fl[0]
		if len(fl) > 1 {
			omppLog.Log("found multiple model run metadata json files, using: " + filepath.Base(metaPath))
		}
	}

	// check results: metadata json file or csv directory must exist
	if metaPath == "" {
		return errors.New("no metadata json file found for model run: " + strconv.Itoa(runId) + " " + runName + " " + runDigest)
	}
	if _, err := os.Stat(metaPath); err != nil {
		return errors.New("no metadata json file found for model run: " + strconv.Itoa(runId) + " " + runName + " " + runDigest)
	}

	// open source database connection and check is it valid
	dn := runOpts.String(toDbDriverArgKey)
	if dn == "" && runOpts.IsExist(dbDriverArgKey) {
		dn = runOpts.String(dbDriverArgKey)
	}
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(toSqliteArgKey), runOpts.String(toDbConnStrArgKey), dn)

	dstDb, _, err := db.Open(cs, dn, true)
	if err != nil {
		return err
	}
	defer dstDb.Close()

	if err := db.CheckOpenmppSchemaVersion(dstDb); err != nil {
		return err
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
	dblFmt := runOpts.String(doubleFormatArgKey)
	encName := runOpts.String(encodingArgKey)

	dstId, err := fromRunTextToDb(dstDb, modelDef, langDef, runName, metaPath, dblFmt, encName)
	if err != nil {
		return err
	}
	if dstId <= 0 {
		return errors.New("model run not found or empty: " + strconv.Itoa(runId) + " " + runName + " " + runDigest)
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
	fl, err := filepath.Glob(inpDir + "/" + modelDef.Model.Name + ".run.*.json")
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

		_, err := fromRunTextToDb(dbConn, modelDef, langDef, "", fl[k], doubleFmt, encodingName)
		if err != nil {
			return err
		}
	}

	return nil
}

// fromRunTextToDb read model run metadata from json file,
// read from csv files parameter values and output tables values,
// convert it to db cells and insert into database,
// and finally update model run digest.
// Double format is used for float model types digest calculation, if non-empty format supplied
// it return source run id (run id from metadata json file) and destination run id
func fromRunTextToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, srcName string, metaPath string, doubleFmt string, encodingName string,
) (int, error) {

	// if no metadata file then exit: nothing to do
	if metaPath == "" {
		return 0, nil // no model run metadata
	}

	// get model run metadata
	// model name and set name must be specified as parameter or inside of metadata json
	var pub db.RunPub
	isExist, err := helper.FromJsonFile(metaPath, &pub)
	if err != nil {
		return 0, err
	}
	if !isExist {
		return 0, nil // no model run
	}

	// check if run subdir exist
	d, f := filepath.Split(metaPath)
	c := strings.TrimSuffix(strings.TrimPrefix(f, pub.ModelName+"."), ".json")

	csvDir := filepath.Join(d, c)
	if _, err := os.Stat(csvDir); err != nil {
		return 0, errors.New("csv run directory not found: " + c)
	}

	// run name: use run name from json metadata if json metadata not empty, else use supplied run name
	if pub.Name != "" && srcName != pub.Name {
		srcName = pub.Name
	}

	// destination: convert from "public" format into destination db rows
	meta, err := pub.FromPublic(dbConn, modelDef)
	if err != nil {
		return 0, err
	}

	// save model run
	isExist, err = meta.UpdateRun(dbConn, modelDef, langDef, doubleFmt)
	if err != nil {
		return 0, err
	}
	dstId := meta.Run.RunId
	if isExist { // exit if model run already exist
		omppLog.Log("Model run ", srcName, " already exists as ", dstId)
		return dstId, nil
	}

	omppLog.Log("Model run from ", srcName, " into id: ", dstId)

	// restore run parameters: all model parameters must be included in the run
	paramLt := db.WriteParamLayout{
		WriteLayout: db.WriteLayout{ToId: meta.Run.RunId},
		DoubleFmt:   doubleFmt,
		IsToRun:     true}

	for j := range modelDef.Param {

		// read parameter values from csv file
		var cell db.CellParam
		cLst, err := fromCsvFile(csvDir, modelDef, modelDef.Param[j].Name, meta.Param[j].SubCount, &cell, encodingName)
		if err != nil {
			return 0, err
		}
		if cLst == nil || cLst.Len() <= 0 {
			return 0, errors.New("run: " + meta.Run.Name + " parameter empty: " + modelDef.Param[j].Name)
		}

		// insert parameter values in model run
		paramLt.Name = modelDef.Param[j].Name
		paramLt.SubCount = meta.Param[j].SubCount

		if err = db.WriteParameter(dbConn, modelDef, &paramLt, cLst); err != nil {
			return 0, err
		}
	}

	// restore run output tables accumulators and expressions, if the table included in run results
	tblLt := db.WriteTableLayout{
		WriteLayout: db.WriteLayout{ToId: meta.Run.RunId},
		DoubleFmt:   doubleFmt}

	for j := range modelDef.Table {

		// check if table exist in model run results
		var isFound bool
		for k := range meta.Table {
			isFound = meta.Table[k].TableHid == modelDef.Table[j].TableHid
			if isFound {
				break
			}
		}
		if !isFound {
			continue // skip table: it is suppressed and not in run results
		}

		// read output table accumulator(s) values from csv file
		var ca db.CellAcc
		acLst, err := fromCsvFile(csvDir, modelDef, modelDef.Table[j].Name, meta.Run.SubCount, &ca, encodingName)
		if err != nil {
			return 0, err
		}

		// read output table expression(s) values from csv file
		var ce db.CellExpr
		ecLst, err := fromCsvFile(csvDir, modelDef, modelDef.Table[j].Name, meta.Run.SubCount, &ce, encodingName)
		if err != nil {
			return 0, err
		}

		// insert output table values (accumulators and expressions) in model run
		tblLt.Name = modelDef.Table[j].Name
		if err = db.WriteOutputTable(dbConn, modelDef, &tblLt, acLst, ecLst); err != nil {
			return 0, err
		}
	}

	// update model run digest
	if meta.Run.ValueDigest == "" {

		svd, err := db.UpdateRunValueDigest(dbConn, dstId)
		if err != nil {
			return 0, err
		}
		meta.Run.ValueDigest = svd
	}

	return dstId, nil
}
