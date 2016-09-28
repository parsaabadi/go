// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"database/sql"
	"encoding/csv"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	omppLog "go.openmpp.org/ompp/log"
)

// copy model from text json and csv files into database
func textToDb(modelName string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// get connection string and driver name
	// use OpenM options if DBCopy ouput database not defined
	cs := runOpts.String(toDbConnectionStr)
	if cs == "" && runOpts.IsExist(config.DbConnectionStr) {
		cs = runOpts.String(config.DbConnectionStr)
	}

	dn := runOpts.String(toDbDriverName)
	if dn == "" && runOpts.IsExist(config.DbDriverName) {
		dn = runOpts.String(config.DbDriverName)
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
	runIdMap, err := fromRunTextListToDb(dstDb, modelDef, langDef, inpDir, runOpts.String(doubleFmtArgKey))
	if err != nil {
		return err
	}

	// insert model workset data from csv into database: input parameters
	setIdMap, err := fromWorksetTextListToDb(dstDb, modelDef, langDef, inpDir, runIdMap)
	if err != nil {
		return err
	}

	// insert modeling tasks and tasks run history from json file into database
	if err = fromTaskListJsonToDb(dstDb, modelDef, langDef, inpDir, runIdMap, setIdMap); err != nil {
		return err
	}
	return nil
}

// copy workset from text json and csv files into database
func textToDbWorkset(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// get workset name and id
	setName := runOpts.String(config.SetName)
	setId := runOpts.Int(config.SetId, 0)

	if setId < 0 || setId == 0 && setName == "" {
		return errors.New("dbcopy invalid argument(s) for set id: " + runOpts.String(config.SetId) + " and/or set name: " + runOpts.String(config.SetName))
	}

	// root for workset data: input directory or name of input.zip
	// it is parameter directory (if specified) or input directory/modelName
	// later this "root" combined with set name subdirectory: root/setName
	inpDir := ""
	if runOpts.IsExist(config.ParamDir) {
		inpDir = filepath.Clean(runOpts.String(config.ParamDir))
	} else {
		inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName)
	}

	if runOpts.Bool(zipArgKey) {
		base := filepath.Base(inpDir)
		omppLog.Log("Unpack ", base, ".zip")

		outDir := runOpts.String(outputDirArgKey)
		if err := helper.UnpackZip(inpDir+".zip", outDir); err != nil {
			return err
		}
		inpDir = filepath.Join(outDir, base)
	}

	// get workset metadata file name by set id or set name or both
	var metaPath string

	if runOpts.IsExist(config.SetName) && runOpts.IsExist(config.SetId) { // both: set id and name

		metaPath = filepath.Join(inpDir,
			modelName+".set."+strconv.Itoa(setId)+"."+helper.ToAlphaNumeric(setName)+".json")

	} else { // set id or set name only

		// make file search pattern
		var namePattern string
		if runOpts.IsExist(config.SetName) && !runOpts.IsExist(config.SetId) { // set name only
			namePattern = modelName + ".set.[0-9]*." + helper.ToAlphaNumeric(setName) + ".json"
		}
		if !runOpts.IsExist(config.SetName) && runOpts.IsExist(config.SetId) { // set id only
			namePattern = modelName + ".set." + strconv.Itoa(setId) + ".*.json"
		}

		// find file name by the pattern: it must be single workset metadata file
		fl, err := filepath.Glob(inpDir + "/" + namePattern)
		if err != nil {
			return err
		}
		if len(fl) == 1 {
			metaPath = fl[0]
		} else {
			if len(fl) <= 0 {
				return errors.New("workset metadata json file not found: " + strconv.Itoa(setId) + " " + setName)
			}
			if len(fl) > 1 {
				return errors.New("found multiple workset metadata json files, it must be single file: " + strconv.Itoa(setId) + " " + setName)
			}
		}
	}

	// check if metadata json file file exist
	if _, err := os.Stat(metaPath); err != nil {
		return errors.New("workset metadata json file not found or empty: " + strconv.Itoa(setId) + " " + setName)
	}

	// get connection string and driver name
	// use OpenM options if DBCopy ouput database not defined
	cs := runOpts.String(toDbConnectionStr)
	if cs == "" && runOpts.IsExist(config.DbConnectionStr) {
		cs = runOpts.String(config.DbConnectionStr)
	}

	dn := runOpts.String(toDbDriverName)
	if dn == "" && runOpts.IsExist(config.DbDriverName) {
		dn = runOpts.String(config.DbDriverName)
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

	// TODO: run id based on run digest
	runIdMap := make(map[int]int)

	// read from metadata json and csv files and update target database
	srcId, _, err := fromWorksetTextToDb(dstDb, modelDef, langDef, metaPath, runIdMap)
	if err != nil {
		return err
	}
	if srcId <= 0 {
		return errors.New("workset not found or empty: " + strconv.Itoa(setId) + " " + setName)
	}

	return nil
}

// fromModelJsonToDb reads model metadata from json file and insert it into database.
func fromModelJsonToDb(dbConn *sql.DB, dbFacet db.Facet, inpDir string, modelName string) (*db.ModelMeta, error) {

	// restore  model metadta from json
	js, err := ioutil.ReadFile(filepath.Join(inpDir, modelName+".model.json"))
	if err != nil {
		return nil, err
	}
	modelDef := &db.ModelMeta{}

	isExist, err := modelDef.FromJson(js)
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
func fromLangTextJsonToDb(dbConn *sql.DB, modelDef *db.ModelMeta, inpDir string) (*db.LangList, error) {

	// restore language list from json and if exist then update db tables
	js, err := ioutil.ReadFile(filepath.Join(inpDir, modelDef.Model.Name+".lang.json"))
	if err != nil {
		return nil, err
	}
	langDef := &db.LangList{}

	isExist, err := langDef.FromJson(js)
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

// fromRunTextListToDb read all model runs (metadata, parameters, output tables)
// from csv and json files, convert it to db cells and insert into database.
// Double format is used for float model types digest calculation, if non-empty format supplied
func fromRunTextListToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangList, inpDir string, doubleFmt string) (map[int]int, error) {

	// get list of model run json files
	fl, err := filepath.Glob(inpDir + "/" + modelDef.Model.Name + ".run.[0-9]*.*.json")
	if err != nil {
		return nil, err
	}
	runIdMap := make(map[int]int, len(fl)) // map[source run id] => destination runt id
	if len(fl) <= 0 {
		return runIdMap, nil // no model runs
	}

	// for each file:
	// read model run metadata, update model in target database
	// read csv files from run csv subdir, update run parameters values and output tables values
	// update model run digest
	for k := range fl {

		srcId, dstId, err := fromRunTextToDb(dbConn, modelDef, langDef, fl[k], doubleFmt)
		if err != nil {
			return nil, err
		}
		if srcId > 0 {
			runIdMap[srcId] = dstId // update run id with actual id value from database
		}
	}

	return runIdMap, nil
}

// fromRunTextToDb read model run metadatafrom json file,
// read from csv files parameter values, output tables values convert it to db cells and insert into database,
// and finally update model run digest.
// Double format is used for float model types digest calculation, if non-empty format supplied
// it return source run id (run id from metadata json file) and destination run id
func fromRunTextToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangList, metaPath string, doubleFmt string) (int, int, error) {

	// get model run metadata
	var meta db.RunMeta
	isExist, err := helper.FromJsonFile(metaPath, &meta)
	if err != nil {
		return 0, 0, err
	}
	if !isExist {
		return 0, 0, nil // no model run
	}

	// save model run
	// update incoming run id's with actual new run id created in database
	srcId := meta.Run.RunId

	isExist, err = db.UpdateRun(dbConn, modelDef, langDef, &meta)
	if err != nil {
		return 0, 0, err
	}
	dstId := meta.Run.RunId
	if isExist { // exit if model run already exist
		omppLog.Log("Model run ", srcId, " already exists as ", dstId)
		return srcId, dstId, nil
	}

	omppLog.Log("Model run from ", srcId, " to ", dstId)

	// check if run subdir exist
	csvDir := filepath.Join(filepath.Dir(metaPath), "run."+strconv.Itoa(srcId)+"."+helper.ToAlphaNumeric(meta.Run.Name))

	if _, err := os.Stat(csvDir); err != nil {
		return 0, 0, errors.New("run directory not found: " + strconv.Itoa(srcId) + " " + meta.Run.Name)
	}

	// restore run parameters: all model parameters must be included in the run
	layout := db.WriteLayout{ToId: meta.Run.RunId, IsToRun: true}

	for j := range modelDef.Param {

		// read parameter values from csv file
		var cell db.Cell
		cLst, err := fromCsvFile(csvDir, modelDef, modelDef.Param[j].Name, &cell)
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
		acLst, err := fromCsvFile(csvDir, modelDef, modelDef.Table[j].Name, &ca)
		if err != nil {
			return 0, 0, err
		}

		// read output table expression(s) values from csv file
		var ce db.CellExpr
		ecLst, err := fromCsvFile(csvDir, modelDef, modelDef.Table[j].Name, &ce)
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

// fromWorksetTextListToDb read all worksets parameters from csv and json files,
// convert it to db cells and insert into database
// update set id's and base run id's with actual id in database
func fromWorksetTextListToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangList, inpDir string, runIdMap map[int]int) (map[int]int, error) {

	// get list of workset json files
	fl, err := filepath.Glob(inpDir + "/" + modelDef.Model.Name + ".set.[0-9]*.*.json")
	if err != nil {
		return nil, err
	}
	setIdMap := make(map[int]int, len(fl)) // map[source set id] => destination set id
	if len(fl) <= 0 {
		return setIdMap, nil // no worksets
	}

	// for each file:
	// read workset metadata, update workset in target database
	// read csv files from workset csv subdir and update parameter values
	for k := range fl {

		srcId, dstId, err := fromWorksetTextToDb(dbConn, modelDef, langDef, fl[k], runIdMap)
		if err != nil {
			return nil, err
		}
		if srcId > 0 {
			setIdMap[srcId] = dstId // update workset id with actual id value from database
		}
	}

	return setIdMap, nil
}

// fromWorksetTextToDb read workset metadata from json file,
// read all parameters from csv files, convert it to db cells and insert into database
// update set id's and base run id's with actual id in destination database
// it return source workset id (set id from metadata json file) and destination set id
func fromWorksetTextToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangList, metaPath string, runIdMap map[int]int) (int, int, error) {

	// get workset metadata
	var wm db.WorksetMeta
	isExist, err := helper.FromJsonFile(metaPath, &wm)
	if err != nil {
		return 0, 0, err
	}
	if !isExist {
		return 0, 0, nil // no workset
	}

	// save model workset
	// update incoming set id with actual new set id created in database
	// update incoming base run id with actual run id in database
	srcId := wm.Set.SetId
	wm.Set.BaseRunId = runIdMap[wm.Set.BaseRunId] // update base run id

	err = db.UpdateWorkset(dbConn, modelDef, langDef, &wm)
	if err != nil {
		return 0, 0, err
	}
	dstId := wm.Set.SetId

	omppLog.Log("Workset from ", srcId, " to ", dstId)

	if len(wm.Param) <= 0 {
		return srcId, dstId, nil // workset is empty, no parameters
	}

	// check if workset subdir exist
	csvDir := filepath.Join(filepath.Dir(metaPath), "set."+strconv.Itoa(srcId)+"."+helper.ToAlphaNumeric(wm.Set.Name))

	if _, err := os.Stat(csvDir); err != nil {
		return 0, 0, errors.New("workset directory not found: " + strconv.Itoa(srcId) + " " + wm.Set.Name)
	}

	// read all workset parameters from csv files
	layout := db.WriteLayout{ToId: dstId}

	for j := range wm.Param {

		// read parameter values from csv file
		var cell db.Cell
		cLst, err := fromCsvFile(csvDir, modelDef, wm.Param[j].Name, &cell)
		if err != nil {
			return 0, 0, err
		}
		if cLst == nil || cLst.Len() <= 0 {
			return 0, 0, errors.New("workset: " + strconv.Itoa(srcId) + " " + wm.Set.Name + " parameter empty: " + wm.Param[j].Name)
		}

		// insert or update parameter values in workset
		layout.Name = wm.Param[j].Name

		err = db.WriteParameter(dbConn, modelDef, &layout, cLst, "")
		if err != nil {
			return 0, 0, err
		}
	}

	return srcId, dstId, nil
}

// fromTaskListJsonToDb reads modeling tasks and tasks run history from json file and insert it into database.
// it does update task id, set id's and run id's with actual id in destination database
func fromTaskListJsonToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangList, inpDir string, runIdMap map[int]int, setIdMap map[int]int) error {

	// get list of task json files
	fl, err := filepath.Glob(inpDir + "/" + modelDef.Model.Name + ".task.[0-9]*.*.json")
	if err != nil {
		return err
	}
	if len(fl) <= 0 {
		return nil // no modeling tasks
	}

	// for each file: read task metadata, update task in target database
	for k := range fl {

		err := fromTaskJsonToDb(dbConn, modelDef, langDef, fl[k], runIdMap, setIdMap)
		if err != nil {
			return err
		}
	}

	return nil
}

// fromWorksetTextToDb reads modeling task and task run history from json file and insert it into database.
// it does update task id, set id's and run id's with actual id in destination database
func fromTaskJsonToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangList, metaPath string, runIdMap map[int]int, setIdMap map[int]int) error {

	// get task metadata
	var tm db.TaskMeta
	isExist, err := helper.FromJsonFile(metaPath, &tm)
	if err != nil {
		return err
	}
	if !isExist {
		return nil // no task
	}
	omppLog.Log("Modeling task ", tm.Task.TaskId)

	// save modeling task
	// update task id, set id's and run id's with actual id in destination database

	err = db.UpdateTask(dbConn, modelDef, langDef, &tm, runIdMap, setIdMap)
	return err
}

// fromCsvFile read parameter or output table csv file and convert it to list of db cells
func fromCsvFile(csvDir string, modelDef *db.ModelMeta, name string, cell db.CsvConverter) (*list.List, error) {

	// converter from csv row []string to db cell
	cvt, err := cell.CsvToCell(modelDef, name)
	if err != nil {
		return nil, err
	}

	// open csv file
	fn, err := cell.CsvFileName(modelDef, name)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(filepath.Join(csvDir, fn))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rd := csv.NewReader(f)
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
			return nil, err
		}

		// skip header line
		if isFirst {
			isFirst = false
			continue
		}

		// convert and append cell to cell list
		c, err := cvt(row)
		if err != nil {
			return nil, err
		}
		cLst.PushBack(c)
	}

	return cLst, nil
}
