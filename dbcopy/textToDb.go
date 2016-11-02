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
	"regexp"
	"strconv"

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
	cs := runOpts.String(toDbConnectionStr)
	// use OpenM options if DBCopy ouput database not defined
	//	if cs == "" && runOpts.IsExist(config.DbConnectionStr) {
	//		cs = runOpts.String(config.DbConnectionStr)
	//	}

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
	encName := runOpts.String(encodingArgKey)

	if err = fromRunTextListToDb(dstDb, modelDef, langDef, inpDir, runOpts.String(config.DoubleFormat), encName); err != nil {
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

// copy model run from text json and csv files into database
func textToDbRun(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// get model run name and id
	runName := runOpts.String(config.RunName)
	runId := runOpts.Int(config.RunId, 0)

	if runId < 0 || runId == 0 && runName == "" {
		return errors.New("dbcopy invalid argument(s) for model run id: " + runOpts.String(config.RunId) + " and/or name: " + runOpts.String(config.RunName))
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

	if runOpts.IsExist(config.RunName) && runOpts.IsExist(config.RunId) { // both: run id and name

		metaPath = filepath.Join(inpDir,
			modelName+".run."+strconv.Itoa(runId)+"."+helper.ToAlphaNumeric(runName)+".json")
		csvDir = filepath.Join(inpDir,
			"run."+strconv.Itoa(runId)+"."+helper.ToAlphaNumeric(runName))

	} else { // run id or run name only

		// make path search patterns for metadata json and csv directory
		var cp string
		if runOpts.IsExist(config.RunName) && !runOpts.IsExist(config.RunId) { // run name only
			cp = "run.[0-9]*." + helper.ToAlphaNumeric(runName)
		}
		if !runOpts.IsExist(config.RunName) && runOpts.IsExist(config.RunId) { // run id only
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
	cs := runOpts.String(toDbConnectionStr)
	// use OpenM options if DBCopy ouput database not defined
	//	if cs == "" && runOpts.IsExist(config.DbConnectionStr) {
	//		cs = runOpts.String(config.DbConnectionStr)
	//	}

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
	// it is parameter directory (if specified) or input directory/modelName.set.id
	// for csv files this "root" combined with subdirectory: root/set.id.setName
	inpDir := ""
	if runOpts.IsExist(config.ParamDir) {
		inpDir = filepath.Clean(runOpts.String(config.ParamDir))
	} else {
		if setId > 0 {
			inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".set."+strconv.Itoa(setId))
		} else {
			inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".set."+setName)
		}
	}

	// unzip if required and use unzipped directory as "root" input diretory
	if runOpts.Bool(zipArgKey) {
		base := filepath.Base(inpDir)
		omppLog.Log("Unpack ", base, ".zip")

		outDir := runOpts.String(outputDirArgKey)
		if outDir == "" {
			outDir = filepath.Dir(inpDir)
		}
		if err := helper.UnpackZip(inpDir+".zip", outDir); err != nil {
			return err
		}
		inpDir = filepath.Join(outDir, base)
	}

	// get workset metadata json path and csv directory by set id or set name or both
	var metaPath string
	var csvDir string

	if runOpts.IsExist(config.SetName) && runOpts.IsExist(config.SetId) { // both: set id and name

		metaPath = filepath.Join(inpDir,
			modelName+".set."+strconv.Itoa(setId)+"."+helper.ToAlphaNumeric(setName)+".json")

		if _, err := os.Stat(metaPath); err != nil { // clear path to indicate metadata json file exist
			metaPath = ""
		}

		csvDir = filepath.Join(inpDir,
			"set."+strconv.Itoa(setId)+"."+helper.ToAlphaNumeric(setName))

		if _, err := os.Stat(csvDir); err != nil { // clear path to indicate csv directory exist
			csvDir = ""
		}

	} else { // set id or set name only

		// make path search patterns for metadata json and csv directory
		var cp string
		if runOpts.IsExist(config.SetName) && !runOpts.IsExist(config.SetId) { // set name only
			cp = "set.[0-9]*." + helper.ToAlphaNumeric(setName)
		}
		if !runOpts.IsExist(config.SetName) && runOpts.IsExist(config.SetId) { // set id only
			cp = "set." + strconv.Itoa(setId) + ".*"
		}
		mp := modelName + "." + cp + ".json"

		// find path to metadata json by pattern
		fl, err := filepath.Glob(inpDir + "/" + mp)
		if err != nil {
			return err
		}
		if len(fl) >= 1 {
			metaPath = fl[0]
			if len(fl) > 1 {
				omppLog.Log("found multiple workset metadata json files, using: " + filepath.Base(metaPath))
			}
		}

		// csv directory:
		// if metadata json file exist then check if csv directory for that json file
		if metaPath != "" {

			re := regexp.MustCompile("\\.set\\.([0-9]+)\\.((_|[0-9A-Za-z])+)\\.json")
			s := re.FindString(filepath.Base(metaPath))

			if len(s) > 6 { // expected match string: .set.4.q.json, csv directory: set.4.q

				csvDir = filepath.Join(inpDir, s[1:len(s)-5])

				if _, err := os.Stat(csvDir); err != nil {
					csvDir = ""
				}
			}

		} else { // metadata json file not exist: search for csv directory by pattern

			fl, err := filepath.Glob(inpDir + "/" + cp)
			if err != nil {
				return err
			}
			if len(fl) >= 1 {
				csvDir = fl[0]
				if len(fl) > 1 {
					omppLog.Log("found multiple workset csv directories, using: " + filepath.Base(csvDir))
				}
			}
		}
	}

	// check results: metadata json file or csv directory must exist
	if metaPath == "" && csvDir == "" {
		return errors.New("no workset metadata json file and no csv directory, workset: " + strconv.Itoa(setId) + " " + setName)
	}

	// get connection string and driver name
	cs := runOpts.String(toDbConnectionStr)
	// use OpenM options if DBCopy ouput database not defined
	//	if cs == "" && runOpts.IsExist(config.DbConnectionStr) {
	//		cs = runOpts.String(config.DbConnectionStr)
	//	}

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

	// read from metadata json and csv files and update target database
	encName := runOpts.String(encodingArgKey)

	srcId, _, err := fromWorksetTextToDb(dstDb, modelDef, langDef, setName, setId, metaPath, csvDir, encName)
	if err != nil {
		return err
	}
	if srcId <= 0 && csvDir == "" {
		return errors.New("workset not found or empty: " + strconv.Itoa(setId) + " " + setName)
	}

	return nil
}

// copy modeling task metadata and run history from json files into database
func textToDbTask(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// get modeling task name and id
	taskName := runOpts.String(config.TaskName)
	taskId := runOpts.Int(config.TaskId, 0)

	if taskId < 0 || taskId == 0 && taskName == "" {
		return errors.New("dbcopy invalid argument(s) for modeling task id: " + runOpts.String(config.TaskId) + " and/or name: " + runOpts.String(config.TaskName))
	}

	// deirectory for task metadata: it is input directory/modelName
	inpDir := ""
	if taskId > 0 {
		inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".task."+strconv.Itoa(taskId))
	} else {
		inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".task."+taskName)
	}

	// get model task metadata json path by task id or task name or both
	var metaPath string

	if runOpts.IsExist(config.TaskName) && runOpts.IsExist(config.TaskId) { // both: task id and name

		metaPath = filepath.Join(inpDir,
			modelName+".task."+strconv.Itoa(taskId)+"."+helper.ToAlphaNumeric(taskName)+".json")

	} else { // task id or task name only

		// make path search patterns for metadata json file
		var mp string
		if runOpts.IsExist(config.TaskName) && !runOpts.IsExist(config.TaskId) { // task name only
			mp = modelName + ".task.[0-9]*." + helper.ToAlphaNumeric(taskName) + ".json"
		}
		if !runOpts.IsExist(config.TaskName) && runOpts.IsExist(config.TaskId) { // task id only
			mp = modelName + ".task." + strconv.Itoa(taskId) + ".*.json"
		}

		// find path to metadata json by pattern
		fl, err := filepath.Glob(inpDir + "/" + mp)
		if err != nil {
			return err
		}
		if len(fl) <= 0 {
			return errors.New("no metadata json file found for modeling task: " + strconv.Itoa(taskId) + " " + taskName)
		}
		if len(fl) > 1 {
			omppLog.Log("found multiple modeling task metadata json files, using: " + filepath.Base(metaPath))
		}
		metaPath = fl[0]
	}

	// check results: metadata json file must exist
	if metaPath == "" {
		return errors.New("no metadata json file found for modeling task: " + strconv.Itoa(taskId) + " " + taskName)
	}
	if _, err := os.Stat(metaPath); err != nil {
		return errors.New("no metadata json file found for modeling task: " + strconv.Itoa(taskId) + " " + taskName)
	}

	// get connection string and driver name
	cs := runOpts.String(toDbConnectionStr)
	// use OpenM options if DBCopy ouput database not defined
	//	if cs == "" && runOpts.IsExist(config.DbConnectionStr) {
	//		cs = runOpts.String(config.DbConnectionStr)
	//	}

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

	// read task metadata from json
	var pub db.TaskPub
	isExist, err := helper.FromJsonFile(metaPath, &pub)
	if err != nil {
		return err
	}
	if !isExist {
		return errors.New("modeling task not found or empty: " + strconv.Itoa(taskId) + " " + taskName)
	}

	// task name: use task name from json metadata, if empty
	if pub.Name != "" && taskName != pub.Name {
		taskName = pub.Name
	}

	// task id: parse json file name to get source task id
	if taskId <= 0 {
		re := regexp.MustCompile("\\.task\\.([0-9]+)\\.")
		s2 := re.FindStringSubmatch(filepath.Base(metaPath))
		if len(s2) >= 2 {
			taskId, _ = strconv.Atoi(s2[1]) // if any error source task id remain default zero
		}
	}

	// restore model runs from json and/or csv files and insert it into database
	var runLst []string
	var isRunNotFound, isRunNotCompleted bool
	encName := runOpts.String(encodingArgKey)
	runRe := regexp.MustCompile("\\.run\\.([0-9]+)\\.((_|[0-9A-Za-z])+)\\.json")

	for j := range pub.TaskRun {
	nextRun:
		for k := range pub.TaskRun[j].TaskRunSet {

			// check is this run id already processed
			runDigest := pub.TaskRun[j].TaskRunSet[k].Run.Digest
			for i := range runLst {
				if runDigest == runLst[i] {
					continue nextRun
				}
			}
			runLst = append(runLst, runDigest)

			// run name must not be empty in order to find run json metadata and csv files
			runName := pub.TaskRun[j].TaskRunSet[k].Run.Name
			if runName == "" {
				isRunNotFound = true // skip: run name empty
				continue
			}

			// run must be completed: status success, error or exit
			if pub.TaskRun[j].TaskRunSet[k].Run.Status != db.DoneRunStatus &&
				pub.TaskRun[j].TaskRunSet[k].Run.Status != db.ExitRunStatus &&
				pub.TaskRun[j].TaskRunSet[k].Run.Status != db.ErrorRunStatus {
				isRunNotCompleted = true
				continue // skip: run not completed
			}

			// make path search patterns for metadata json and csv directory
			cp := "run.[0-9]*." + helper.ToAlphaNumeric(runName)
			mp := modelName + "." + cp + ".json"
			var jsonPath, csvDir string

			// find path to metadata json by pattern
			fl, err := filepath.Glob(inpDir + "/" + mp)
			if err != nil {
				return err
			}
			if len(fl) <= 0 {
				isRunNotFound = true // skip: no run metadata
				continue
			}
			jsonPath = fl[0]
			if len(fl) > 1 {
				omppLog.Log("found multiple model run metadata json files, using: " + filepath.Base(jsonPath))
			}

			// csv directory: check if csv directory exist for that json file
			s := runRe.FindString(filepath.Base(jsonPath))

			if len(s) > 6 { // expected match string: .run.4.q.json, csv directory: run.4.q
				csvDir = filepath.Join(inpDir, s[1:len(s)-5])
			}

			// check results: metadata json file or csv directory must exist
			if jsonPath == "" || csvDir == "" {
				isRunNotFound = true // skip: no run metadata json file or csv directory
				continue
			}
			if _, err := os.Stat(jsonPath); err != nil {
				isRunNotFound = true // skip: no run metadata json file
				continue
			}
			if _, err := os.Stat(csvDir); err != nil {
				isRunNotFound = true // skip: no run csv directory
				continue
			}

			// read from metadata json and csv files and update target database
			srcId, _, err := fromRunTextToDb(dstDb, modelDef, langDef, runName, 0, jsonPath, csvDir, encName)
			if err != nil {
				return err
			}
			if srcId <= 0 {
				isRunNotFound = true // run json file empty
			}
		}
	}

	// restore workset by set name from json and/or csv files and insert it into database
	var wsLst []string
	isSetNotFound := false
	setRe := regexp.MustCompile("\\.set\\.([0-9]+)\\.((_|[0-9A-Za-z])+)\\.json")

	var fws = func(dbConn *sql.DB, setName string) error {

		// check is workset already processed
		for i := range wsLst {
			if setName == wsLst[i] {
				return nil
			}
		}
		wsLst = append(wsLst, setName)

		// make path search patterns for metadata json and csv directory
		cp := "set.[0-9]*." + helper.ToAlphaNumeric(setName)
		mp := modelName + "." + cp + ".json"
		var jsonPath, csvDir string

		// find path to metadata json by pattern
		fl, err := filepath.Glob(inpDir + "/" + mp)
		if err != nil {
			return err
		}
		if len(fl) >= 1 {
			jsonPath = fl[0]
			if len(fl) > 1 {
				omppLog.Log("found multiple workset metadata json files, using: " + filepath.Base(jsonPath))
			}
		}

		// csv directory:
		// if metadata json file exist then check if csv directory for that json file
		if jsonPath != "" {

			s := setRe.FindString(filepath.Base(jsonPath))

			if len(s) > 6 { // expected match string: .set.4.q.json, csv directory: set.4.q

				csvDir = filepath.Join(inpDir, s[1:len(s)-5])

				if _, err := os.Stat(csvDir); err != nil {
					csvDir = ""
				}
			}

		} else { // metadata json file not exist: search for csv directory by pattern

			fl, err := filepath.Glob(inpDir + "/" + cp)
			if err != nil {
				return err
			}
			if len(fl) >= 1 {
				csvDir = fl[0]
				if len(fl) > 1 {
					omppLog.Log("found multiple workset csv directories, using: " + filepath.Base(csvDir))
				}
			}
		}

		// check results: metadata json file or csv directory must exist
		if jsonPath == "" && csvDir == "" {
			isSetNotFound = true // exit: no workset json and no csv directory exists
			return nil
		}

		// write workset metadata into json and parameter values into csv files
		srcId, _, err := fromWorksetTextToDb(dbConn, modelDef, langDef, setName, 0, jsonPath, csvDir, encName)
		if err != nil {
			return err
		}
		if srcId <= 0 && csvDir == "" {
			isSetNotFound = true // workset empty: json empty and csv directory empty
		}
		return nil
	}

	// restore task body worksets
	for k := range pub.Set {
		if err = fws(dstDb, pub.Set[k]); err != nil {
			return err
		}
	}

	// restore worksets from model run history
	for j := range pub.TaskRun {
		for k := range pub.TaskRun[j].TaskRunSet {
			if err = fws(dstDb, pub.TaskRun[j].TaskRunSet[k].SetName); err != nil {
				return err
			}
		}
	}

	// display warnings if any workset not found (files and csv directories not found)
	// display warnings if any model runs not found or not completed
	if isSetNotFound {
		omppLog.Log("Warning: task ", pub.Name, " workset(s) not found, copy of task incomplete")
	}
	if isRunNotFound {
		omppLog.Log("Warning: task ", pub.Name, " model run(s) not found, copy of task run history incomplete")
	}
	if isRunNotCompleted {
		omppLog.Log("Warning: task ", pub.Name, " model run(s) not completed, copy of task run history incomplete")
	}

	// insert or update modeling task and task run history into database
	dstId, err := fromTaskJsonToDb(dstDb, modelDef, langDef, &pub)
	if err != nil {
		return err
	}
	omppLog.Log("Modeling task from ", taskId, " ", pub.Name, " to ", dstId)

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
	meta, err := pub.FromPublic(dbConn, modelDef, langDef)
	if err != nil {
		return 0, 0, err
	}

	// save model run
	isExist, err = meta.UpdateRun(dbConn, modelDef)
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

// fromWorksetTextListToDb read all worksets parameters from csv and json files,
// convert it to db cells and insert into database
// update set id's and base run id's with actual id in database
func fromWorksetTextListToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, inpDir string, encodingName string) error {

	// get list of workset json files
	fl, err := filepath.Glob(inpDir + "/" + modelDef.Model.Name + ".set.[0-9]*.*.json")
	if err != nil {
		return err
	}
	if len(fl) <= 0 {
		return nil // no worksets
	}

	// for each file:
	// read workset metadata, update workset in target database
	// read csv files from workset csv subdir and update parameter values
	for k := range fl {

		// check if workset subdir exist
		re := regexp.MustCompile("\\.set\\.([0-9]+)\\.((_|[0-9A-Za-z])+)\\.json")
		s := re.FindString(filepath.Base(fl[k]))

		csvDir := ""
		if len(s) > 6 { // expected match string: .set.4.q.json, csv directory: set.4.q

			csvDir = filepath.Join(inpDir, s[1:len(s)-5])

			if _, err := os.Stat(csvDir); err != nil {
				csvDir = ""
			}
		}

		_, _, err := fromWorksetTextToDb(dbConn, modelDef, langDef, "", 0, fl[k], csvDir, encodingName)
		if err != nil {
			return err
		}
	}

	return nil
}

// fromWorksetTextToDb read workset metadata from json file,
// read all parameters from csv files, convert it to db cells and insert into database
// update set id's and base run id's with actual id in destination database
// it return source workset id (set id from metadata json file) and destination set id
func fromWorksetTextToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, srcName string, srcId int, metaPath string, csvDir string, encodingName string,
) (int, int, error) {

	// if no metadata file and no csv directory then exit: nothing to do
	if metaPath == "" && csvDir == "" {
		return 0, 0, nil // no workset
	}

	// get workset metadata:
	// model name and set name must be specified as parameter or inside of metadata json
	var pub db.WorksetPub

	if metaPath == "" && csvDir != "" { // no metadata json file, only csv directory
		pub.Name = srcName
		pub.ModelName = modelDef.Model.Name
	}

	if metaPath != "" { // read metadata json file

		isExist, err := helper.FromJsonFile(metaPath, &pub)
		if err != nil {
			return 0, 0, err
		}

		if !isExist { // metadata from json is empty

			if csvDir == "" { // if metadata json empty and no csv directory then exit: no data
				return 0, 0, nil
			}
			// metadata empty but there is csv directory: use expected model name and set name
			pub.Name = srcName
			pub.ModelName = modelDef.Model.Name

		} else { // metadata from json

			// set id: parse json file name to get source set id
			re := regexp.MustCompile("\\.set\\.([0-9]+)\\.")
			s2 := re.FindStringSubmatch(filepath.Base(metaPath))
			if len(s2) >= 2 {
				srcId, _ = strconv.Atoi(s2[1]) // if any error source set id remain default zero
			}
		}
	}
	if pub.Name == "" {
		return 0, 0, errors.New("workset name is empty and metadata json file not found or empty")
	}

	// if only csv directory specified: make list of parameters based on csv file names
	if metaPath == "" && csvDir != "" {

		fl, err := filepath.Glob(csvDir + "/*.csv")
		if err != nil {
			return 0, 0, err
		}
		pub.Param = make([]db.NameLangNote, len(fl))

		for j := range fl {
			fn := filepath.Base(fl[j])
			fn = fn[:len(fn)-4] // remove .csv extension
			pub.Param[j].Name = fn
		}
	}

	// destination: convert from "public" format into destination db rows
	// display warning if base run not found in destination database
	ws, err := pub.FromPublic(dbConn, modelDef, langDef)
	if err != nil {
		return 0, 0, err
	}
	if ws.Set.BaseRunId <= 0 && pub.BaseRunDigest != "" {
		omppLog.Log("Warning: workset ", ws.Set.Name, ", base run not found by digest ", pub.BaseRunDigest)
	}

	// save workset metadata as "read-write" and after importing all parameters set it as "readonly"
	isReadonly := pub.IsReadonly
	ws.Set.IsReadonly = false

	err = ws.UpdateWorkset(dbConn, modelDef)
	if err != nil {
		return 0, 0, err
	}
	dstId := ws.Set.SetId // actual set id from destination database

	// read all workset parameters and copy into destination database
	omppLog.Log("Workset ", ws.Set.Name, " from id ", srcId, " to ", dstId)

	// read all workset parameters from csv files
	layout := db.WriteLayout{ToId: dstId}

	for j := range pub.Param {

		// read parameter values from csv file
		var cell db.Cell
		cLst, err := fromCsvFile(csvDir, modelDef, pub.Param[j].Name, &cell, encodingName)
		if err != nil {
			return 0, 0, err
		}
		if cLst == nil || cLst.Len() <= 0 {
			return 0, 0, errors.New("workset: " + strconv.Itoa(srcId) + " " + ws.Set.Name + " parameter empty: " + pub.Param[j].Name)
		}

		// insert or update parameter values in workset
		layout.Name = pub.Param[j].Name

		err = db.WriteParameter(dbConn, modelDef, &layout, cLst, "")
		if err != nil {
			return 0, 0, err
		}
	}

	// update workset readonly status with actual value
	err = db.UpdateWorksetReadonly(dbConn, dstId, isReadonly)
	if err != nil {
		return 0, 0, err
	}

	return srcId, dstId, nil
}

// fromTaskListJsonToDb reads modeling tasks and tasks run history from json file and insert it into database.
// it does update task id, set id's and run id's with actual id in destination database
func fromTaskListJsonToDb(dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, inpDir string) error {

	// get list of task json files
	fl, err := filepath.Glob(inpDir + "/" + modelDef.Model.Name + ".task.[0-9]*.*.json")
	if err != nil {
		return err
	}
	if len(fl) <= 0 {
		return nil // no modeling tasks
	}

	// for each file: read task metadata, update task in target database
	re := regexp.MustCompile("\\.task\\.([0-9]+)\\.")

	for k := range fl {

		// read task metadata from json
		var pub db.TaskPub
		isExist, err := helper.FromJsonFile(fl[k], &pub)
		if err != nil {
			return err
		}
		if !isExist {
			continue // skip: no modeling task, file not exist or empty
		}

		// task id: parse json file name to get source task id
		// model name and task name must be specified as parameter or inside of metadata json
		s2 := re.FindStringSubmatch(filepath.Base(fl[k]))
		srcId := 0
		if len(s2) >= 2 {
			srcId, _ = strconv.Atoi(s2[1]) // if any error source task id remain zero
		}

		// insert or update modeling task and task run history into database
		dstId, err := fromTaskJsonToDb(dbConn, modelDef, langDef, &pub)
		if err != nil {
			return err
		}
		omppLog.Log("Modeling task from ", srcId, " ", pub.Name, " to ", dstId)
	}

	return nil
}

// fromTaskTextToDb insert or update modeling task and task run history into database.
// it does update task id with actual id in destination database and return it
func fromTaskJsonToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, pubMeta *db.TaskPub) (int, error) {

	// convert from "public" format into destination db rows
	meta, isSetNotFound, isTaskRunNotFound, err := pubMeta.FromPublic(dbConn, modelDef, langDef)
	if err != nil {
		return 0, err
	}
	if isSetNotFound {
		omppLog.Log("Warning: task ", meta.Task.Name, " worksets not found, copy of task incomplete")
	}
	if isTaskRunNotFound {
		omppLog.Log("Warning: task ", meta.Task.Name, " worksets or model runs not found, copy of task run history incomplete")
	}

	// save modeling task metadata
	err = meta.UpdateTask(dbConn, modelDef)
	if err != nil {
		return 0, err
	}
	return meta.Task.TaskId, nil
}

// fromCsvFile read parameter or output table csv file and convert it to list of db cells
func fromCsvFile(
	csvDir string, modelDef *db.ModelMeta, name string, cell db.CsvConverter, encodingName string) (*list.List, error) {

	// converter from csv row []string to db cell
	cvt, err := cell.CsvToCell(modelDef, name)
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
