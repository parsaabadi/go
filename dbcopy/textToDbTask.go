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

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// copy modeling task metadata and run history from json files into database
func textToDbTask(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// get modeling task name and id
	taskName := runOpts.String(taskNameArgKey)
	taskId := runOpts.Int(taskIdArgKey, 0)

	if taskId < 0 || taskId == 0 && taskName == "" {
		return errors.New("dbcopy invalid argument(s) for modeling task id: " + runOpts.String(taskIdArgKey) + " and/or name: " + runOpts.String(taskNameArgKey))
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

	if runOpts.IsExist(taskNameArgKey) && runOpts.IsExist(taskIdArgKey) { // both: task id and name

		metaPath = filepath.Join(inpDir,
			modelName+".task."+strconv.Itoa(taskId)+"."+helper.ToAlphaNumeric(taskName)+".json")

	} else { // task id or task name only

		// make path search patterns for metadata json file
		var mp string
		if runOpts.IsExist(taskNameArgKey) && !runOpts.IsExist(taskIdArgKey) { // task name only
			mp = modelName + ".task.*" + helper.ToAlphaNumeric(taskName) + ".json"
		}
		if !runOpts.IsExist(taskNameArgKey) && runOpts.IsExist(taskIdArgKey) { // task id only
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
	cs := runOpts.String(toDbConnStrArgKey)

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

	// restore model runs from json and/or csv files and insert it into database
	var runLst []string
	var isRunNotFound, isRunNotCompleted bool
	dblFmt := runOpts.String(doubleFormatArgKey)
	encName := runOpts.String(encodingArgKey)

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
			//cp := "run.*" + helper.ToAlphaNumeric(runName)
			mp := modelName + ".run.*" + helper.ToAlphaNumeric(runName) + ".json"
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
			d, f := filepath.Split(jsonPath)
			c := strings.TrimSuffix(strings.TrimPrefix(f, modelName+"."), ".json")

			if len(c) <= 4 { // expected csv directory: run.4.r or run.r
				csvDir = ""
			} else {
				csvDir = filepath.Join(d, c)
				if _, err := os.Stat(csvDir); err != nil {
					csvDir = ""
				}
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
			dstId, err := fromRunTextToDb(dstDb, modelDef, langDef, runName, jsonPath, dblFmt, encName)
			if err != nil {
				return err
			}
			if dstId <= 0 {
				isRunNotFound = true // run json file empty
			}
		}
	}

	// restore workset by set name from json and/or csv files and insert it into database
	var wsLst []string
	isSetNotFound := false

	var fws = func(dbConn *sql.DB, setName string) error {

		// check is workset already processed
		for i := range wsLst {
			if setName == wsLst[i] {
				return nil
			}
		}
		wsLst = append(wsLst, setName)

		// make path search patterns for metadata json and csv directory
		cp := "set.*" + helper.ToAlphaNumeric(setName)
		mp := modelName + "." + cp + ".json"
		var jsonPath, csvDir string

		// find path to metadata json by pattern
		fl, err := filepath.Glob(inpDir + "/" + mp)
		if err != nil {
			return err
		}
		if len(fl) >= 1 { // set name is unique per model, it is expected to be only one file
			jsonPath = fl[0]
			if len(fl) > 1 {
				omppLog.Log("found multiple workset metadata json files, using: " + filepath.Base(jsonPath))
			}
		}

		// csv directory:
		// if metadata json file exist then check if csv directory for that json file
		if jsonPath != "" {

			d, f := filepath.Split(jsonPath)
			c := strings.TrimSuffix(strings.TrimPrefix(f, modelName+"."), ".json")

			if len(c) <= 4 { // expected csv directory: set.4.w or set.w
				csvDir = ""
			} else {
				csvDir = filepath.Join(d, c)
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
		dstId, err := fromWorksetTextToDb(dbConn, modelDef, langDef, setName, jsonPath, csvDir, encName)
		if err != nil {
			return err
		}
		if dstId <= 0 {
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
	omppLog.Log("Modeling task from ", pub.Name, " into id: ", dstId)

	return nil
}

// fromTaskListJsonToDb reads modeling tasks and tasks run history from json file and insert it into database.
// it does update task id, set id's and run id's with actual id in destination database
func fromTaskListJsonToDb(dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, inpDir string) error {

	// get list of task json files
	fl, err := filepath.Glob(inpDir + "/" + modelDef.Model.Name + ".task.*.json")
	if err != nil {
		return err
	}
	if len(fl) <= 0 {
		return nil // no modeling tasks
	}

	// for each file: read task metadata, update task in target database
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

		// insert or update modeling task and task run history into database
		dstId, err := fromTaskJsonToDb(dbConn, modelDef, langDef, &pub)
		if err != nil {
			return err
		}
		omppLog.Log("Modeling task from ", pub.Name, " into id: ", dstId)
	}

	return nil
}

// fromTaskTextToDb insert or update modeling task and task run history into database.
// it does update task id with actual id in destination database and return it
func fromTaskJsonToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, pubMeta *db.TaskPub) (int, error) {

	// convert from "public" format into destination db rows
	meta, isSetNotFound, isTaskRunNotFound, err := pubMeta.FromPublic(dbConn, modelDef)
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
	err = meta.UpdateTask(dbConn, modelDef, langDef)
	if err != nil {
		return 0, err
	}
	return meta.Task.TaskId, nil
}
