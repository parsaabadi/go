// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// copy modeling task metadata and run history from database into text json and csv files
func dbToTextTask(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// get task name and id
	taskName := runOpts.String(taskNameArgKey)
	taskId := runOpts.Int(taskIdArgKey, 0)

	// conflicting options: use task id if positive else use task name
	if runOpts.IsExist(taskNameArgKey) && runOpts.IsExist(taskIdArgKey) {
		if taskId > 0 {
			omppLog.Log("dbcopy options conflict. Using task id: ", taskId, " ignore task name: ", taskName)
			taskName = ""
		} else {
			omppLog.Log("dbcopy options conflict. Using task name: ", taskName, " ignore task id: ", taskId)
			taskId = 0
		}
	}

	if taskId < 0 || taskId == 0 && taskName == "" {
		return errors.New("dbcopy invalid argument(s) for task id: " + runOpts.String(taskIdArgKey) + " and/or task name: " + runOpts.String(taskNameArgKey))
	}

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

	// get task metadata by id or name
	var taskRow *db.TaskRow
	var outDir string
	if taskId > 0 {
		if taskRow, err = db.GetTask(srcDb, taskId); err != nil {
			return err
		}
		if taskRow == nil {
			return errors.New("modeling task not found, task id: " + strconv.Itoa(taskId))
		}
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".task."+strconv.Itoa(taskId))
	} else {
		if taskRow, err = db.GetTaskByName(srcDb, modelDef.Model.ModelId, taskName); err != nil {
			return err
		}
		if taskRow == nil {
			return errors.New("modeling task not found: " + taskName)
		}
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".task."+taskName)
	}

	meta, err := db.GetTaskFull(srcDb, taskRow, "") // get task full metadata, including task run history
	if err != nil {
		return err
	}

	// create new output directory for task metadata
	err = os.MkdirAll(outDir, 0750)
	if err != nil {
		return err
	}

	// write task metadata into json file
	if err = toTaskJson(srcDb, modelDef, meta, outDir); err != nil {
		return err
	}

	// save runs from model run history
	var runIdLst []int
	var isRunNotFound, isRunNotCompleted bool
	dblFmt := runOpts.String(doubleFormatArgKey)
	isIdCsv := runOpts.Bool(useIdCsvArgKey)

	for j := range meta.TaskRun {
	nextRun:
		for k := range meta.TaskRun[j].TaskRunSet {

			// check is this run id already processed
			runId := meta.TaskRun[j].TaskRunSet[k].RunId
			for i := range runIdLst {
				if runId == runIdLst[i] {
					continue nextRun
				}
			}
			runIdLst = append(runIdLst, runId)

			// find model run metadata by id
			runRow, err := db.GetRun(srcDb, runId)
			if err != nil {
				return err
			}
			if runRow == nil {
				isRunNotFound = true
				continue // skip: run not found
			}

			// run must be completed: status success, error or exit
			if runRow.Status != db.DoneRunStatus && runRow.Status != db.ExitRunStatus && runRow.Status != db.ErrorRunStatus {
				isRunNotCompleted = true
				continue // skip: run not completed
			}

			rm, err := db.GetRunFull(srcDb, runRow, "") // get full model run metadata
			if err != nil {
				return err
			}

			// write model run metadata into json, parameters and output result values into csv files
			if err = toRunText(srcDb, modelDef, rm, outDir, dblFmt, isIdCsv); err != nil {
				return err
			}
		}
	}

	// find workset by set id and save it's metadata to json and workset parameters to csv
	var wsIdLst []int
	var isSetNotFound, isSetNotReadOnly bool

	var fws = func(dbConn *sql.DB, setId int) error {

		// check is workset already processed
		for i := range wsIdLst {
			if setId == wsIdLst[i] {
				return nil
			}
		}
		wsIdLst = append(wsIdLst, setId)

		// get workset by id
		wsRow, err := db.GetWorkset(dbConn, setId)
		if err != nil {
			return err
		}
		if wsRow == nil { // exit: workset not found
			isSetNotFound = true
			return nil
		}
		if !wsRow.IsReadonly { // exit: workset not readonly
			isSetNotReadOnly = true
			return nil
		}

		wm, err := db.GetWorksetFull(dbConn, wsRow, "") // get full workset metadata
		if err != nil {
			return err
		}

		// write workset metadata into json and parameter values into csv files
		if err = toWorksetText(dbConn, modelDef, wm, outDir, dblFmt, isIdCsv); err != nil {
			return err
		}
		return nil
	}

	// save task body worksets
	for k := range meta.Set {
		if err = fws(srcDb, meta.Set[k]); err != nil {
			return err
		}
	}

	// save worksets from model run history
	for j := range meta.TaskRun {
		for k := range meta.TaskRun[j].TaskRunSet {
			if err = fws(srcDb, meta.TaskRun[j].TaskRunSet[k].SetId); err != nil {
				return err
			}
		}
	}

	// display warnings if any worksets not found or not readonly
	// display warnings if any model runs not exists or not completed
	if isSetNotFound {
		omppLog.Log("Warning: task ", meta.Task.Name, " workset(s) not found, copy of task incomplete")
	}
	if isSetNotReadOnly {
		omppLog.Log("Warning: task ", meta.Task.Name, " workset(s) not readonly, copy of task incomplete")
	}
	if isRunNotFound {
		omppLog.Log("Warning: task ", meta.Task.Name, " model run(s) not found, copy of task run history incomplete")
	}
	if isRunNotCompleted {
		omppLog.Log("Warning: task ", meta.Task.Name, " model run(s) not completed, copy of task run history incomplete")
	}

	// pack worksets metadata json and csv files into zip
	if runOpts.Bool(zipArgKey) {
		zipPath, err := helper.PackZip(outDir, "")
		if err != nil {
			return err
		}
		omppLog.Log("Packed ", zipPath)
	}
	return nil
}

// toTaskListJson convert all successfully completed tasks and tasks run history to json and write into json files
func toTaskListJson(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string) error {

	// get all modeling tasks and successfully completed tasks run history
	tl, err := db.GetTaskFullList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// read each task metadata and write into json files
	for k := range tl {
		if err := toTaskJson(dbConn, modelDef, &tl[k], outDir); err != nil {
			return err
		}
	}
	return nil
}

// toTaskJson convert modeling task and task run history to json and write into json file
func toTaskJson(dbConn *sql.DB, modelDef *db.ModelMeta, meta *db.TaskMeta, outDir string) error {

	// convert db rows into "public" format
	omppLog.Log("Modeling task ", meta.Task.TaskId, " ", meta.Task.Name)

	pub, err := meta.ToPublic(dbConn, modelDef)
	if err != nil {
		return err
	}

	// save modeling task metadata into json
	err = helper.ToJsonFile(filepath.Join(
		outDir,
		modelDef.Model.Name+".task."+strconv.Itoa(meta.Task.TaskId)+"."+helper.ToAlphaNumeric(meta.Task.Name)+".json"),
		pub)
	return err
}
