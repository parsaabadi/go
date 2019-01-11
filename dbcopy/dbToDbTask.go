// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"strconv"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// copy modeling task metadata and run history from source database to destination database
func dbToDbTask(modelName string, modelDigest string, runOpts *config.RunOptions) error {

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

	// validate source and destination
	inpConnStr := runOpts.String(dbConnStrArgKey)
	inpDriver := runOpts.String(dbDriverArgKey)
	outConnStr := runOpts.String(toDbConnStrArgKey)
	outDriver := runOpts.String(toDbDriverArgKey)

	if inpConnStr == outConnStr && inpDriver == outDriver {
		return errors.New("source same as destination: cannot overwrite model in database")
	}

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, inpConnStr, inpDriver)
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	nv, err := db.OpenmppSchemaVersion(srcDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid source database, likely not an openM++ database")
	}

	// open destination database and check is it valid
	cs, dn = db.IfEmptyMakeDefault(modelName, outConnStr, outDriver)
	dstDb, _, err := db.Open(cs, dn, true)
	if err != nil {
		return err
	}
	defer dstDb.Close()

	nv, err = db.OpenmppSchemaVersion(dstDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid destination database, likely not an openM++ database")
	}

	// source: get model metadata
	srcModel, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = srcModel.Model.Name // set model name: it can be empty and only model digest specified

	// get task metadata by id or name
	var taskRow *db.TaskRow
	if taskId > 0 {
		if taskRow, err = db.GetTask(srcDb, taskId); err != nil {
			return err
		}
		if taskRow == nil {
			return errors.New("modeling task not found, task id: " + strconv.Itoa(taskId))
		}
	} else {
		if taskRow, err = db.GetTaskByName(srcDb, srcModel.Model.ModelId, taskName); err != nil {
			return err
		}
		if taskRow == nil {
			return errors.New("modeling task not found: " + taskName)
		}
	}

	meta, err := db.GetTaskFull(srcDb, taskRow, "") // get task full metadata, including task run history
	if err != nil {
		return err
	}

	// destination: get model metadata
	dstModel, err := db.GetModel(dstDb, modelName, modelDigest)
	if err != nil {
		return err
	}

	// destination: get list of languages
	dstLang, err := db.GetLanguages(dstDb)
	if err != nil {
		return err
	}

	// copy to destiantion model runs from task run history
	var runIdLst []int
	var isRunNotFound, isRunNotCompleted bool
	dblFmt := runOpts.String(doubleFormatArgKey)

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
			if !db.IsRunCompleted(runRow.Status) {
				isRunNotCompleted = true
				continue // skip: run not completed
			}

			rm, err := db.GetRunFull(srcDb, runRow, "") // get full model run metadata
			if err != nil {
				return err
			}

			// convert model run db rows into "public" format
			// and copy source model run metadata, parameter values, output results into destination database
			runPub, err := rm.ToPublic(srcDb, srcModel)
			if err != nil {
				return err
			}
			_, err = copyRunDbToDb(srcDb, dstDb, srcModel, dstModel, rm.Run.RunId, runPub, dstLang, dblFmt)
			if err != nil {
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

		// convert workset db rows into "public" format
		// and copy source workset metadata and parameters into destination database
		setPub, err := wm.ToPublic(srcDb, srcModel)
		if err != nil {
			return err
		}
		_, err = copyWorksetDbToDb(srcDb, dstDb, srcModel, dstModel, wm.Set.SetId, setPub, dstLang)
		if err != nil {
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

	// convert task db rows into "public" format
	// and copy source task metadata into destination database
	pub, err := meta.ToPublic(srcDb, srcModel)
	if err != nil {
		return err
	}
	_, err = copyTaskDbToDb(srcDb, dstDb, srcModel, dstModel, meta.Task.TaskId, pub, dstLang)
	if err != nil {
		return err
	}
	return nil
}

// copyTaskListDbToDb do copy all modeling tasks from source to destination database
func copyTaskListDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, dstLang *db.LangMeta) error {

	// source: get all modeling tasks metadata in all languages
	srcTl, err := db.GetTaskFullList(srcDb, srcModel.Model.ModelId, true, "")
	if err != nil {
		return err
	}
	if len(srcTl) <= 0 {
		return nil
	}

	// copy task metadata from source to destination database by using "public" format
	for k := range srcTl {

		// convert task metadata db rows into "public"" format
		pub, err := srcTl[k].ToPublic(srcDb, srcModel)
		if err != nil {
			return err
		}

		// save into destination database
		_, err = copyTaskDbToDb(srcDb, dstDb, srcModel, dstModel, srcTl[k].Task.TaskId, pub, dstLang)
		if err != nil {
			return err
		}
	}
	return nil
}

// copyTaskDbToDb do copy modeling task metadata and task run history from source to destination database
// it return destination task id (task id in destination database)
func copyTaskDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, srcId int, pub *db.TaskPub, dstLang *db.LangMeta) (int, error) {

	// validate parameters
	if pub == nil {
		return 0, errors.New("invalid (empty) source modeling task metadata, source task not found or not exists")
	}

	// destination: convert from "public" format into destination db rows
	dstTask, isSetNotFound, isTaskRunNotFound, err := pub.FromPublic(dstDb, dstModel)
	if err != nil {
		return 0, err
	}
	if isSetNotFound {
		omppLog.Log("Warning: task ", dstTask.Task.Name, " worksets not found, copy of task incomplete")
	}
	if isTaskRunNotFound {
		omppLog.Log("Warning: task ", dstTask.Task.Name, " worksets or model runs not found, copy of task run history incomplete")
	}

	// destination: save modeling task metadata
	err = dstTask.UpdateTaskFull(dstDb, dstModel, dstLang)
	if err != nil {
		return 0, err
	}
	dstId := dstTask.Task.TaskId
	omppLog.Log("Modeling task from ", srcId, " ", pub.Name, " to ", dstId)

	return dstId, nil
}
