// Copyright (c) 2020 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"strconv"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// rename model run
func dbRenameRun(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// new run name argument required and cannot be empty
	newRunName := runOpts.String(runNewNameArgKey)
	if newRunName == "" {
		return errors.New("dbcopy invalid (empty or missing) argument of: " + runNewNameArgKey)
	}

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(dbConnStrArgKey), runOpts.String(dbDriverArgKey))
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	if err := db.CheckOpenmppSchemaVersion(srcDb); err != nil {
		return err
	}

	// find the model by name and/or digest
	isFound, modelId, err := db.GetModelId(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New("model " + modelName + " " + modelDigest + " not found")
	}

	// find model run metadata by id, run digest or name
	runId, runDigest, runName, isFirst, isLast := runIdDigestNameFromOptions(runOpts)
	if runId < 0 || runId == 0 && runName == "" && runDigest == "" && !isFirst && !isLast {
		return errors.New("dbcopy invalid argument(s) run id: " + runOpts.String(runIdArgKey) + ", run name: " + runOpts.String(runNameArgKey) + ", run digest: " + runOpts.String(runDigestArgKey))
	}
	runRow, e := findModelRunByIdDigestName(srcDb, modelId, runId, runDigest, runName, isFirst, isLast)
	if e != nil {
		return e
	}
	if runRow == nil {
		return errors.New("model run not found: " + runOpts.String(runIdArgKey) + " " + runOpts.String(runNameArgKey) + " " + runOpts.String(runDigestArgKey))
	}

	// check is this run belong to the model
	if runRow.ModelId != modelId {
		return errors.New("model run " + strconv.Itoa(runRow.RunId) + " " + runRow.Name + " " + runRow.RunDigest + " does not belong to model " + modelName + " " + modelDigest)
	}

	// run must be completed: status success, error or exit
	if !db.IsRunCompleted(runRow.Status) {
		return errors.New("model run not completed: " + strconv.Itoa(runRow.RunId) + " " + runRow.Name + " " + runRow.RunDigest)
	}

	// rename model run
	omppLog.Log("Rename model run ", runRow.RunId, " ", runRow.Name, " into: ", newRunName)

	isFound, err = db.RenameRun(srcDb, runRow.RunId, newRunName)
	if err != nil {
		return errors.New("failed to rename model run " + strconv.Itoa(runRow.RunId) + " " + runRow.Name + ": " + err.Error())
	}
	if !isFound {
		return errors.New("model run not found: " + strconv.Itoa(runRow.RunId) + " " + runRow.Name)
	}
	return nil
}

// rename workset
func dbRenameWorkset(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// new workset name argument required and cannot be empty
	newSetName := runOpts.String(setNewNameArgKey)
	if newSetName == "" {
		return errors.New("dbcopy invalid (empty or missing) argument of: " + setNewNameArgKey)
	}

	// get workset name and id
	setName := runOpts.String(setNameArgKey)
	setId := runOpts.Int(setIdArgKey, 0)

	// conflicting options: use set id if positive else use set name
	if runOpts.IsExist(setNameArgKey) && runOpts.IsExist(setIdArgKey) {
		if setId > 0 {
			omppLog.Log("dbcopy options conflict. Using set id: ", setId, " ignore set name: ", setName)
			setName = ""
		} else {
			omppLog.Log("dbcopy options conflict. Using set name: ", setName, " ignore set id: ", setId)
			setId = 0
		}
	}

	if setId < 0 || setId == 0 && setName == "" {
		return errors.New("dbcopy invalid argument(s) for set id: " + runOpts.String(setIdArgKey) + " and/or set name: " + runOpts.String(setNameArgKey))
	}

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(dbConnStrArgKey), runOpts.String(dbDriverArgKey))
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	if err := db.CheckOpenmppSchemaVersion(srcDb); err != nil {
		return err
	}

	// find the model
	isFound, modelId, err := db.GetModelId(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New("model " + modelName + " " + modelDigest + " not found")
	}

	// get workset metadata by id or name
	var wsRow *db.WorksetRow
	if setId > 0 {
		if wsRow, err = db.GetWorkset(srcDb, setId); err != nil {
			return err
		}
		if wsRow == nil {
			return errors.New("workset not found, set id: " + strconv.Itoa(setId))
		}
	} else {
		if wsRow, err = db.GetWorksetByName(srcDb, modelId, setName); err != nil {
			return err
		}
		if wsRow == nil {
			return errors.New("workset not found: " + setName)
		}
	}

	// check is this workset belong to the model
	if wsRow.ModelId != modelId {
		return errors.New("workset " + strconv.Itoa(wsRow.SetId) + " " + wsRow.Name + " does not belong to model " + modelName + " " + modelDigest)
	}

	// rename workset (even it is read-only)
	omppLog.Log("Rename workset ", wsRow.SetId, " ", wsRow.Name, " into: ", newSetName)

	isFound, err = db.RenameWorkset(srcDb, wsRow.SetId, newSetName, true)
	if err != nil {
		return errors.New("failed to rename workset " + strconv.Itoa(wsRow.SetId) + " " + wsRow.Name + " " + err.Error())
	}
	if !isFound {
		return errors.New("workset not found: " + strconv.Itoa(wsRow.SetId) + " " + wsRow.Name)
	}

	return nil
}

// rename modeling task
func dbRenameTask(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// new task name argument required and cannot be empty
	newTaskName := runOpts.String(taskNewNameArgKey)
	if newTaskName == "" {
		return errors.New("dbcopy invalid (empty or missing) argument of: " + taskNewNameArgKey)
	}

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

	if err := db.CheckOpenmppSchemaVersion(srcDb); err != nil {
		return err
	}

	// find the model
	isFound, modelId, err := db.GetModelId(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New("model " + modelName + " " + modelDigest + " not found")
	}

	// find modeling task by id or name
	var taskRow *db.TaskRow
	if taskId > 0 {
		if taskRow, err = db.GetTask(srcDb, taskId); err != nil {
			return err
		}
		if taskRow == nil {
			return errors.New("modeling task not found, task id: " + strconv.Itoa(taskId))
		}
	} else {
		if taskRow, err = db.GetTaskByName(srcDb, modelId, taskName); err != nil {
			return err
		}
		if taskRow == nil {
			return errors.New("modeling task not found: " + taskName)
		}
	}

	// check is this task belong to the model
	if taskRow.ModelId != modelId {
		return errors.New("modeling task " + strconv.Itoa(taskRow.TaskId) + " " + taskRow.Name + " does not belong to model " + modelName + " " + modelDigest)
	}

	// rename modeling task
	omppLog.Log("Rename task ", taskRow.TaskId, " ", taskRow.Name, " into: ", newTaskName)

	isFound, err = db.RenameTask(srcDb, taskRow.TaskId, newTaskName)
	if err != nil {
		return errors.New("failed to rename modeling task " + strconv.Itoa(taskRow.TaskId) + " " + taskRow.Name + " " + err.Error())
	}
	if !isFound {
		return errors.New("modeling task not found: " + strconv.Itoa(taskRow.TaskId) + " " + taskRow.Name)
	}
	return nil
}
