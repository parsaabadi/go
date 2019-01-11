// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// UpdateTaskDef replace or merge task definition: task text (description and notes) and task input worksets into database.
// It does replace or merge task_txt and task_set db rows.
// If task does not exist then new task created.
func (mc *ModelCatalog) UpdateTaskDef(isReplace bool, tpd *db.TaskDefPub) (bool, string, string, error) {

	// validate parameters
	if tpd == nil {
		omppLog.Log("Error: invalid (empty) modeling task definition")
		return false, "", "", errors.New("Error: invalid (empty) modeling task definition")
	}

	// if model digest-or-name or task name is empty then return empty results
	dn := tpd.ModelDigest
	if dn == "" {
		dn = tpd.ModelName
	}
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false, "", "", nil
	}

	tn := tpd.Name
	if tn == "" {
		omppLog.Log("Warning: invalid (empty) modeling task name")
		return false, "", "", nil
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, dn, tn, nil // return empty result: model not found or error
	}

	// lock catalog and update model run
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// convert run from "public" into db rows
	// all input worskset names must exist in workset_lst
	tm, isSetNotFound, _, err := (&db.TaskPub{TaskDefPub: *tpd}).FromPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at modeling task conversion: ", dn, ": ", tn, ": ", err.Error())
		return false, dn, tn, err
	}
	if isSetNotFound {
		omppLog.Log("Error at modeling task conversion, invalid input set name(s): ", dn, ": ", tn, ": ", err.Error())
		return false, dn, tn, err
	}

	// replace or merge task text and task input worksets into database task_lst, task_txt, task_set tables
	if isReplace {
		err = tm.ReplaceTaskDef(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, mc.modelLst[idx].langMeta)
	} else {
		err = tm.MergeTaskDef(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta, mc.modelLst[idx].langMeta)
	}
	if err != nil {
		omppLog.Log("Error at update modeling task: ", dn, ": ", tn, ": ", err.Error())
		return false, dn, tn, err
	}

	return true, dn, tn, nil
}

// DeleteTask do delete modeling task, task run history from database.
// Task run history deleted only from task_run_lst and task_run_set tables,
// it does not delete model runs or any model input sets (worksets).
// If multiple models with same name exist then result is undefined.
// If task does not exists in database then it is empty operation.
// If modeling task is running during delete then result is undefined and model may fail with database error.
func (mc *ModelCatalog) DeleteTask(dn, tn string) (bool, error) {

	// if model digest-or-name or task name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false, nil
	}
	if tn == "" {
		omppLog.Log("Warning: invalid (empty) task name")
		return false, nil
	}

	// if model metadata not loaded then read it from database
	idx, ok := mc.loadModelMeta(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false, nil // return empty result: model not found or error
	}

	// lock catalog and delete task
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// find task in database
	t, err := db.GetTaskByName(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, tn)
	if err != nil {
		omppLog.Log("Error at get modeling task: ", dn, ": ", tn, ": ", err.Error())
		return false, err
	}
	if t == nil {
		return false, nil // return OK: task not found
	}

	// delete task from database
	err = db.DeleteTask(mc.modelLst[idx].dbConn, t.TaskId)
	if err != nil {
		omppLog.Log("Error at delete task: ", dn, ": ", tn, ": ", err.Error())
		return false, err
	}

	return true, nil
}
