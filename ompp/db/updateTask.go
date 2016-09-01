// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
	"time"

	"go.openmpp.org/ompp/helper"
)

// UpdateTaskList insert new update existing modeling tasks and task run history in database.
// Model id, task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func UpdateTaskList(
	dbConn *sql.DB, modelDef *ModelMeta, langDef *LangList, tl *TaskList, runIdMap map[int]int, setIdMap map[int]int) error {

	// validate parameters
	if tl == nil {
		return nil // source is empty: nothing to do, exit
	}
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return errors.New("invalid (empty) language list")
	}
	if tl.ModelName != modelDef.Model.Name || tl.ModelDigest != modelDef.Model.Digest {
		return errors.New("invalid model name " + tl.ModelName + " or digest " + tl.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}
	if len(tl.Lst) <= 0 {
		return nil // source is empty: nothing to do, exit
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err = doUpdateTaskList(trx, modelDef, langDef, tl.Lst, runIdMap, setIdMap); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()

	return nil
}

// doUpdateTaskList insert new or update existing tasks and task run history in database.
// It does update as part of transaction
// Model id, task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func doUpdateTaskList(
	trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, taskLst []TaskMeta, runIdMap map[int]int, setIdMap map[int]int) error {

	smId := strconv.Itoa(modelDef.Model.ModelId)

	for idx := range taskLst {

		// task name cannot be empty
		if taskLst[idx].Task.Name == "" {
			return errors.New("invalid (empty) task name, id: " + strconv.Itoa(taskLst[idx].Task.TaskId))
		}
		taskLst[idx].Task.ModelId = modelDef.Model.ModelId // update model id

		// get new task id if task not exist
		//
		// UPDATE id_lst SET id_value =
		//   CASE
		//     WHEN 0 = (SELECT COUNT(*) FROM task_lst WHERE model_id = 1 AND task_name = 'task_44')
		//       THEN id_value + 1
		//     ELSE id_value
		//   END
		// WHERE id_key = 'task_id'
		err := TrxUpdate(trx,
			"UPDATE id_lst SET id_value ="+
				" CASE"+
				" WHEN 0 ="+
				" (SELECT COUNT(*) FROM task_lst"+
				" WHERE model_id = "+smId+" AND task_name = "+toQuoted(taskLst[idx].Task.Name)+
				" )"+
				" THEN id_value + 1"+
				" ELSE id_value"+
				" END"+
				" WHERE id_key = 'task_id'")
		if err != nil {
			return err
		}

		// check if this task already exist
		taskId := 0
		err = TrxSelectFirst(trx,
			"SELECT task_id FROM task_lst WHERE model_id = "+smId+" AND task_name = "+toQuoted(taskLst[idx].Task.Name),
			func(row *sql.Row) error {
				return row.Scan(&taskId)
			})
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		// if task not exist then insert new else update existing
		if taskId <= 0 {

			// get new task id
			err = TrxSelectFirst(trx,
				"SELECT id_value FROM id_lst WHERE id_key = 'task_id'",
				func(row *sql.Row) error {
					return row.Scan(&taskId)
				})
			switch {
			case err == sql.ErrNoRows:
				return errors.New("invalid destination database, likely not an openM++ database")
			case err != nil:
				return err
			}
			taskLst[idx].Task.TaskId = taskId // update task id with actual value

			// insert new task
			err = doInsertTask(trx, modelDef, langDef, &taskLst[idx], runIdMap, setIdMap)
			if err != nil {
				return err
			}

		} else { // update existing task

			taskLst[idx].Task.TaskId = taskId // update task id with actual value

			err = doUpdateTask(trx, modelDef, langDef, &taskLst[idx], runIdMap, setIdMap)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// doInsertTask insert new workset metadata in database.
// It does update as part of transaction
// Task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func doInsertTask(
	trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, meta *TaskMeta, runIdMap map[int]int, setIdMap map[int]int) error {

	stId := strconv.Itoa(meta.Task.TaskId)

	// INSERT INTO task_lst (task_id, model_id, task_name) VALUES (88, 11, 'modelOne task')
	err := TrxUpdate(trx,
		"INSERT INTO task_lst (task_id, model_id, task_name) VALUES ("+
			stId+", "+
			strconv.Itoa(modelDef.Model.ModelId)+", "+
			toQuoted(meta.Task.Name)+")")
	if err != nil {
		return err
	}

	// insert new rows into task body tables: task_txt, task_set, task_run_lst, task_run_set
	if err = doInsertTaskBody(trx, modelDef, langDef, meta, runIdMap, setIdMap); err != nil {
		return err
	}
	return nil
}

// doUpdateTask update workset metadata in database.
// It does update as part of transaction
// Task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func doUpdateTask(
	trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, meta *TaskMeta, runIdMap map[int]int, setIdMap map[int]int) error {

	// update with unique name
	stId := strconv.Itoa(meta.Task.TaskId)
	sn := "task_" + stId + "_" + helper.MakeDateTime(time.Now())

	// UPDATE task_lst SET task_name = 'task_88_2014-08-17 16:57:04.0123' WHERE task_id = 88
	err := TrxUpdate(trx,
		"UPDATE task_lst SET task_name = "+toQuoted(sn)+" WHERE task_id = "+stId)
	if err != nil {
		return err
	}

	// delete existing task_run_set, task_run_lst, task_set, task_txt
	err = TrxUpdate(trx, "DELETE FROM task_run_set WHERE task_id ="+stId)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx, "DELETE FROM task_run_lst WHERE task_id ="+stId)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx, "DELETE FROM task_set WHERE task_id ="+stId)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx, "DELETE FROM task_txt WHERE task_id ="+stId)
	if err != nil {
		return err
	}

	// insert new rows into task body tables: task_txt, task_set, task_run_lst, task_run_set
	if err = doInsertTaskBody(trx, modelDef, langDef, meta, runIdMap, setIdMap); err != nil {
		return err
	}

	// restore actual task name
	// UPDATE task_lst SET task_name = 'my task' WHERE task_id = 88
	err = TrxUpdate(trx,
		"UPDATE task_lst SET task_name = "+toQuoted(meta.Task.Name)+" WHERE task_id = "+stId)
	if err != nil {
		return err
	}
	return nil
}

// doInsertTaskBody insert new rows into task body tables: task_txt, task_set, task_run_lst, task_run_set
// It does update as part of transaction
// Task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func doInsertTaskBody(
	trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, meta *TaskMeta, runIdMap map[int]int, setIdMap map[int]int) error {

	stId := strconv.Itoa(meta.Task.TaskId)

	// update task text (description and notes)
	for j := range meta.Txt {

		// update task id and language id
		meta.Txt[j].TaskId = meta.Task.TaskId

		k, ok := langDef.codeIndex[meta.Txt[j].LangCode]
		if !ok {
			return errors.New("invalid language code " + meta.Txt[j].LangCode)
		}
		meta.Txt[j].LangId = langDef.LangWord[k].LangId

		// insert into task_txt
		err := TrxUpdate(trx,
			"INSERT INTO task_txt (task_id, lang_id, descr, note) VALUES ("+
				stId+", "+
				strconv.Itoa(meta.Txt[j].LangId)+", "+
				toQuoted(meta.Txt[j].Descr)+", "+
				toQuotedOrNull(meta.Txt[j].Note)+")")
		if err != nil {
			return err
		}
	}

	// update task input: task workset list
	// remove workset if it is not exist in destination database
	for j := range meta.Set {

		// update set id with actual set id in database
		i, ok := setIdMap[meta.Set[j]]
		if !ok {
			continue // skip workset if not exist in destination database
		}
		meta.Set[j] = i

		// insert into task_set
		err := TrxUpdate(trx,
			"INSERT INTO task_set (task_id, set_id) VALUES ("+
				stId+", "+
				strconv.Itoa(meta.Set[j])+")")
		if err != nil {
			return err
		}
	}

	// task run history and status
	trm := make(map[int]int, len(meta.TaskRun))

	for j := range meta.TaskRun {

		// update task id
		meta.TaskRun[j].TaskId = meta.Task.TaskId

		// get new task run id
		id := 0

		err := TrxUpdate(trx, "UPDATE id_lst SET id_value = id_value + 1 WHERE id_key = 'task_run_id'")
		if err != nil {
			return err
		}
		err = TrxSelectFirst(trx,
			"SELECT id_value FROM id_lst WHERE id_key = 'task_run_id'",
			func(row *sql.Row) error {
				return row.Scan(&id)
			})
		switch {
		case err == sql.ErrNoRows:
			return errors.New("invalid destination database, likely not an openM++ database")
		case err != nil:
			return err
		}

		trm[meta.TaskRun[j].TaskRunId] = id
		meta.TaskRun[j].TaskRunId = id // update task run id with new id from database

		// insert into task_run_lst
		err = TrxUpdate(trx,
			"INSERT INTO task_run_lst (task_run_id, task_id, sub_count, create_dt, status, update_dt)"+
				" VALUES ("+
				strconv.Itoa(id)+", "+
				stId+", "+
				strconv.Itoa(meta.TaskRun[j].SubCount)+", "+
				toQuoted(meta.TaskRun[j].CreateDateTime)+", "+
				toQuoted(meta.TaskRun[j].Status)+", "+
				toQuoted(meta.TaskRun[j].UpdateDateTime)+")")
		if err != nil {
			return err
		}
	}

	// update task run history body: task run id, run id, set id
	// remove task run, model run and workset if it is not exist in destination database
	for j := range meta.TaskRunSet {

		// update task run id with new id from  database
		i, ok := trm[meta.TaskRunSet[j].TaskRunId]
		if !ok {
			continue // skip: task run not exist in destination database
		}
		meta.TaskRunSet[j].TaskRunId = i

		// update run id with actual set id in database
		i, ok = runIdMap[meta.TaskRunSet[j].RunId]
		if !ok {
			continue // skip: model run not exist in destination database
		}
		meta.TaskRunSet[j].RunId = i

		// update set id with actual set id in database
		i, ok = setIdMap[meta.TaskRunSet[j].SetId]
		if !ok {
			continue // skip workset if not exist in destination database
		}
		meta.TaskRunSet[j].SetId = i

		// update task id
		meta.TaskRunSet[j].TaskId = meta.Task.TaskId

		// insert into task_run_set
		err := TrxUpdate(trx,
			"INSERT INTO task_run_set (task_run_id, run_id, set_id, task_id)"+
				" VALUES ("+
				strconv.Itoa(meta.TaskRunSet[j].TaskRunId)+", "+
				strconv.Itoa(meta.TaskRunSet[j].RunId)+", "+
				strconv.Itoa(meta.TaskRunSet[j].SetId)+", "+
				stId+")")
		if err != nil {
			return err
		}
	}

	return nil
}
