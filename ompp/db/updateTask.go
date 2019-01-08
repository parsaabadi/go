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

// UpdateTask insert new or update existing modeling task and task run history in database.
//
// Model id, task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func (meta *TaskMeta) UpdateTask(dbConn *sql.DB, modelDef *ModelMeta, langDef *LangMeta) error {

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return errors.New("invalid (empty) language list")
	}
	if meta.Task.ModelId != modelDef.Model.ModelId {
		return errors.New("model task: " + strconv.Itoa(meta.Task.TaskId) + " " + meta.Task.Name + " invalid model id " + strconv.Itoa(meta.Task.ModelId) + " expected: " + strconv.Itoa(modelDef.Model.ModelId))
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err = doUpdateOrInsertTask(trx, modelDef, meta, langDef); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()

	return nil
}

// doUpdateOrInsertTask insert new or update existing tasks and task run history in database.
// It does update as part of transaction
// Model id, task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func doUpdateOrInsertTask(trx *sql.Tx, modelDef *ModelMeta, meta *TaskMeta, langDef *LangMeta) error {

	smId := strconv.Itoa(modelDef.Model.ModelId)

	// task name cannot be empty
	if meta.Task.Name == "" {
		return errors.New("invalid (empty) task name, id: " + strconv.Itoa(meta.Task.TaskId))
	}
	meta.Task.ModelId = modelDef.Model.ModelId // update model id

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
			" WHERE model_id = "+smId+" AND task_name = "+toQuoted(meta.Task.Name)+
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
		"SELECT task_id FROM task_lst WHERE model_id = "+smId+" AND task_name = "+toQuoted(meta.Task.Name),
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
		meta.Task.TaskId = taskId // update task id with actual value

		// insert new task
		if err = doInsertTask(trx, modelDef, meta, langDef); err != nil {
			return err
		}

	} else { // update existing task

		meta.Task.TaskId = taskId // update task id with actual value

		if err = doUpdateTask(trx, modelDef, meta, langDef); err != nil {
			return err
		}
	}

	return nil
}

// doInsertTask insert new workset metadata in database.
// It does update as part of transaction
// Task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func doInsertTask(trx *sql.Tx, modelDef *ModelMeta, meta *TaskMeta, langDef *LangMeta) error {

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
	if err = doInsertTaskBody(trx, modelDef, meta, langDef); err != nil {
		return err
	}
	return nil
}

// doUpdateTask update workset metadata in database.
// It does update as part of transaction
// Task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func doUpdateTask(trx *sql.Tx, modelDef *ModelMeta, meta *TaskMeta, langDef *LangMeta) error {

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
	if err = doInsertTaskBody(trx, modelDef, meta, langDef); err != nil {
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
// Task id and task run id updated with actual database id's.
func doInsertTaskBody(trx *sql.Tx, modelDef *ModelMeta, meta *TaskMeta, langDef *LangMeta) error {

	stId := strconv.Itoa(meta.Task.TaskId)

	// update task text (description and notes)
	for j := range meta.Txt {

		// update task id
		meta.Txt[j].TaskId = meta.Task.TaskId

		// if language code valid then insert into task_txt
		if lId, ok := langDef.IdByCode(meta.Txt[j].LangCode); ok {

			err := TrxUpdate(trx,
				"INSERT INTO task_txt (task_id, lang_id, descr, note) VALUES ("+
					stId+", "+
					strconv.Itoa(lId)+", "+
					toQuoted(meta.Txt[j].Descr)+", "+
					toQuotedOrNull(meta.Txt[j].Note)+")")
			if err != nil {
				return err
			}
		}
	}

	// update task input: task workset list
	for j := range meta.Set {
		err := TrxUpdate(trx,
			"INSERT INTO task_set (task_id, set_id) VALUES ("+
				stId+", "+
				strconv.Itoa(meta.Set[j])+")")
		if err != nil {
			return err
		}
	}

	// task run history and status
	for k := range meta.TaskRun {

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
		meta.TaskRun[k].TaskRunId = id            // update task run id
		meta.TaskRun[k].TaskId = meta.Task.TaskId // update task id

		// insert into task_run_lst
		err = TrxUpdate(trx,
			"INSERT INTO task_run_lst (task_run_id, task_id, run_name, sub_count, create_dt, status, update_dt)"+
				" VALUES ("+
				strconv.Itoa(id)+", "+
				stId+", "+
				toQuoted(meta.TaskRun[k].Name)+", "+
				strconv.Itoa(meta.TaskRun[k].SubCount)+", "+
				toQuoted(meta.TaskRun[k].CreateDateTime)+", "+
				toQuoted(meta.TaskRun[k].Status)+", "+
				toQuoted(meta.TaskRun[k].UpdateDateTime)+")")
		if err != nil {
			return err
		}

		// update task run history body: set task run id and task id
		// remove task run if it is not exist in destination database
		for j := range meta.TaskRun[k].TaskRunSet {

			meta.TaskRun[k].TaskRunSet[j].TaskRunId = id            // update task run id
			meta.TaskRun[k].TaskRunSet[j].TaskId = meta.Task.TaskId // update task id

			// insert into task_run_set
			err := TrxUpdate(trx,
				"INSERT INTO task_run_set (task_run_id, run_id, set_id, task_id)"+
					" VALUES ("+
					strconv.Itoa(meta.TaskRun[k].TaskRunSet[j].TaskRunId)+", "+
					strconv.Itoa(meta.TaskRun[k].TaskRunSet[j].RunId)+", "+
					strconv.Itoa(meta.TaskRun[k].TaskRunSet[j].SetId)+", "+
					stId+")")
			if err != nil {
				return err
			}
		}
	}

	return nil
}
