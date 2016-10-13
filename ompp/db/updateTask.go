// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"sort"
	"strconv"
	"time"

	"go.openmpp.org/ompp/helper"
)

// FromPublic convert modeling task metadata from "public" format (coming from json import-export) into db rows.
//
// It return task metadata and two boolean flags:
// (1) isSetNotFound = true if some of task workset names not found in current database
// (2) isTaskRunNotFound = true if some of task run (pairs of set, model run) set or model run not found in current database.
//
// Worksets are searched by set name, which is unique inside of the model.
// Model run serached by:
// if run digest not empty then by digest;
// else if run status is error then by run_name, sub_count, sub_completed, status, create_dt.
func (pub *TaskPub) FromPublic(dbConn *sql.DB, modelDef *ModelMeta, langDef *LangMeta) (*TaskMeta, bool, bool, error) {

	// validate parameters
	if modelDef == nil {
		return nil, false, false, errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return nil, false, false, errors.New("invalid (empty) language list")
	}
	if pub.ModelName == "" && pub.ModelDigest == "" {
		return nil, false, false, errors.New("invalid (empty) model name and digest, modeling task: " + pub.Name)
	}

	// validate task model name and/or digest: task must belong to the model
	if (pub.ModelName != "" && pub.ModelName != modelDef.Model.Name) ||
		(pub.ModelDigest != "" && pub.ModelDigest != modelDef.Model.Digest) {
		return nil, false, false, errors.New("invalid model name " + pub.ModelName + " or digest " + pub.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// task header: task_lst row with zero default task id
	meta := TaskMeta{
		Task: TaskRow{
			TaskId:  0, // task id is undefined
			ModelId: modelDef.Model.ModelId,
			Name:    pub.Name,
		},
		Txt: make([]TaskTxtRow, len(pub.Txt)),
	}

	// task description and notes: task_txt rows
	// use task id default zero
	for k := range pub.Txt {
		meta.Txt[k].LangCode = pub.Txt[k].LangCode
		meta.Txt[k].LangId = langDef.IdByCode(pub.Txt[k].LangCode)
		meta.Txt[k].Descr = pub.Txt[k].Descr
		meta.Txt[k].Note = pub.Txt[k].Note
	}

	// task_set rows:
	// find "public" workset name in current database
	// ignore worksets, which does not exist
	wni := make(map[string]int, len(pub.TaskRun)) // map (set name) => set id

	err := SelectRows(dbConn,
		"SELECT W.set_id, W.set_name"+
			" FROM workset_lst W WHERE W.model_id = "+strconv.Itoa(modelDef.Model.ModelId)+
			" ORDER BY 1",
		func(rows *sql.Rows) error {

			var id int
			var sn string
			if err := rows.Scan(&id, &sn); err != nil {
				return err
			}

			for k := range pub.Set { // include only set id's where name is "public" metadata name list
				if sn == pub.Set[k] {
					meta.Set = append(meta.Set, id) // workset found
					break
				}
			}

			// if workset name in the any of model run then include it in the map of [name]=>set id
		trLoop:
			for k := range pub.TaskRun {
				for j := range pub.TaskRun[k].TaskRunSet {
					if sn == pub.TaskRun[k].TaskRunSet[j].SetName {
						wni[sn] = id
						break trLoop
					}
				}
			}
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return nil, false, false, err
	}
	isSetNotFound := len(pub.Set) > len(meta.Set) // some "public" workset names not found in database

	// build task run history body as list of (run id, set id)
	// select only runs where status is completed (success, exit, error)
	// if digest not empty then use digest as run key
	// else if run status is error then use run_name, sub_count, sub_completed, status, create_dt

	tri := make(map[int][]TaskRunSetRow, len(pub.TaskRun)) // map [index in pub.TaskRun] => [](run id, set id)

	err = SelectRows(dbConn,
		"SELECT R.run_id, R.run_name, R.sub_count, R.sub_completed, R.create_dt, R.status, R.run_digest"+
			" FROM run_lst R"+
			" WHERE R.model_id = "+strconv.Itoa(modelDef.Model.ModelId)+
			" AND R.status IN ("+toQuoted(DoneRunStatus)+", "+toQuoted(ErrorRunStatus)+", "+toQuoted(ExitRunStatus)+")"+
			" ORDER BY 1",
		func(rows *sql.Rows) error {

			var rId int
			var trsName string
			var trsSubCount int
			var trsSubCompleted int
			var trsCreateDateTime string
			var trsStatus string
			var dg sql.NullString
			if err := rows.Scan(
				&rId, &trsName, &trsSubCount, &trsSubCompleted, &trsCreateDateTime, &trsStatus, &dg); err != nil {
				return err
			}
			sd := ""
			if dg.Valid {
				sd = dg.String // run digest
			}

			// find pair of (run, set) in the history of task run
			for k := range pub.TaskRun {
				for j := range pub.TaskRun[k].TaskRunSet {

					// find set by name
					sId, ok := wni[pub.TaskRun[k].TaskRunSet[j].SetName]
					if !ok {
						continue // skip: no set id for that row of task run history
					}

					// find run:
					// if digest not empty then by digest
					// else if run status is error then by name, status...
					if (sd != "" && sd == pub.TaskRun[k].TaskRunSet[j].Run.Digest) ||
						(sd == "" &&
							trsStatus == ErrorRunStatus &&
							trsName == pub.TaskRun[k].TaskRunSet[j].Run.Name &&
							trsSubCount == pub.TaskRun[k].SubCount &&
							trsSubCompleted == pub.TaskRun[k].TaskRunSet[j].Run.SubCompleted &&
							trsCreateDateTime == pub.TaskRun[k].TaskRunSet[j].Run.CreateDateTime &&
							trsStatus == pub.TaskRun[k].TaskRunSet[j].Run.Status) {

						rsLst := tri[k]
						tri[k] = append(rsLst, TaskRunSetRow{RunId: rId, SetId: sId}) // add (run id, set id) to task run history
						break
					}
				}
			}

			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return nil, false, false, err
	}

	// sort task run history in the order of task runs in "public" pub.TaskRun list
	idxArr := make([]int, 0, len(tri))
	for idx := range tri {
		idxArr = append(idxArr, idx)
	}
	sort.Ints(idxArr)

	// build metadata db rows of task run history
	// task id and task run id = zero default
	isTaskRunNotFound := false

	meta.TaskRun = make([]taskRunItem, len(idxArr))
	for k, idx := range idxArr {

		// header: task run status,...
		meta.TaskRun[k].Status = pub.TaskRun[idx].Status
		meta.TaskRun[k].SubCount = pub.TaskRun[idx].SubCount
		meta.TaskRun[k].CreateDateTime = pub.TaskRun[idx].CreateDateTime
		meta.TaskRun[k].UpdateDateTime = pub.TaskRun[idx].UpdateDateTime

		// task run body: pairs of (run id, set id)
		meta.TaskRun[k].TaskRunSet = tri[idx]

		// set flag if any run is or set id not found in target database
		if !isTaskRunNotFound {
			isTaskRunNotFound = len(meta.TaskRun[k].TaskRunSet) != len(pub.TaskRun[idx].TaskRunSet)
		}
	}
	if !isTaskRunNotFound { // set flag if any row in task run history not found in target database
		isTaskRunNotFound = len(meta.TaskRun) != len(pub.TaskRun)
	}

	return &meta, isSetNotFound, isTaskRunNotFound, nil
}

// UpdateTask insert new or update existing modeling task and task run history in database.
//
// Model id, task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func (meta *TaskMeta) UpdateTask(dbConn *sql.DB, modelDef *ModelMeta) error {

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if meta.Task.ModelId != modelDef.Model.ModelId {
		return errors.New("model task: " + strconv.Itoa(meta.Task.TaskId) + " " + meta.Task.Name + " invalid model id " + strconv.Itoa(meta.Task.ModelId) + " expected: " + strconv.Itoa(modelDef.Model.ModelId))
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err = doUpdateOrInsertTask(trx, modelDef, meta); err != nil {
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
func doUpdateOrInsertTask(trx *sql.Tx, modelDef *ModelMeta, meta *TaskMeta) error {

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
		if err = doInsertTask(trx, modelDef, meta); err != nil {
			return err
		}

	} else { // update existing task

		meta.Task.TaskId = taskId // update task id with actual value

		if err = doUpdateTask(trx, modelDef, meta); err != nil {
			return err
		}
	}

	return nil
}

// doInsertTask insert new workset metadata in database.
// It does update as part of transaction
// Task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func doInsertTask(trx *sql.Tx, modelDef *ModelMeta, meta *TaskMeta) error {

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
	if err = doInsertTaskBody(trx, modelDef, meta); err != nil {
		return err
	}
	return nil
}

// doUpdateTask update workset metadata in database.
// It does update as part of transaction
// Task id, run id, set id updated with actual database id's.
// Remove non-existing worksets and model runs from task
func doUpdateTask(trx *sql.Tx, modelDef *ModelMeta, meta *TaskMeta) error {

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
	if err = doInsertTaskBody(trx, modelDef, meta); err != nil {
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
func doInsertTaskBody(trx *sql.Tx, modelDef *ModelMeta, meta *TaskMeta) error {

	stId := strconv.Itoa(meta.Task.TaskId)

	// update task text (description and notes)
	for j := range meta.Txt {

		// update task id and language id
		meta.Txt[j].TaskId = meta.Task.TaskId

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
			"INSERT INTO task_run_lst (task_run_id, task_id, sub_count, create_dt, status, update_dt)"+
				" VALUES ("+
				strconv.Itoa(id)+", "+
				stId+", "+
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
