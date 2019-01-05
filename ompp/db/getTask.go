// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// GetTask return modeling task rows by id: task_lst table row and set ids from task_set
func GetTask(dbConn *sql.DB, taskId int) (*TaskRow, error) {
	return getTaskRow(dbConn,
		"SELECT K.task_id, K.model_id, K.task_name FROM task_lst K"+
			" WHERE K.task_id ="+strconv.Itoa(taskId))
}

// GetTaskByName return modeling task rows by id: task_lst table row and set ids from task_set
func GetTaskByName(dbConn *sql.DB, modelId int, name string) (*TaskRow, error) {
	return getTaskRow(dbConn,
		"SELECT K.task_id, K.model_id, K.task_name FROM task_lst K"+
			" WHERE K.task_id = "+
			" ("+
			" SELECT MIN(M.task_id) FROM task_lst M"+
			" WHERE M.model_id = "+strconv.Itoa(modelId)+" AND M.task_name ="+toQuoted(name)+
			" )")
}

// GetTaskList return list of model tasks: task_lst rows.
func GetTaskList(dbConn *sql.DB, modelId int) ([]TaskRow, error) {

	// model not found: model id must be positive
	if modelId <= 0 {
		return nil, nil
	}

	// get list of modeling task for that model id
	q := "SELECT K.task_id, K.model_id, K.task_name FROM task_lst K" +
		" WHERE K.model_id =" + strconv.Itoa(modelId) +
		" ORDER BY 1"

	taskRs, err := getTaskLst(dbConn, q)
	if err != nil {
		return nil, err
	}
	if len(taskRs) <= 0 { // no tasks found
		return nil, nil
	}
	return taskRs, nil
}

// GetTaskListText return list of modeling tasks with description and notes: task_lst and task_txt rows.
//
// If langCode not empty then only specified language selected else all languages
func GetTaskListText(dbConn *sql.DB, modelId int, langCode string) ([]TaskRow, []TaskTxtRow, error) {

	// model not found: model id must be positive
	if modelId <= 0 {
		return nil, nil, nil
	}

	// get list of modeling task for that model id
	q := "SELECT K.task_id, K.model_id, K.task_name FROM task_lst K" +
		" WHERE K.model_id =" + strconv.Itoa(modelId) +
		" ORDER BY 1"

	taskRs, err := getTaskLst(dbConn, q)
	if err != nil {
		return nil, nil, err
	}
	if len(taskRs) <= 0 { // no tasks found
		return nil, nil, err
	}

	// get tasks description and notes by model id
	q = "SELECT M.task_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM task_txt M" +
		" INNER JOIN task_lst K ON (K.task_id = M.task_id)" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE K.model_id = " + strconv.Itoa(modelId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2"

	txtRs, err := getTaskText(dbConn, q)

	return taskRs, txtRs, nil
}

// GetTaskSetIds return modeling task set id's by task id from task_set db table
func GetTaskSetIds(dbConn *sql.DB, taskId int) ([]int, error) {

	var idRs []int

	err := SelectRows(dbConn,
		"SELECT TS.set_id FROM task_set TS WHERE TS.task_id = "+strconv.Itoa(taskId)+" ORDER BY 1",
		func(rows *sql.Rows) error {
			var id int
			if err := rows.Scan(&id); err != nil {
				return err
			}
			idRs = append(idRs, id)
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	return idRs, nil
}

// getTaskRow return task_lst table row.
func getTaskRow(dbConn *sql.DB, query string) (*TaskRow, error) {

	var taskRow TaskRow

	err := SelectFirst(dbConn, query,
		func(row *sql.Row) error {
			if err := row.Scan(
				&taskRow.TaskId, &taskRow.ModelId, &taskRow.Name); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return &taskRow, nil
}

// getTaskLst return task_lst table rows.
func getTaskLst(dbConn *sql.DB, query string) ([]TaskRow, error) {

	// get list of modeling task for that model id
	var taskRs []TaskRow

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r TaskRow
			if err := rows.Scan(&r.TaskId, &r.ModelId, &r.Name); err != nil {
				return err
			}
			taskRs = append(taskRs, r)
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return taskRs, nil
}

// getTaskSetLst return modeling tasks id map to set ids from task_set table
func getTaskSetLst(dbConn *sql.DB, query string) (map[int][]int, error) {

	var tsRs = make(map[int][]int)

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var taskId, setId int
			if err := rows.Scan(&taskId, &setId); err != nil {
				return err
			}
			tsRs[taskId] = append(tsRs[taskId], setId)
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	return tsRs, nil
}

// GetTaskText return modeling task description and notes: task_txt table rows.
//
// If langCode not empty then only specified language selected else all languages
func GetTaskText(dbConn *sql.DB, taskId int, langCode string) ([]TaskTxtRow, error) {

	q := "SELECT M.task_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM task_txt M" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE M.task_id = " + strconv.Itoa(taskId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2"

	return getTaskText(dbConn, q)
}

// getRunText return modeling task description and notes: task_txt table rows.
func getTaskText(dbConn *sql.DB, query string) ([]TaskTxtRow, error) {

	// select db rows from task_txt
	var txtLst []TaskTxtRow

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r TaskTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.TaskId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			txtLst = append(txtLst, r)
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return txtLst, nil
}

// GetTaskFirstRun return first run of the modeling task: task_run_lst table row.
func GetTaskFirstRun(dbConn *sql.DB, taskId int) (*TaskRunRow, error) {

	return getTaskRunRow(dbConn,
		"SELECT"+
			" R.task_run_id, R.task_id, R.sub_count, R.create_dt, R.status, R.update_dt"+
			" FROM task_run_lst R"+
			" WHERE R.task_run_id ="+
			" (SELECT MIN(M.task_run_id) FROM task_run_lst M WHERE M.task_id = "+strconv.Itoa(taskId)+")")
}

// GetTaskLastRun return last run of the modeling task: task_run_lst table row.
func GetTaskLastRun(dbConn *sql.DB, taskId int) (*TaskRunRow, error) {

	return getTaskRunRow(dbConn,
		"SELECT"+
			" R.task_run_id, R.task_id, R.sub_count, R.create_dt, R.status, R.update_dt"+
			" FROM task_run_lst R"+
			" WHERE R.task_run_id ="+
			" (SELECT MAX(M.task_run_id) FROM task_run_lst M WHERE M.task_id = "+strconv.Itoa(taskId)+")")
}

// GetTaskLastCompletedRun return last completed run of the modeling task: task_run_lst table row.
//
// Task run completed if run status one of: s=success, x=exit, e=error
func GetTaskLastCompletedRun(dbConn *sql.DB, taskId int) (*TaskRunRow, error) {

	return getTaskRunRow(dbConn,
		"SELECT"+
			" R.task_run_id, R.task_id, R.sub_count, R.create_dt, R.status, R.update_dt"+
			" FROM task_run_lst R"+
			" WHERE R.task_run_id ="+
			" ("+
			" SELECT MAX(M.task_run_id) FROM task_run_lst M"+
			" WHERE M.task_id = "+strconv.Itoa(taskId)+
			" AND M.status IN ("+toQuoted(DoneRunStatus)+", "+toQuoted(ErrorRunStatus)+", "+toQuoted(ExitRunStatus)+")"+
			" )")
}

// GetTaskRun return modeling task run status and result: task_run_lst and task_run_set table rows.
func GetTaskRun(dbConn *sql.DB, taskRunId int) (*TaskRunRow, []TaskRunSetRow, error) {

	// get task run status
	trRow, err := getTaskRunRow(dbConn,
		"SELECT task_run_id, task_id, sub_count, create_dt, status, update_dt"+
			" FROM task_run_lst"+
			" WHERE task_run_id = "+strconv.Itoa(taskRunId))
	if err != nil {
		return nil, nil, err
	}
	if trRow == nil {
		return nil, nil, nil // task run not found
	}

	// select run results for that task run
	trsRs, err := getTaskRunSetLst(dbConn,
		"SELECT task_run_id, run_id, set_id, task_id"+
			" FROM task_run_set"+
			" WHERE task_run_id = "+strconv.Itoa(taskRunId)+
			" ORDER BY 1, 2")
	if err != nil {
		return nil, nil, err
	}
	return trRow, trsRs, nil
}

// GetTaskRunByTaskId return list of modeling task runs by task id: task_run_lst table rows.
func GetTaskRunByTaskId(dbConn *sql.DB, taskId int) ([]TaskRunRow, error) {

	return getTaskRunLst(dbConn,
		"SELECT task_run_id, task_id, sub_count, create_dt, status, update_dt"+
			" FROM task_run_lst"+
			" WHERE task_id = "+strconv.Itoa(taskId)+
			" ORDER BY 1")
}

// getTaskRunRow return task_run_lst table row.
func getTaskRunRow(dbConn *sql.DB, query string) (*TaskRunRow, error) {

	var trRow TaskRunRow

	err := SelectFirst(dbConn, query,
		func(row *sql.Row) error {
			if err := row.Scan(
				&trRow.TaskRunId, &trRow.TaskId, &trRow.SubCount, &trRow.CreateDateTime, &trRow.Status, &trRow.UpdateDateTime); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return &trRow, nil
}

// getTaskRunLst return list of modeling task runs: task_run_lst table rows.
func getTaskRunLst(dbConn *sql.DB, query string) ([]TaskRunRow, error) {

	var trRs []TaskRunRow

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r TaskRunRow
			if err := rows.Scan(
				&r.TaskRunId, &r.TaskId, &r.SubCount, &r.CreateDateTime, &r.Status, &r.UpdateDateTime); err != nil {
				return err
			}
			trRs = append(trRs, r)
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	return trRs, nil
}

// getTaskRunLst return list of modeling task run body: task_run_set table rows.
func getTaskRunSetLst(dbConn *sql.DB, query string) ([]TaskRunSetRow, error) {

	var trsRs []TaskRunSetRow

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r TaskRunSetRow
			if err := rows.Scan(&r.TaskRunId, &r.RunId, &r.SetId, &r.TaskId); err != nil {
				return err
			}
			trsRs = append(trsRs, r)
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return trsRs, nil
}

// GetTaskFull return modeling task metadata, description, notes and run history
// from db-tables: task_lst, task_txt, task_set, task_run_lst, task_run_set.
//
// It does not return non-completed task runs (run in progress).
// If langCode not empty then only specified language selected else all languages
func GetTaskFull(dbConn *sql.DB, taskRow *TaskRow, langCode string) (*TaskMeta, error) {

	// validate parameters
	if taskRow == nil {
		return nil, errors.New("invalid (empty) task row, it may be task not found")
	}

	// where filters
	taskWhere := " WHERE K.task_id = " + strconv.Itoa(taskRow.TaskId)

	statusFilter := " AND H.status IN (" +
		toQuoted(DoneRunStatus) + ", " + toQuoted(ErrorRunStatus) + ", " + toQuoted(ExitRunStatus) + ")"

	var langFilter string
	if langCode != "" {
		langFilter = " AND L.lang_code = " + toQuoted(langCode)
	}

	// task meta header: task_lst row, model name and digest
	meta := &TaskMeta{Task: *taskRow}

	// get tasks description and notes by model id
	q := "SELECT M.task_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM task_txt M" +
		" INNER JOIN task_lst K ON (K.task_id = M.task_id)" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		taskWhere +
		langFilter +
		" ORDER BY 1, 2"

	txtRs, err := getTaskText(dbConn, q)
	meta.Txt = txtRs

	// get list of set ids for the task
	setRs, err := GetTaskSetIds(dbConn, taskRow.TaskId)
	if err != nil {
		return nil, err
	}
	meta.Set = setRs

	// get task run history and status
	q = "SELECT H.task_run_id, H.task_id, H.sub_count, H.create_dt, H.status, H.update_dt" +
		" FROM task_run_lst H" +
		" INNER JOIN task_lst K ON (K.task_id = H.task_id)" +
		taskWhere +
		statusFilter +
		" ORDER BY 1"

	runRs, err := getTaskRunLst(dbConn, q)
	if err != nil {
		return nil, err
	}

	meta.TaskRun = make([]taskRunItem, len(runRs))
	ri := make(map[int]int, len(runRs)) // map (task run id) => index in task run array

	for k := range runRs {
		ri[runRs[k].TaskRunId] = k
		meta.TaskRun[k].TaskRunRow = runRs[k]
	}

	// select run results for the tasks
	q = "SELECT M.task_run_id, M.run_id, M.set_id, M.task_id" +
		" FROM task_run_set M" +
		" INNER JOIN task_run_lst H ON (H.task_run_id = M.task_run_id)" +
		" INNER JOIN task_lst K ON (K.task_id = H.task_id)" +
		taskWhere +
		statusFilter +
		" ORDER BY 1, 2"

	runSetRs, err := getTaskRunSetLst(dbConn, q)
	if err != nil {
		return nil, err
	}

	for k := range runSetRs {
		if i, ok := ri[runSetRs[k].TaskRunId]; ok {
			meta.TaskRun[i].TaskRunSet = append(meta.TaskRun[i].TaskRunSet, runSetRs[k])
		}
	}

	return meta, nil
}

// GetTaskFullList return list of modeling tasks metadata, description, notes and run history
// from db-tables: task_lst, task_txt, task_set, task_run_lst, task_run_set.
//
// If isSuccess true then return only successfully completed task runs else all completed runs.
// It does not return non-completed task runs (run in progress).
// If langCode not empty then only specified language selected else all languages
func GetTaskFullList(dbConn *sql.DB, modelId int, isSuccess bool, langCode string) ([]TaskMeta, error) {

	// where filters
	var statusFilter string
	if isSuccess {
		statusFilter = " AND H.status = " + toQuoted(DoneRunStatus)
	} else {
		statusFilter = " AND H.status IN (" +
			toQuoted(DoneRunStatus) + ", " + toQuoted(ErrorRunStatus) + ", " + toQuoted(ExitRunStatus) + ")"
	}

	var langFilter string
	if langCode != "" {
		langFilter = " AND L.lang_code = " + toQuoted(langCode)
	}

	// get list of modeling task for that model id
	smId := strconv.Itoa(modelId)

	q := "SELECT K.task_id, K.model_id, K.task_name FROM task_lst K" +
		" WHERE K.model_id =" + smId +
		" ORDER BY 1"

	taskRs, err := getTaskLst(dbConn, q)
	if err != nil {
		return nil, err
	}

	// get tasks description and notes by model id
	q = "SELECT M.task_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM task_txt M" +
		" INNER JOIN task_lst K ON (K.task_id = M.task_id)" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE K.model_id = " + smId +
		langFilter +
		" ORDER BY 1, 2"

	txtRs, err := getTaskText(dbConn, q)

	// get list of set ids for each task
	q = "SELECT M.task_id, M.set_id" +
		" FROM task_set M" +
		" INNER JOIN task_lst K ON (K.task_id = M.task_id)" +
		" WHERE K.model_id = " + smId +
		" ORDER BY 1, 2"

	setRs, err := getTaskSetLst(dbConn, q)
	if err != nil {
		return nil, err
	}

	// get task run history and status
	q = "SELECT H.task_run_id, H.task_id, H.sub_count, H.create_dt, H.status, H.update_dt" +
		" FROM task_run_lst H" +
		" INNER JOIN task_lst K ON (K.task_id = H.task_id)" +
		" WHERE K.model_id = " + smId +
		statusFilter +
		" ORDER BY 1"

	runRs, err := getTaskRunLst(dbConn, q)
	if err != nil {
		return nil, err
	}

	// select run results for the tasks
	q = "SELECT M.task_run_id, M.run_id, M.set_id, M.task_id" +
		" FROM task_run_set M" +
		" INNER JOIN task_run_lst H ON (H.task_run_id = M.task_run_id)" +
		" INNER JOIN task_lst K ON (K.task_id = H.task_id)" +
		" WHERE K.model_id = " + smId +
		statusFilter +
		" ORDER BY 1, 2"

	runSetRs, err := getTaskRunSetLst(dbConn, q)
	if err != nil {
		return nil, err
	}

	// convert to output result: join run pieces in struct by task id
	tl := make([]TaskMeta, len(taskRs))
	im := make(map[int]int) // map [task id] => index of task_lst row

	for k := range taskRs {
		taskId := taskRs[k].TaskId
		tl[k].Task = taskRs[k]
		tl[k].Set = setRs[taskId]
		im[taskId] = k
	}
	for k := range txtRs {
		if i, ok := im[txtRs[k].TaskId]; ok {
			tl[i].Txt = append(tl[i].Txt, txtRs[k])
		}
	}
	for k := range runRs {
		if i, ok := im[runRs[k].TaskId]; ok {
			tl[i].TaskRun = append(tl[i].TaskRun, taskRunItem{TaskRunRow: runRs[k]})
		}
	}
	for k := range runSetRs {
		// find task run id in the list af task runs for the task
		// and append task pair of (run id, set id) to that task run
		if i, ok := im[runSetRs[k].TaskId]; ok {
			for j := range tl[i].TaskRun {
				if tl[i].TaskRun[j].TaskRunRow.TaskRunId == runSetRs[k].TaskRunId {
					tl[i].TaskRun[j].TaskRunSet = append(tl[i].TaskRun[j].TaskRunSet, runSetRs[k])
					break
				}
			}
		}
	}

	return tl, nil
}
