// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"sort"
	"strconv"
)

// ToPublic convert modeling task metadata db rows into "public" modeling task format for json import-export.
func (meta *TaskMeta) ToPublic(dbConn *sql.DB, modelDef *ModelMeta) (*TaskPub, error) {

	// validate model id: task must belong to the model
	if meta.Task.ModelId != modelDef.Model.ModelId {
		return nil, errors.New("task: " + strconv.Itoa(meta.Task.TaskId) + " " + meta.Task.Name + ", model id " + strconv.Itoa(meta.Task.ModelId) + " expected: " + strconv.Itoa(modelDef.Model.ModelId))
	}

	// task header
	pub := TaskPub{
		TaskDefPub: TaskDefPub{
			ModelName:   modelDef.Model.Name,
			ModelDigest: modelDef.Model.Digest,
			Name:        meta.Task.Name,
			Txt:         make([]DescrNote, len(meta.Txt)),
			Set:         []string{}},
		TaskRun: []taskRunPub{},
	}

	// task description and notes by language
	for k := range meta.Txt {
		pub.Txt[k] = DescrNote{
			LangCode: meta.Txt[k].LangCode,
			Descr:    meta.Txt[k].Descr,
			Note:     meta.Txt[k].Note}
	}

	// task workset list:
	// select workset names for the task
	// ignore worksets if id is not in the input list of task set id's
	if len(meta.Set) > 0 {
		err := SelectRows(dbConn,
			"SELECT W.set_id, W.set_name"+
				" FROM task_set TS"+
				" INNER JOIN workset_lst W ON (W.set_id = TS.set_id)"+
				" WHERE TS.task_id = "+strconv.Itoa(meta.Task.TaskId)+
				" ORDER BY 1",
			func(rows *sql.Rows) error {
				var id int
				var sn string
				if err := rows.Scan(&id, &sn); err != nil {
					return err
				}
				for _, i := range meta.Set { // include only set id's which are in the meta list of set id's
					if i == id {
						pub.Set = append(pub.Set, sn) // workset found
						return nil
					}
				}
				return nil // ignore set id which is not found in the input list of task set id's
			})
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
	}

	// task run history: header rows
	// select list task run's
	// ignore task run if id is not in the input list of task run id's
	if len(meta.TaskRun) > 0 {

		ri := make(map[int]int) // map (task run id) => index in task run array

		err := SelectRows(dbConn,
			"SELECT TR.task_run_id, TR.run_name, TR.sub_count, TR.create_dt, TR.status, TR.update_dt"+
				" FROM task_run_lst TR"+
				" WHERE TR.task_id = "+strconv.Itoa(meta.Task.TaskId)+
				" ORDER BY 1",
			func(rows *sql.Rows) error {
				var id int
				var r taskRunPub
				if err := rows.Scan(&id, &r.Name, &r.SubCount, &r.CreateDateTime, &r.Status, &r.UpdateDateTime); err != nil {
					return err
				}
				for k := range meta.TaskRun { // include only task run id's which are in the meta list of run id's
					if id == meta.TaskRun[k].TaskRunId {
						ri[meta.TaskRun[k].TaskRunId] = len(pub.TaskRun) // index of task run id
						pub.TaskRun = append(pub.TaskRun, r)             // task run id found
						return nil
					}
				}
				return nil // ignore task run id which is not found in the input list of run id's
			})
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}

		// task run history body: pairs of (set id, run id)
		// select list task run's body rows
		// ignore task run body row if any of: (task run id, run id, set id) is not in the input list of id's
		err = SelectRows(dbConn,
			"SELECT"+
				" TRS.task_run_id, TRS.run_id, TRS.set_id, W.set_name,"+
				" R.run_name, R.sub_completed, R.create_dt, R.status, R.run_digest"+
				" FROM task_run_set TRS"+
				" INNER JOIN workset_lst W ON (W.set_id = TRS.set_id)"+
				" INNER JOIN run_lst R ON (R.run_id = TRS.run_id)"+
				" WHERE TRS.task_id = "+strconv.Itoa(meta.Task.TaskId)+
				" ORDER BY 1, 2",
			func(rows *sql.Rows) error {
				var trId, wId, rId int
				var r taskRunSetPub
				if err := rows.Scan(&trId, &rId, &wId, &r.SetName,
					&r.Run.Name, &r.Run.SubCompleted, &r.Run.CreateDateTime, &r.Run.Status, &r.Run.Digest); err != nil {
					return err
				}
				for k := range meta.TaskRun { // include only task run id's which are in the meta list of run id's

					if trId != meta.TaskRun[k].TaskRunId { // skip if task run id not the same as in db row
						continue
					}

					// find pair of (run id, set id) in the metadata
					for j := range meta.TaskRun[k].TaskRunSet {

						if rId != meta.TaskRun[k].TaskRunSet[j].RunId || wId != meta.TaskRun[k].TaskRunSet[j].SetId {
							continue // skip if db row run id or set id is not in the metadata
						}
						// task run id, run id, set id found in the input meta task run list
						// get index of that task run id in the "public" task run list
						if i, ok := ri[meta.TaskRun[k].TaskRunId]; ok {
							pub.TaskRun[i].TaskRunSet = append(pub.TaskRun[i].TaskRunSet, r) // found
						}
						return nil // done with that db row
					}
					return nil // db row not found: no such run id or set id
				}
				return nil // ignore task run id which is not found in the input list of run id's
			})
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
	}

	return &pub, nil
}

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
func (pub *TaskPub) FromPublic(dbConn *sql.DB, modelDef *ModelMeta) (*TaskMeta, bool, bool, error) {

	// validate parameters
	if modelDef == nil {
		return nil, false, false, errors.New("invalid (empty) model metadata")
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
		TaskDef: TaskDef{
			Task: TaskRow{
				TaskId:  0, // task id is undefined
				ModelId: modelDef.Model.ModelId,
				Name:    pub.Name,
			},
			Txt: make([]TaskTxtRow, len(pub.Txt)),
		},
		TaskRun: []taskRunItem{},
	}

	// task description and notes: task_txt rows
	// use task id default zero
	for k := range pub.Txt {
		meta.Txt[k].LangCode = pub.Txt[k].LangCode
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

		// header: task run name, status,...
		meta.TaskRun[k].Name = pub.TaskRun[idx].Name
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
