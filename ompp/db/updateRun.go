// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.openmpp.org/ompp/helper"
)

// UpdateRun insert new or return existing model run metadata in database.
//
// Run status must be completed (success, exit or error) otherwise error returned.
// If this run already exist then nothing is updated in database, only metadata updated with actual run id.
// Following is used to find existing model run:
// if digest not "" empty then by run digest;
// else if status is error then by run_name, sub_count, sub_completed, status, create_dt.
//
// Double format is used for progress value float conversion, if non-empty format supplied.
//
// It return "is found" flag and update metadata with actual run id in database.
func (meta *RunMeta) UpdateRun(dbConn *sql.DB, modelDef *ModelMeta, langDef *LangMeta, doubleFmt string) (bool, error) {

	// validate parameters
	if modelDef == nil {
		return false, errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return false, errors.New("invalid (empty) language list")
	}
	if meta.Run.ModelId != modelDef.Model.ModelId {
		return false, errors.New("model run: " + strconv.Itoa(meta.Run.RunId) + " " + meta.Run.Name + " invalid model id " + strconv.Itoa(meta.Run.ModelId) + " expected: " + strconv.Itoa(modelDef.Model.ModelId))
	}
	if !IsRunCompleted(meta.Run.Status) {
		return false, errors.New("model run not completed: " + strconv.Itoa(meta.Run.RunId) + " " + meta.Run.Name)
	}

	// find existing run:
	// if digest not "" empty then by run digest
	// else if status is error then by run_name, sub_count, sub_completed, status, create_dt
	var dstId int

	if meta.Run.Digest != "" || (meta.Run.Status == ErrorRunStatus && meta.Run.Name != "" && meta.Run.CreateDateTime != "") {

		q := "SELECT MIN(R.run_id)" +
			" FROM run_lst R" +
			" WHERE R.model_id = " + strconv.Itoa(modelDef.Model.ModelId)

		if meta.Run.Digest != "" {
			q += " AND R.run_digest = " + toQuoted(meta.Run.Digest)
		} else {
			q += " AND R.run_name = " + toQuoted(meta.Run.Name) +
				" AND R.sub_count = " + strconv.Itoa(meta.Run.SubCount) +
				" AND R.sub_completed = " + strconv.Itoa(meta.Run.SubCompleted) +
				" AND R.status = " + toQuoted(meta.Run.Status) +
				" AND R.create_dt = " + toQuoted(meta.Run.CreateDateTime)
		}

		err := SelectFirst(dbConn,
			q,
			func(row *sql.Row) error {
				var rId sql.NullInt64
				if err := row.Scan(&rId); err != nil {
					return err
				}
				if rId.Valid {
					dstId = int(rId.Int64)
				}
				return nil
			})
		switch {
		case err == sql.ErrNoRows:
			dstId = 0 // model run not exist, select min() should always return run_id or null
		case err != nil:
			return false, err
		}
	}

	// if run id exist then update run id
	if dstId > 0 {
		meta.Run.RunId = dstId
		for k := range meta.Txt {
			meta.Txt[k].RunId = dstId
		}
		for k := range meta.Param {
			for j := range meta.Param[k].Txt {
				meta.Param[k].Txt[j].RunId = dstId
			}
		}
		return true, nil
	}
	// else: run not exist

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return false, err
	}
	err = doInsertRun(trx, modelDef, meta, langDef, doubleFmt)
	if err != nil {
		trx.Rollback()
		return false, err
	}
	trx.Commit()

	return false, nil
}

// UpdateRunDigest does recalculate and update run_lst table with new run digest and return it.
// If run not exist or status is not success or exit then digest is "" empty (not updated).
func UpdateRunDigest(dbConn *sql.DB, runId int) (string, error) {

	// validate parameters
	if runId <= 0 {
		return "", errors.New("invalid model run id: " + strconv.Itoa(runId))
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return "", err
	}
	sd, err := doUpdateRunDigest(trx, runId)
	if err != nil {
		trx.Rollback()
		return "", err
	}
	trx.Commit()

	return sd, nil
}

// doUpdateRunDigest recalculate and update run_lst table with new run digest and return it.
// It does update as part of transaction
// If run not exist or status is not success or exit then digest is "" empty (not updated).
// Run digest include run metadata, run parameters value digests and output tables value digests
func doUpdateRunDigest(trx *sql.Tx, runId int) (string, error) {

	// check if this run exists and status is success or exit
	srId := strconv.Itoa(runId)
	var mId int
	var runName string
	var runStatus string
	var subCount int
	var subCompleted int

	err := TrxSelectFirst(trx,
		"SELECT model_id, run_name, sub_count, sub_completed, status FROM run_lst WHERE run_id = "+srId,
		func(row *sql.Row) error {
			return row.Scan(&mId, &runName, &subCount, &subCompleted, &runStatus)
		})
	switch {
	case err == sql.ErrNoRows:
		return "", nil // run not exist
	case err != nil:
		return "", err
	}
	if runStatus != DoneRunStatus && runStatus != ExitRunStatus { // run status not success or exit
		return "", err
	}

	// digest header: run metadata
	hMd5 := md5.New()

	_, err = hMd5.Write([]byte(
		"run_name,sub_count,sub_completed,status\n" +
			runName + "," + strconv.Itoa(subCount) + "," + strconv.Itoa(subCompleted) + "," + runStatus + "\n"))
	if err != nil {
		return "", err
	}

	// append run parameters value digest header
	_, err = hMd5.Write([]byte("run_digest\n"))
	if err != nil {
		return "", err
	}

	// append run parameters values digest to run digest
	err = TrxSelectRows(trx,
		"SELECT M.model_parameter_id, R.run_digest"+
			" FROM run_parameter R"+
			" INNER JOIN model_parameter_dic M ON (M.parameter_hid = R.parameter_hid)"+
			" WHERE M.model_id = "+strconv.Itoa(mId)+
			" AND R.run_id = "+strconv.Itoa(runId)+
			" ORDER BY 1",
		func(rows *sql.Rows) error {

			var i int
			var sd sql.NullString

			err := rows.Scan(&i, &sd)
			if err != nil {
				return err
			}
			if sd.Valid {
				_, err = hMd5.Write([]byte(sd.String + "\n"))
			} else {
				_, err = hMd5.Write([]byte("\n"))
			}
			return err
		})
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}

	// if run completed succesfully then append output tables value digest
	if runStatus == DoneRunStatus {

		// append output tables value digest header
		_, err = hMd5.Write([]byte("run_digest\n"))
		if err != nil {
			return "", err
		}

		// append output tables values digest to run digest
		err = TrxSelectRows(trx,
			"SELECT M.model_table_id, R.run_digest"+
				" FROM run_table R"+
				" INNER JOIN model_table_dic M ON (M.table_hid = R.table_hid)"+
				" WHERE M.model_id = "+strconv.Itoa(mId)+
				" AND R.run_id = "+strconv.Itoa(runId)+
				" ORDER BY 1",
			func(rows *sql.Rows) error {

				var i int
				var sd sql.NullString

				err := rows.Scan(&i, &sd)
				if err != nil {
					return err
				}
				if sd.Valid {
					_, err = hMd5.Write([]byte(sd.String + "\n"))
				} else {
					_, err = hMd5.Write([]byte("\n"))
				}
				return err
			})
		if err != nil && err != sql.ErrNoRows {
			return "", err
		}
	}

	// update model run digest
	dg := fmt.Sprintf("%x", hMd5.Sum(nil))

	err = TrxUpdate(trx,
		"UPDATE run_lst SET run_digest = "+toQuoted(dg)+" WHERE run_id = "+srId)
	if err != nil {
		return "", err
	}

	return dg, nil
}

// doInsertRun insert new model run metadata in database.
// It does update as part of transaction.
// Run status must be completed (success, exit or error) otherwise error returned.
// Double format is used for progress value float conversion, if non-empty format supplied.
func doInsertRun(trx *sql.Tx, modelDef *ModelMeta, meta *RunMeta, langDef *LangMeta, doubleFmt string) error {

	// validate: run must be completed
	if !IsRunCompleted(meta.Run.Status) {
		return errors.New("model run not completed: " + strconv.Itoa(meta.Run.RunId) + " " + meta.Run.Name)
	}

	meta.Run.ModelId = modelDef.Model.ModelId // update model id

	// run name, create date-time, update date-time should not be empty
	if meta.Run.CreateDateTime == "" {
		meta.Run.CreateDateTime = helper.MakeDateTime(time.Now())
	}
	if meta.Run.UpdateDateTime == "" {
		meta.Run.UpdateDateTime = meta.Run.CreateDateTime
	}
	if meta.Run.Name == "" {
		meta.Run.Name = helper.ToAlphaNumeric(
			modelDef.Model.Name + "_" + meta.Run.CreateDateTime + "_" + strconv.Itoa(meta.Run.RunId))
	}

	// get new run id
	runId := 0

	err := TrxUpdate(trx, "UPDATE id_lst SET id_value = id_value + 1 WHERE id_key = 'run_id_set_id'")
	if err != nil {
		return err
	}
	err = TrxSelectFirst(trx,
		"SELECT id_value FROM id_lst WHERE id_key = 'run_id_set_id'",
		func(row *sql.Row) error {
			return row.Scan(&runId)
		})
	switch {
	case err == sql.ErrNoRows:
		return errors.New("invalid destination database, likely not an openM++ database")
	case err != nil:
		return err
	}
	meta.Run.RunId = runId
	srId := strconv.Itoa(runId)

	// INSERT INTO run_lst: treat empty run digest as NULL
	var sd string
	if meta.Run.Digest != "" {
		sd = meta.Run.Digest
	} else {
		sd = "NULL"
	}
	err = TrxUpdate(trx,
		"INSERT INTO run_lst"+
			" (run_id, model_id, run_name, sub_count, sub_started, sub_completed, sub_restart, create_dt, status, update_dt, run_digest)"+
			" VALUES ("+
			srId+", "+
			strconv.Itoa(modelDef.Model.ModelId)+", "+
			toQuoted(meta.Run.Name)+", "+
			strconv.Itoa(meta.Run.SubCount)+", "+
			strconv.Itoa(meta.Run.SubStarted)+", "+
			strconv.Itoa(meta.Run.SubCompleted)+", "+
			"0, "+
			toQuoted(meta.Run.CreateDateTime)+", "+
			toQuoted(meta.Run.Status)+", "+
			toQuoted(meta.Run.UpdateDateTime)+", "+
			toQuoted(sd)+")")
	if err != nil {
		return err
	}

	// update run text (description and notes)
	for j := range meta.Txt {

		meta.Txt[j].RunId = runId // update run id

		// if language code valid then insert into run_txt
		if lId, ok := langDef.IdByCode(meta.Txt[j].LangCode); ok {

			err = TrxUpdate(trx,
				"INSERT INTO run_txt (run_id, lang_id, descr, note) VALUES ("+
					srId+", "+
					strconv.Itoa(lId)+", "+
					toQuoted(meta.Txt[j].Descr)+", "+
					toQuotedOrNull(meta.Txt[j].Note)+")")
			if err != nil {
				return err
			}
		}
	}

	// update run options: options used to run the model
	for key, val := range meta.Opts {

		// insert into run_option
		err = TrxUpdate(trx,
			"INSERT INTO run_option (run_id, option_key, option_value) VALUES ("+
				srId+", "+
				toQuoted(key)+", "+
				toQuoted(val)+")")
		if err != nil {
			return err
		}
	}

	// update parameter run text: parameter run value notes
	for k := range meta.Param {
		for j := range meta.Param[k].Txt {

			meta.Param[k].Txt[j].RunId = runId // update run id

			// if language code valid then insert into run_parameter_txt
			if lId, ok := langDef.IdByCode(meta.Param[k].Txt[j].LangCode); ok {

				err = TrxUpdate(trx,
					"INSERT INTO run_parameter_txt (run_id, parameter_hid, lang_id, note) VALUES ("+
						srId+", "+
						strconv.Itoa(meta.Param[k].ParamHid)+", "+
						strconv.Itoa(lId)+", "+
						toQuotedOrNull(meta.Param[k].Txt[j].Note)+")")
				if err != nil {
					return err
				}
			}
		}
	}

	// update sub-values run progress
	for k := range meta.Progress {

		// convert progress value using double format, if non-empty
		sVal := ""
		if doubleFmt != "" {
			sVal = fmt.Sprintf(doubleFmt, meta.Progress[k].Value)
		} else {
			sVal = fmt.Sprint(meta.Progress[k].Value)
		}

		// insert into run_progress
		err = TrxUpdate(trx,
			"INSERT INTO run_progress"+
				" (run_id, sub_id, create_dt, status, update_dt, progress_count, progress_value)"+
				" VALUES ("+
				srId+", "+
				strconv.Itoa(meta.Progress[k].SubId)+", "+
				toQuoted(meta.Progress[k].CreateDateTime)+", "+
				toQuoted(meta.Progress[k].Status)+", "+
				toQuoted(meta.Progress[k].UpdateDateTime)+", "+
				strconv.Itoa(meta.Progress[k].Count)+", "+
				sVal+")")
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateRunText merge run text (run description and notes) and run parameter notes into run_txt and run_parameter_txt tables.
//
// New run text merged with existing db rows by update or insert if rows not exist db tables.
// If run not exist then function does nothing.
// Run status must be completed (success, exit or error) otherwise error returned.
// Run id of the input run metadata updated with runId value.
func (meta *RunMeta) UpdateRunText(dbConn *sql.DB, modelDef *ModelMeta, runId int, langDef *LangMeta) error {

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return errors.New("invalid (empty) language list")
	}
	if meta.Run.ModelId != modelDef.Model.ModelId {
		return errors.New("model run: " + strconv.Itoa(runId) + " " + meta.Run.Name + " invalid model id " + strconv.Itoa(meta.Run.ModelId) + " expected: " + strconv.Itoa(modelDef.Model.ModelId))
	}

	// check run status: if not completed then exit
	var st string

	err := SelectFirst(dbConn,
		"SELECT status FROM run_lst WHERE run_id = "+strconv.Itoa(runId),
		func(row *sql.Row) error {
			err := row.Scan(&st)
			return err
		})
	switch {
	case err == sql.ErrNoRows:
		return nil // model run not found: nothing to do
	case err != nil:
		return err
	}

	// if run run not completed then exit
	if !IsRunCompleted(st) {
		return errors.New("model run not completed: " + strconv.Itoa(runId) + " " + meta.Run.Name)
	}
	meta.Run.RunId = runId // run id exist: update run id in run metadata

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	err = doMergeRunText(trx, runId, meta, langDef)
	if err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()

	return nil
}

// doMergeRunText merge run text and run parameter notes into run_txt and run_parameter_txt tables by run_id.
// It does update as part of transaction.
// Run id of the input runTxt and paramTxt db rows updated with runId value.
func doMergeRunText(trx *sql.Tx, runId int, meta *RunMeta, langDef *LangMeta) error {

	// update existing or insert new run_txt db rows
	srId := strconv.Itoa(runId)
	meta.Run.RunId = runId

	for k := range meta.Txt {

		meta.Txt[k].RunId = runId // update run id

		// if language code valid then insert new run_txt db rows
		if lId, ok := langDef.IdByCode(meta.Txt[k].LangCode); ok {

			err := TrxUpdate(trx,
				"DELETE FROM run_txt WHERE run_id = "+srId+" AND lang_id = "+strconv.Itoa(lId))
			if err != nil {
				return err
			}
			err = TrxUpdate(trx,
				"INSERT INTO run_txt (run_id, lang_id, descr, note) VALUES ("+
					srId+", "+
					strconv.Itoa(lId)+", "+
					toQuoted(meta.Txt[k].Descr)+", "+
					toQuotedOrNull(meta.Txt[k].Note)+")")
			if err != nil {
				return err
			}
		}
	}

	// update existing or insert new run_parameter_txt db rows
	for k := range meta.Param {
		for j := range meta.Param[k].Txt {

			meta.Param[k].Txt[j].RunId = runId // update run id

			// if language code valid then insert new run_txt db rows
			if lId, ok := langDef.IdByCode(meta.Param[k].Txt[j].LangCode); ok {

				err := TrxUpdate(trx,
					"DELETE FROM run_parameter_txt"+
						" WHERE run_id = "+srId+
						" AND parameter_hid = "+strconv.Itoa(meta.Param[k].Txt[j].ParamHid)+
						" AND lang_id = "+strconv.Itoa(lId))
				if err != nil {
					return err
				}
				err = TrxUpdate(trx,
					"INSERT INTO run_parameter_txt (run_id, parameter_hid, lang_id, note) VALUES ("+
						srId+", "+
						strconv.Itoa(meta.Param[k].Txt[j].ParamHid)+", "+
						strconv.Itoa(lId)+", "+
						toQuotedOrNull(meta.Param[k].Txt[j].Note)+")")
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
