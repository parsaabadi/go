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

// FromPublic convert model run metadata from "public" format (coming from json import-export) into db rows.
func (pub *RunPub) FromPublic(dbConn *sql.DB, modelDef *ModelMeta, langDef *LangMeta) (*RunMeta, error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return nil, errors.New("invalid (empty) language list")
	}
	if pub.ModelName == "" && pub.ModelDigest == "" {
		return nil, errors.New("invalid (empty) model name and digest, model run: " + pub.Name + " " + pub.CreateDateTime)
	}

	// validate run model name and/or digest: run must belong to the model
	if (pub.ModelName != "" && pub.ModelName != modelDef.Model.Name) ||
		(pub.ModelDigest != "" && pub.ModelDigest != modelDef.Model.Digest) {
		return nil, errors.New("invalid model name " + pub.ModelName + " or digest " + pub.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// run header: run_lst row with zero default run id
	meta := RunMeta{
		Run: RunRow{
			RunId:          0, // run id is undefined
			ModelId:        modelDef.Model.ModelId,
			Name:           pub.Name,
			SubCount:       pub.SubCount,
			SubStarted:     pub.SubStarted,
			SubCompleted:   pub.SubCompleted,
			CreateDateTime: pub.CreateDateTime,
			Status:         pub.Status,
			UpdateDateTime: pub.UpdateDateTime,
			Digest:         pub.Digest,
		},
		Txt:      make([]RunTxtRow, len(pub.Txt)),
		Opts:     make(map[string]string, len(pub.Opts)),
		ParamTxt: make([]runParam, len(pub.ParamTxt)),
	}

	// model run description and notes: run_txt rows
	// use run id default zero
	for k := range pub.Txt {
		meta.Txt[k].LangCode = pub.Txt[k].LangCode
		meta.Txt[k].LangId = langDef.IdByCode(pub.Txt[k].LangCode)
		meta.Txt[k].Descr = pub.Txt[k].Descr
		meta.Txt[k].Note = pub.Txt[k].Note
	}

	// run options
	for k, v := range pub.Opts {
		meta.Opts[k] = v
	}

	// run parameters value notes: run_parameter_txt rows
	// use set id default zero
	for k := range pub.ParamTxt {

		// find model parameter index by name
		idx, ok := modelDef.ParamByName(pub.ParamTxt[k].Name)
		if !ok {
			return nil, errors.New("model run: " + pub.Name + " parameter " + pub.ParamTxt[k].Name + " not found")
		}
		meta.ParamTxt[k].ParamHid = modelDef.Param[idx].ParamHid

		// workset parameter value notes, use set id default zero
		if len(pub.ParamTxt[k].Txt) > 0 {
			meta.ParamTxt[k].Txt = make([]RunParamTxtRow, len(pub.ParamTxt[k].Txt))

			for j := range pub.ParamTxt[k].Txt {
				meta.ParamTxt[k].Txt[j].ParamHid = meta.ParamTxt[k].ParamHid
				meta.ParamTxt[k].Txt[j].LangCode = pub.ParamTxt[k].Txt[j].LangCode
				meta.ParamTxt[k].Txt[j].LangId = langDef.IdByCode(pub.ParamTxt[k].Txt[j].LangCode)
				meta.ParamTxt[k].Txt[j].Note = pub.ParamTxt[k].Txt[j].Note
			}
		}
	}

	return &meta, nil
}

// UpdateRun insert new or return existing model run metadata in database.
//
// Run status must be completed (success, exit or error) otherwise error returned.
// If this run already exist then nothing is updated in database, only metadate updated with actual run id.
// Following is used to find existing model run:
// if digest not "" empty then by run digest;
// else if status is error then by run_name, sub_count, sub_completed, status, create_dt.
//
// It return "is found" flag and update metadata with actual run id in database.
func (meta *RunMeta) UpdateRun(dbConn *sql.DB, modelDef *ModelMeta) (bool, error) {

	// validate parameters
	if modelDef == nil {
		return false, errors.New("invalid (empty) model metadata")
	}
	if meta.Run.ModelId != modelDef.Model.ModelId {
		return false, errors.New("model run: " + strconv.Itoa(meta.Run.RunId) + " " + meta.Run.Name + " invalid model id " + strconv.Itoa(meta.Run.ModelId) + " expected: " + strconv.Itoa(modelDef.Model.ModelId))
	}
	if meta.Run.Status != DoneRunStatus && meta.Run.Status != ExitRunStatus && meta.Run.Status != ErrorRunStatus {
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
		for k := range meta.ParamTxt {
			for j := range meta.ParamTxt[k].Txt {
				meta.ParamTxt[k].Txt[j].RunId = dstId
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
	err = doInsertRun(trx, modelDef, meta)
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

// UpdateRunText update run_txt table of existing run_id.
//
// Run id of the input txt db rows updated with runId value.
// If run not exist or status is not completed (success, exit, error) then function does nothing.
func UpdateRunText(dbConn *sql.DB, runId int, txt []RunTxtRow) error {

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
	if st != DoneRunStatus && st != ExitRunStatus && st != ErrorRunStatus {
		return nil
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	err = doUpdateRunText(trx, runId, txt)
	if err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()

	return nil
}

// doUpdateRunText update run_txt table of existing run_id.
// It does update as part of transaction.
// Run id of the input txt db rows updated with runId value.
func doUpdateRunText(trx *sql.Tx, runId int, txt []RunTxtRow) error {

	// delete existing run text
	srId := strconv.Itoa(runId)

	err := TrxUpdate(trx, "DELETE FROM run_txt WHERE run_id = "+srId)
	if err != nil {
		return err
	}

	// insert new run_txt db rows
	for k := range txt {

		txt[k].RunId = runId // update run id

		err = TrxUpdate(trx,
			"INSERT INTO run_txt (run_id, lang_id, descr, note) VALUES ("+
				srId+", "+
				strconv.Itoa(txt[k].LangId)+", "+
				toQuoted(txt[k].Descr)+", "+
				toQuotedOrNull(txt[k].Note)+")")
		if err != nil {
			return err
		}
	}
	return nil
}

// doInsertRun insert new model run metadata in database.
// It does update as part of transaction
// Run status must be completed (success, exit or error) otherwise error returned.
func doInsertRun(trx *sql.Tx, modelDef *ModelMeta, meta *RunMeta) error {

	// validate: run must be completed
	if meta.Run.Status != DoneRunStatus && meta.Run.Status != ExitRunStatus && meta.Run.Status != ErrorRunStatus {
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

		// insert into run_txt
		err = TrxUpdate(trx,
			"INSERT INTO run_txt (run_id, lang_id, descr, note) VALUES ("+
				srId+", "+
				strconv.Itoa(meta.Txt[j].LangId)+", "+
				toQuoted(meta.Txt[j].Descr)+", "+
				toQuotedOrNull(meta.Txt[j].Note)+")")
		if err != nil {
			return err
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
	for k := range meta.ParamTxt {
		for j := range meta.ParamTxt[k].Txt {

			meta.ParamTxt[k].Txt[j].RunId = runId // update run id

			// insert into run_parameter_txt
			err = TrxUpdate(trx,
				"INSERT INTO run_parameter_txt (run_id, parameter_hid, lang_id, note) VALUES ("+
					srId+", "+
					strconv.Itoa(meta.ParamTxt[k].ParamHid)+", "+
					strconv.Itoa(meta.ParamTxt[k].Txt[j].LangId)+", "+
					toQuotedOrNull(meta.ParamTxt[k].Txt[j].Note)+")")
			if err != nil {
				return err
			}
		}
	}

	return nil
}
