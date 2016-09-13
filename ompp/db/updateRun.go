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

// UpdateRunList insert new model run metadata in database.
// Model id, run id, parameter Hid id updated with actual database id's.
func UpdateRunList(
	dbConn *sql.DB, modelDef *ModelMeta, langDef *LangList, rl *RunList) (map[int]int, error) {

	// validate parameters
	if rl == nil {
		return make(map[int]int), nil // source is empty: nothing to do, exit
	}
	if len(rl.Lst) <= 0 {
		return make(map[int]int), nil // source is empty: nothing to do, exit
	}
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return nil, errors.New("invalid (empty) language list")
	}
	if rl.ModelName != modelDef.Model.Name || rl.ModelDigest != modelDef.Model.Digest {
		return nil, errors.New("invalid model name " + rl.ModelName + " or digest " + rl.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return nil, err
	}
	runIdMap, err := doUpdateRunList(trx, modelDef, langDef, rl.Lst)
	if err != nil {
		trx.Rollback()
		return nil, err
	}
	trx.Commit()

	return runIdMap, nil
}

// doUpdateRunList insert new model run metadata in database.
// It does update as part of transaction
// Model id, run id, parameter Hid id updated with actual database id's.
func doUpdateRunList(
	trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, runLst []RunMeta) (map[int]int, error) {

	smId := strconv.Itoa(modelDef.Model.ModelId)
	runIdMap := make(map[int]int, len(runLst))

	for idx := range runLst {

		runLst[idx].Run.ModelId = modelDef.Model.ModelId // update model id

		// get new run id
		runId := 0

		err := TrxUpdate(trx, "UPDATE id_lst SET id_value = id_value + 1 WHERE id_key = 'run_id_set_id'")
		if err != nil {
			return nil, err
		}
		err = TrxSelectFirst(trx,
			"SELECT id_value FROM id_lst WHERE id_key = 'run_id_set_id'",
			func(row *sql.Row) error {
				return row.Scan(&runId)
			})
		switch {
		case err == sql.ErrNoRows:
			return nil, errors.New("invalid destination database, likely not an openM++ database")
		case err != nil:
			return nil, err
		}
		runIdMap[runLst[idx].Run.RunId] = runId // save map between incoming and actual new run id
		runLst[idx].Run.RunId = runId
		srId := strconv.Itoa(runId)

		// update run name if empty
		if runLst[idx].Run.Name == "" {
			runLst[idx].Run.Name = helper.ToAlphaNumeric(modelDef.Model.Name + "_" + helper.MakeDateTime(time.Now()) + "_" + srId)
		}

		// INSERT INTO run_lst (task_id, model_id, task_name) VALUES (88, 11, 'modelOne task')
		err = TrxUpdate(trx,
			"INSERT INTO run_lst"+
				" (run_id, model_id, run_name, sub_count, sub_started, sub_completed, sub_restart, create_dt, status, update_dt)"+
				" VALUES ("+
				srId+", "+
				smId+", "+
				toQuoted(runLst[idx].Run.Name)+", "+
				strconv.Itoa(runLst[idx].Run.SubCount)+", "+
				strconv.Itoa(runLst[idx].Run.SubStarted)+", "+
				strconv.Itoa(runLst[idx].Run.SubCompleted)+", "+
				"0, "+
				toQuoted(runLst[idx].Run.CreateDateTime)+", "+
				toQuoted(runLst[idx].Run.Status)+", "+
				toQuoted(runLst[idx].Run.UpdateDateTime)+")")
		if err != nil {
			return nil, err
		}

		// update run text (description and notes)
		for j := range runLst[idx].Txt {

			// update run id and language id
			runLst[idx].Txt[j].RunId = runId

			k, ok := langDef.codeIndex[runLst[idx].Txt[j].LangCode]
			if !ok {
				return nil, errors.New("invalid language code " + runLst[idx].Txt[j].LangCode)
			}
			runLst[idx].Txt[j].LangId = langDef.LangWord[k].LangId

			// insert into run_txt
			err = TrxUpdate(trx,
				"INSERT INTO run_txt (run_id, lang_id, descr, note) VALUES ("+
					srId+", "+
					strconv.Itoa(runLst[idx].Txt[j].LangId)+", "+
					toQuoted(runLst[idx].Txt[j].Descr)+", "+
					toQuotedOrNull(runLst[idx].Txt[j].Note)+")")
			if err != nil {
				return nil, err
			}
		}

		// update run options: options used to run the model
		for key, val := range runLst[idx].Opts {

			// insert into run_option
			err = TrxUpdate(trx,
				"INSERT INTO run_option (run_id, option_key, option_value) VALUES ("+
					srId+", "+
					toQuoted(key)+", "+
					toQuoted(val)+")")
			if err != nil {
				return nil, err
			}
		}

		// update parameter run text: parameter run value notes
		for j := range runLst[idx].ParamTxt {

			// update run id, language id and parameter Hid
			runLst[idx].ParamTxt[j].RunId = runId

			k, ok := langDef.codeIndex[runLst[idx].ParamTxt[j].LangCode]
			if !ok {
				return nil, errors.New("invalid language code " + runLst[idx].ParamTxt[j].LangCode)
			}
			runLst[idx].ParamTxt[j].LangId = langDef.LangWord[k].LangId

			hId := modelDef.ParamHidById(runLst[idx].ParamTxt[j].ParamId)
			if hId <= 0 {
				return nil, errors.New("invalid parameter id: " + strconv.Itoa(runLst[idx].ParamTxt[j].ParamId))
			}

			// insert into run_parameter_txt
			err = TrxUpdate(trx,
				"INSERT INTO run_parameter_txt (run_id, parameter_hid, lang_id, note) VALUES ("+
					srId+", "+
					strconv.Itoa(hId)+", "+
					strconv.Itoa(runLst[idx].ParamTxt[j].LangId)+", "+
					toQuotedOrNull(runLst[idx].ParamTxt[j].Note)+")")
			if err != nil {
				return nil, err
			}
		}
	}

	return runIdMap, nil
}
