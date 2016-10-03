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

// UpdateWorksetReadonly update workset readonly status.
func UpdateWorksetReadonly(dbConn *sql.DB, setId int, isReadonly bool) error {

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	err = TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = "+toBoolStr(isReadonly)+", "+" update_dt = "+toQuoted(helper.MakeDateTime(time.Now()))+
			" WHERE set_id ="+strconv.Itoa(setId))
	if err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()

	return nil
}

// UpdateWorkset insert new or update existing workset metadata in database.
//
// Model id, set id, parameter Hid, base run id updated with actual database id's.
func UpdateWorkset(dbConn *sql.DB, modelDef *ModelMeta, langDef *LangList, meta *WorksetMeta) error {

	// validate parameters
	if meta == nil {
		return nil // source is empty: nothing to do, exit
	}
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return errors.New("invalid (empty) language list")
	}
	if meta.ModelName != modelDef.Model.Name || meta.ModelDigest != modelDef.Model.Digest {
		return errors.New("invalid workset model name " + meta.ModelName + " or digest " + meta.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	err = doUpdateOrInsertWorkset(trx, modelDef, langDef, meta)
	if err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()

	return nil
}

// doUpdateOrInsertWorkset insert new or update existing workset metadata in database.
// It does update as part of transaction
// Model id, set id, parameter Hid, base run id updated with actual database id's.
func doUpdateOrInsertWorkset(trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, meta *WorksetMeta) error {

	smId := strconv.Itoa(modelDef.Model.ModelId)

	// workset name cannot be empty
	if meta.Set.Name == "" {
		return errors.New("invalid (empty) workset name, id: " + strconv.Itoa(meta.Set.SetId))
	}
	meta.Set.ModelId = modelDef.Model.ModelId // update model id

	// get new set id if workset not exist
	//
	// UPDATE id_lst SET id_value =
	//   CASE
	//     WHEN 0 = (SELECT COUNT(*) FROM workset_lst WHERE model_id = 1 AND set_name = 'set 22')
	//       THEN id_value + 1
	//     ELSE id_value
	//   END
	// WHERE id_key = 'run_id_set_id'
	err := TrxUpdate(trx,
		"UPDATE id_lst SET id_value ="+
			" CASE"+
			" WHEN 0 ="+
			" (SELECT COUNT(*) FROM workset_lst"+
			" WHERE model_id = "+smId+" AND set_name = "+toQuoted(meta.Set.Name)+
			" )"+
			" THEN id_value + 1"+
			" ELSE id_value"+
			" END"+
			" WHERE id_key = 'run_id_set_id'")
	if err != nil {
		return err
	}

	// check if this workset already exist
	setId := 0
	err = TrxSelectFirst(trx,
		"SELECT set_id FROM workset_lst WHERE model_id = "+smId+" AND set_name = "+toQuoted(meta.Set.Name),
		func(row *sql.Row) error {
			return row.Scan(&setId)
		})
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// if workset not exist then insert new else update existing
	if setId <= 0 {

		// get new set id
		err = TrxSelectFirst(trx,
			"SELECT id_value FROM id_lst WHERE id_key = 'run_id_set_id'",
			func(row *sql.Row) error {
				return row.Scan(&setId)
			})
		switch {
		case err == sql.ErrNoRows:
			return errors.New("invalid destination database, likely not an openM++ database")
		case err != nil:
			return err
		}

		meta.Set.SetId = setId // update set id with actual value

		// insert new workset
		err = doInsertWorkset(trx, modelDef, langDef, meta)
		if err != nil {
			return err
		}

	} else { // update existing workset

		meta.Set.SetId = setId // workset exist, id may be different

		err = doUpdateWorkset(trx, modelDef, langDef, meta)
		if err != nil {
			return err
		}
	}

	return nil
}

// doInsertWorkset insert new workset metadata in database.
// It does update as part of transaction
// New workset created as read-only.
// Model id, parameter Hid, base run id updated with actual database id's.
func doInsertWorkset(trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, meta *WorksetMeta) error {

	// if workset based on existing run then base run id must be positive
	sbId := ""
	if meta.Set.BaseRunId > 0 {
		sbId = strconv.Itoa(meta.Set.BaseRunId)
	} else {
		sbId = "NULL"
	}

	// new workset created as read-only
	meta.Set.IsReadonly = true

	// set update time if not defined
	if meta.Set.UpdateDateTime == "" {
		meta.Set.UpdateDateTime = helper.MakeDateTime(time.Now())
	}

	// INSERT INTO workset_lst (set_id, base_run_id, model_id, set_name, is_readonly, update_dt)
	// VALUES (22, NULL, 1, 'set 22', 0, '2012-08-17 16:05:59.0123')
	err := TrxUpdate(trx,
		"INSERT INTO workset_lst (set_id, base_run_id, model_id, set_name, is_readonly, update_dt)"+
			" VALUES ("+
			strconv.Itoa(meta.Set.SetId)+", "+
			sbId+", "+
			strconv.Itoa(modelDef.Model.ModelId)+", "+
			toQuoted(meta.Set.Name)+", "+
			toBoolStr(meta.Set.IsReadonly)+", "+
			toQuoted(meta.Set.UpdateDateTime)+")")
	if err != nil {
		return err
	}

	// insert new rows into workset body tables: workset_txt, workset_parameter, workset_parameter_txt
	if err = doInsertWorksetBody(trx, modelDef, langDef, meta); err != nil {
		return err
	}
	return err
}

// doUpdateWorkset update workset metadata in database.
// It does update as part of transaction
// Model id, parameter Hid, base run id updated with actual database id's.
func doUpdateWorkset(trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, meta *WorksetMeta) error {

	sId := strconv.Itoa(meta.Set.SetId)

	// UPDATE workset_lst
	// SET is_readonly = 0, update_dt = '2012-08-17 16:05:59.0123'
	// WHERE set_id = 22
	//
	if meta.Set.UpdateDateTime == "" {
		meta.Set.UpdateDateTime = helper.MakeDateTime(time.Now())
	}
	err := TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = "+toBoolStr(meta.Set.IsReadonly)+", "+" update_dt = "+toQuoted(meta.Set.UpdateDateTime)+
			" WHERE set_id ="+sId)
	if err != nil {
		return err
	}

	// delete existing workset_parameter_txt, workset_parameter, workset_txt
	err = TrxUpdate(trx, "DELETE FROM workset_parameter_txt WHERE set_id ="+sId)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx, "DELETE FROM workset_parameter WHERE set_id ="+sId)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx, "DELETE FROM workset_txt WHERE set_id ="+sId)
	if err != nil {
		return err
	}

	// insert new rows into workset body tables: workset_txt, workset_parameter, workset_parameter_txt
	if err = doInsertWorksetBody(trx, modelDef, langDef, meta); err != nil {
		return err
	}
	return nil
}

// doInsertWorksetBody insert into workset metadata tables: workset_txt, workset_parameter, workset_parameter_txt
// It does update as part of transaction
// Model id, parameter Hid, base run id updated with actual database id's.
func doInsertWorksetBody(trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, meta *WorksetMeta) error {

	sId := strconv.Itoa(meta.Set.SetId)

	// insert workset text (description and notes)
	for j := range meta.Txt {

		// update set id and language id
		meta.Txt[j].SetId = meta.Set.SetId

		k, ok := langDef.codeIndex[meta.Txt[j].LangCode]
		if !ok {
			return errors.New("invalid language code " + meta.Txt[j].LangCode)
		}
		meta.Txt[j].LangId = langDef.LangWord[k].LangId

		// insert into workset_txt
		err := TrxUpdate(trx,
			"INSERT INTO workset_txt (set_id, lang_id, descr, note) VALUES ("+
				sId+", "+
				strconv.Itoa(meta.Txt[j].LangId)+", "+
				toQuoted(meta.Txt[j].Descr)+", "+
				toQuotedOrNull(meta.Txt[j].Note)+")")
		if err != nil {
			return err
		}
	}

	// insert workset parameters list
	for j := range meta.Param {

		// update model id and parameter Hid
		meta.Param[j].ModelId = modelDef.Model.ModelId

		hId := modelDef.ParamHidById(meta.Param[j].ParamId)
		if hId <= 0 {
			return errors.New("invalid parameter id: " + strconv.Itoa(meta.Param[j].ParamId))
		}
		meta.Param[j].ParamHid = hId

		// insert into workset_parameter
		err := TrxUpdate(trx,
			"INSERT INTO workset_parameter (set_id, parameter_hid) VALUES ("+sId+", "+strconv.Itoa(hId)+")")
		if err != nil {
			return err
		}
	}

	// insert new workset parameter text: parameter value notes
	for j := range meta.ParamTxt {

		// update set id, language id and parameter Hid
		meta.ParamTxt[j].SetId = meta.Set.SetId

		k, ok := langDef.codeIndex[meta.ParamTxt[j].LangCode]
		if !ok {
			return errors.New("invalid language code " + meta.ParamTxt[j].LangCode)
		}
		meta.ParamTxt[j].LangId = langDef.LangWord[k].LangId

		hId := modelDef.ParamHidById(meta.ParamTxt[j].ParamId)
		if hId <= 0 {
			return errors.New("invalid parameter id: " + strconv.Itoa(meta.ParamTxt[j].ParamId))
		}

		// insert into workset_parameter_txt
		err := TrxUpdate(trx,
			"INSERT INTO workset_parameter_txt (set_id, parameter_hid, lang_id, note) VALUES ("+
				sId+", "+
				strconv.Itoa(hId)+", "+
				strconv.Itoa(meta.ParamTxt[j].LangId)+", "+
				toQuotedOrNull(meta.ParamTxt[j].Note)+")")
		if err != nil {
			return err
		}
	}

	return nil
}
