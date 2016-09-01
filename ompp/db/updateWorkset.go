// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// UpdateWorksetList insert new or update existing workset metadata in database.
// Model id, set id, parameter Hid, base run id updated with actual database id's.
func UpdateWorksetList(
	dbConn *sql.DB, modelDef *ModelMeta, langDef *LangList, runIdMap map[int]int, wl *WorksetList) (map[int]int, error) {

	// validate parameters
	if wl == nil {
		return make(map[int]int), nil // source is empty: nothing to do, exit
	}
	if len(wl.Lst) <= 0 {
		return make(map[int]int), nil // source is empty: nothing to do, exit
	}
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return nil, errors.New("invalid (empty) language list")
	}
	if wl.ModelName != modelDef.Model.Name || wl.ModelDigest != modelDef.Model.Digest {
		return nil, errors.New("invalid model name " + wl.ModelName + " or digest " + wl.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return nil, err
	}
	setIdMap, err := doUpdateWorksetList(trx, modelDef, langDef, runIdMap, wl.Lst)
	if err != nil {
		trx.Rollback()
		return nil, err
	}
	trx.Commit()

	return setIdMap, nil
}

// doUpdateWorksetList insert new or update existing workset metadata in database.
// It does update as part of transaction
// Model id, set id, parameter Hid, base run id updated with actual database id's.
func doUpdateWorksetList(
	trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, runIdMap map[int]int, setLst []WorksetMeta) (map[int]int, error) {

	smId := strconv.Itoa(modelDef.Model.ModelId)
	setIdMap := make(map[int]int, len(setLst))

	for idx := range setLst {

		// workset name cannot be empty
		if setLst[idx].Set.Name == "" {
			return nil, errors.New("invalid (empty) workset name, id: " + strconv.Itoa(setLst[idx].Set.SetId))
		}
		setLst[idx].Set.ModelId = modelDef.Model.ModelId // update model id

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
				" WHERE model_id = "+smId+" AND set_name = "+toQuoted(setLst[idx].Set.Name)+
				" )"+
				" THEN id_value + 1"+
				" ELSE id_value"+
				" END"+
				" WHERE id_key = 'run_id_set_id'")
		if err != nil {
			return nil, err
		}

		// check if this workset already exist
		setId := 0
		err = TrxSelectFirst(trx,
			"SELECT set_id FROM workset_lst WHERE model_id = "+smId+" AND set_name = "+toQuoted(setLst[idx].Set.Name),
			func(row *sql.Row) error {
				return row.Scan(&setId)
			})
		if err != nil && err != sql.ErrNoRows {
			return nil, err
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
				return nil, errors.New("invalid destination database, likely not an openM++ database")
			case err != nil:
				return nil, err
			}

			setIdMap[setLst[idx].Set.SetId] = setId // save map between incoming and actual new set id
			setLst[idx].Set.SetId = setId

			newRunId, ok := runIdMap[setLst[idx].Set.BaseRunId] // find new value of base run id
			if ok && newRunId > 0 {
				setLst[idx].Set.BaseRunId = newRunId
			} else {
				setLst[idx].Set.BaseRunId = 0 // no base run id (it should be found if exist)
			}

			// insert new workset
			err = doInsertWorkset(trx, modelDef, langDef, &setLst[idx])
			if err != nil {
				return nil, err
			}

		} else { // update existing workset

			setIdMap[setLst[idx].Set.SetId] = setId // workset already exist
			setLst[idx].Set.SetId = setId

			err = doUpdateWorkset(trx, modelDef, langDef, &setLst[idx])
			if err != nil {
				return nil, err
			}
		}
	}

	return setIdMap, nil
}

// doInsertWorkset insert new workset metadata in database.
// It does update as part of transaction
// Model id, parameter Hid, base run id updated with actual database id's.
func doInsertWorkset(trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, meta *WorksetMeta) error {

	// if workset based on existing run then base run id must be positive
	sbId := ""
	if meta.Set.BaseRunId > 0 {
		sbId = strconv.Itoa(meta.Set.BaseRunId)
	} else {
		sbId = "NULL"
	}
	sId := strconv.Itoa(meta.Set.SetId)

	// INSERT INTO workset_lst (set_id, base_run_id, model_id, set_name, is_readonly, update_dt)
	// VALUES (22, NULL, 1, 'set 22', 0, '2012-08-17 16:05:59.0123')
	err := TrxUpdate(trx,
		"INSERT INTO workset_lst (set_id, base_run_id, model_id, set_name, is_readonly, update_dt)"+
			" VALUES ("+
			sId+", "+
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
	return nil
}

// doUpdateWorkset update workset metadata in database.
// It does update as part of transaction
// Model id, parameter Hid, base run id updated with actual database id's.
func doUpdateWorkset(trx *sql.Tx, modelDef *ModelMeta, langDef *LangList, meta *WorksetMeta) error {

	sId := strconv.Itoa(meta.Set.SetId)

	// UPDATE workset_lst
	// SET is_readonly = 0, update_dt = '2012-08-17 16:05:59.0123'
	// WHERE set_id = 22
	err := TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = "+toBoolStr(meta.Set.IsReadonly)+", update_dt = "+toQuoted(meta.Set.Name)+
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
