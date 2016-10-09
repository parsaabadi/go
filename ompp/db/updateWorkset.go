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

// FromPublic convert workset metadata from "public" format (coming from json import-export) into db rows.
func (pub *WorksetPub) FromPublic(dbConn *sql.DB, modelDef *ModelMeta, langDef *LangMeta) (*WorksetMeta, error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return nil, errors.New("invalid (empty) language list")
	}
	if pub.Name == "" {
		return nil, errors.New("invalid (empty) workset name")
	}
	if pub.ModelName == "" && pub.ModelDigest == "" {
		return nil, errors.New("invalid (empty) model name and digest, workset: " + pub.Name)
	}

	// validate workset model name and/or digest: workset must belong to the model
	if (pub.ModelName != "" && pub.ModelName != modelDef.Model.Name) ||
		(pub.ModelDigest != "" && pub.ModelDigest != modelDef.Model.Digest) {
		return nil, errors.New("invalid workset model name " + pub.ModelName + " or digest " + pub.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// workset header: workset_lst row with zero default set id
	ws := WorksetMeta{
		Set: WorksetRow{
			SetId:          0, // set id is undefined
			Name:           pub.Name,
			ModelId:        modelDef.Model.ModelId,
			IsReadonly:     pub.IsReadonly,
			UpdateDateTime: pub.UpdateDateTime,
		},
		Txt:   make([]WorksetTxtRow, len(pub.Txt)),
		Param: make([]WorksetParam, len(pub.Param)),
	}

	// if base run digest not "" empty then find base run for that workset
	if pub.BaseRunDigest != "" {
		runRow, err := GetRunByDigest(dbConn, pub.BaseRunDigest)
		if err != nil {
			return nil, err
		}
		if runRow != nil {
			ws.Set.BaseRunId = runRow.RunId //	base run found
		}
	}

	// workset description and notes: workset_txt rows
	// use set id default zero
	for k := range pub.Txt {
		ws.Txt[k].LangCode = pub.Txt[k].LangCode
		ws.Txt[k].LangId = langDef.IdByCode(pub.Txt[k].LangCode)
		ws.Txt[k].Descr = pub.Txt[k].Descr
		ws.Txt[k].Note = pub.Txt[k].Note
	}

	// workset parameters and parameter value notes: workset_parameter, workset_parameter_txt rows
	// use set id default zero
	for k := range pub.Param {

		// find model parameter index by name
		idx, ok := modelDef.ParamByName(pub.Param[k].Name)
		if !ok {
			return nil, errors.New("workset: " + pub.Name + " parameter " + pub.Param[k].Name + " not found")
		}
		ws.Param[k].ParamHid = modelDef.Param[idx].ParamHid

		// workset parameter value notes, use set id default zero
		if len(pub.Param[k].Txt) > 0 {
			ws.Param[k].Txt = make([]WorksetParamTxtRow, len(pub.Param[k].Txt))

			for j := range pub.Param[k].Txt {
				ws.Param[k].Txt[j].ParamHid = ws.Param[k].ParamHid
				ws.Param[k].Txt[j].LangCode = pub.Param[k].Txt[j].LangCode
				ws.Param[k].Txt[j].LangId = langDef.IdByCode(pub.Param[k].Txt[j].LangCode)
				ws.Param[k].Txt[j].Note = pub.Param[k].Txt[j].Note
			}
		}
	}

	return &ws, nil
}

// UpdateWorkset insert new or update existing workset metadata in database.
//
// Set name is used to find workset and set id updated with actual database value
func (meta *WorksetMeta) UpdateWorkset(dbConn *sql.DB, modelDef *ModelMeta) error {

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if meta.Set.Name == "" {
		return errors.New("invalid (empty) workset name")
	}
	if meta.Set.ModelId != modelDef.Model.ModelId {
		return errors.New("workset: " + meta.Set.Name + " invalid model id " + strconv.Itoa(meta.Set.ModelId) + " expected: " + strconv.Itoa(modelDef.Model.ModelId))
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	err = doUpdateOrInsertWorkset(trx, modelDef, meta)
	if err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()

	return nil
}

// doUpdateOrInsertWorkset insert new or update existing workset metadata in database.
// It does update as part of transaction
// Set name is used to find workset and set id updated with actual database value
func doUpdateOrInsertWorkset(trx *sql.Tx, modelDef *ModelMeta, meta *WorksetMeta) error {

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
		err = doInsertWorkset(trx, modelDef, meta)
		if err != nil {
			return err
		}

	} else { // update existing workset

		meta.Set.SetId = setId // workset exist, id may be different

		err = doUpdateWorkset(trx, modelDef, meta)
		if err != nil {
			return err
		}
	}

	return nil
}

// doInsertWorkset insert new workset metadata in database.
// It does update as part of transaction
// Set id updated with actual database id of that workset
func doInsertWorkset(trx *sql.Tx, modelDef *ModelMeta, meta *WorksetMeta) error {

	// if workset based on existing run then base run id must be positive
	sbId := ""
	if meta.Set.BaseRunId > 0 {
		sbId = strconv.Itoa(meta.Set.BaseRunId)
	} else {
		sbId = "NULL"
	}

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
	if err = doInsertWorksetBody(trx, modelDef, meta); err != nil {
		return err
	}
	return err
}

// doUpdateWorkset update workset metadata in database.
// It does update as part of transaction
// Set id updated with actual database id of that workset
// It does delete existing parameter values which are not in the list of workset parameters.
func doUpdateWorkset(trx *sql.Tx, modelDef *ModelMeta, meta *WorksetMeta) error {

	// if workset based on existing run then base run id must be positive
	sbId := ""
	if meta.Set.BaseRunId > 0 {
		sbId = strconv.Itoa(meta.Set.BaseRunId)
	} else {
		sbId = "NULL"
	}

	sId := strconv.Itoa(meta.Set.SetId)

	// UPDATE workset_lst
	// SET is_readonly = 0, base_run_id = 1234, update_dt = '2012-08-17 16:05:59.0123'
	// WHERE set_id = 22
	//
	if meta.Set.UpdateDateTime == "" {
		meta.Set.UpdateDateTime = helper.MakeDateTime(time.Now())
	}
	err := TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = "+toBoolStr(meta.Set.IsReadonly)+", "+
			" base_run_id = "+sbId+", "+
			" update_dt = "+toQuoted(meta.Set.UpdateDateTime)+
			" WHERE set_id ="+sId)
	if err != nil {
		return err
	}

	// get Hid's of current workset parameters list
	var hs []int
	err = TrxSelectRows(trx,
		"SELECT parameter_hid FROM workset_parameter WHERE set_id ="+sId,
		func(rows *sql.Rows) error {
			var i int
			err := rows.Scan(&i)
			if err != nil {
				return err
			}
			hs = append(hs, i)
			return err
		})
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// delete values for existing parameters, which are not in the new parameters list
hidLoop:
	for _, h := range hs {

		for j := range meta.Param {
			if meta.Param[j].ParamHid == h {
				continue hidLoop // id found in the new parameters list
			}
		}
		// parameter exist in current version workset and not in the new incoming parameters list

		if idx, ok := modelDef.ParamByHid(h); ok { // delete current values of parameter
			if err = TrxUpdate(trx,
				"DELETE FROM "+modelDef.Param[idx].DbSetTable+" WHERE set_id = "+sId); err != nil {
				return err
			}
		}
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
	if err = doInsertWorksetBody(trx, modelDef, meta); err != nil {
		return err
	}
	return nil
}

// doInsertWorksetBody insert into workset metadata tables: workset_txt, workset_parameter, workset_parameter_txt
// It does update as part of transaction
// Set id updated with actual database id of that workset
func doInsertWorksetBody(trx *sql.Tx, modelDef *ModelMeta, meta *WorksetMeta) error {

	sId := strconv.Itoa(meta.Set.SetId)

	// insert workset text (description and notes)
	for j := range meta.Txt {

		meta.Txt[j].SetId = meta.Set.SetId // update set id

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
	// insert new workset parameter text: parameter value notes
	for k := range meta.Param {

		// insert workset parameter
		err := TrxUpdate(trx,
			"INSERT INTO workset_parameter (set_id, parameter_hid) VALUES ("+sId+", "+strconv.Itoa(meta.Param[k].ParamHid)+")")
		if err != nil {
			return err
		}

		// insert new workset parameter text: parameter value notes
		for j := range meta.Param[k].Txt {

			meta.Param[k].Txt[j].SetId = meta.Set.SetId // update set id

			// insert into workset_parameter_txt
			err := TrxUpdate(trx,
				"INSERT INTO workset_parameter_txt (set_id, parameter_hid, lang_id, note) VALUES ("+
					sId+", "+
					strconv.Itoa(meta.Param[k].ParamHid)+", "+
					strconv.Itoa(meta.Param[k].Txt[j].LangId)+", "+
					toQuotedOrNull(meta.Param[k].Txt[j].Note)+")")
			if err != nil {
				return err
			}
		}
	}

	return nil
}
