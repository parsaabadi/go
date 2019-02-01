// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"go.openmpp.org/ompp/helper"
)

// UpdateWorksetParameter add new or replace existing workset parameter.
//
// If parameter already exist in workset then parameter metadata either merged or replaced.
// If parameter not exist in workset then parameter values must be supplied, it cannot be empty.
// If parameter not exist in workset then new parameter metadata and values inserted.
// If list of new parameter values supplied then parameter values completely replaced.
//
// Set name is used to find workset and set id updated with actual database value.
// Workset must be read-write for replace or merge.
func (meta *WorksetMeta) UpdateWorksetParameter(
	dbConn *sql.DB, modelDef *ModelMeta, isReplaceMeta bool, param *ParamRunSetPub, cellLst *list.List, langDef *LangMeta,
) (int, error) {

	// validate parameters
	if modelDef == nil {
		return 0, errors.New("invalid (empty) model metadata")
	}
	if param == nil {
		return 0, errors.New("invalid (empty) parameter metadata")
	}
	if langDef == nil {
		return 0, errors.New("invalid (empty) language list")
	}
	if meta.Set.Name == "" {
		return 0, errors.New("invalid (empty) workset name")
	}
	if meta.Set.ModelId != modelDef.Model.ModelId {
		return 0, errors.New("workset: " + meta.Set.Name + " invalid model id " + strconv.Itoa(meta.Set.ModelId) + " expected: " + strconv.Itoa(modelDef.Model.ModelId))
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return 0, err
	}

	// if parameter values supplied then sub-value count must be positive
	isData := cellLst != nil && cellLst.Len() > 0
	if isData && param.SubCount <= 0 {
		return 0, errors.New("parameter sub-value count must be positive: " + strconv.Itoa(param.SubCount) + ": " + param.Name)
	}

	// create, replace or merge workset metadata
	paramHid, err := doUpdateWorksetParameterMeta(trx, modelDef, meta, isReplaceMeta, param, isData, langDef)
	if err != nil {
		trx.Rollback()
		return 0, err
	}

	// if new parameter values supplied then insert or update parameter values in workset
	if paramHid > 0 && isData {

		var pm *ParamMeta
		if k, ok := modelDef.ParamByName(param.Name); ok {
			pm = &modelDef.Param[k]
		} else {
			trx.Rollback()
			return 0, errors.New("parameter not found: " + param.Name)
		}

		err = doWriteSetParameter(trx, pm, meta.Set.SetId, param.SubCount, false, cellLst)
		if err != nil {
			trx.Rollback()
			return 0, err
		}
	}

	trx.Commit()

	return paramHid, nil
}

// doUpdateWorksetParameter insert new or update existing workset parameter metadata.
// It does update as part of transaction.
//
// If parameter already exist in workset then parameter metadata either merged or replaced.
// If parameter not exist in workset then parameter values must be supplied, it cannobe empty.
// If parameter not exist in workset then new parameter metadata and values inserted.
// If list of new parameter values supplied then parameter values completely replaced.
//
// Set name is used to find workset and set id updated with actual database value.
// Workset must be read-write for replace or merge.
func doUpdateWorksetParameterMeta(
	trx *sql.Tx, modelDef *ModelMeta, wm *WorksetMeta, isReplaceMeta bool, param *ParamRunSetPub, isData bool, langDef *LangMeta,
) (int, error) {

	// find model parameter hId by name
	i, ok := modelDef.ParamByName(param.Name)
	if !ok {
		return 0, errors.New("model: " + modelDef.Model.Name + " parameter " + param.Name + " not found")
	}
	paramHid := modelDef.Param[i].ParamHid
	spHid := strconv.Itoa(paramHid)

	// "lock" workset to prevent update or use by the model
	wm.Set.ModelId = modelDef.Model.ModelId // update model id

	err := TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = is_readonly + 1"+
			" WHERE model_id = "+strconv.Itoa(modelDef.Model.ModelId)+" AND set_name = "+toQuoted(wm.Set.Name))
	if err != nil {
		return 0, err
	}

	// check if workset exist and not readonly
	var setId, nRd int
	err = TrxSelectFirst(trx,
		"SELECT set_id, is_readonly FROM workset_lst"+
			" WHERE model_id = "+strconv.Itoa(modelDef.Model.ModelId)+" AND set_name = "+toQuoted(wm.Set.Name),
		func(row *sql.Row) error {
			if err := row.Scan(&setId, &nRd); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return 0, errors.New("failed to update: workset not found: " + wm.Set.Name)
	case err != nil:
		return 0, err
	case nRd != 1:
		return 0, errors.New("failed to update: workset is read-only: " + wm.Set.Name)
	}
	wm.Set.SetId = setId // workset exist, id may be different

	// check if parameter exist in workset_parameter
	sId := strconv.Itoa(wm.Set.SetId)

	if isData {
		err = TrxUpdate(trx, "UPDATE workset_parameter"+
			" SET sub_count = "+strconv.Itoa(param.SubCount)+
			" WHERE set_id = "+sId+" AND parameter_hid = "+spHid)
	} else {
		err = TrxUpdate(trx, "UPDATE workset_parameter"+
			" SET parameter_hid = "+spHid+
			" WHERE set_id = "+sId+" AND parameter_hid = "+spHid)
	}
	if err != nil {
		return 0, err
	}
	err = TrxSelectFirst(trx,
		"SELECT parameter_hid FROM workset_parameter WHERE set_id = "+sId+" AND parameter_hid = "+spHid,
		func(row *sql.Row) error {
			h := 0
			if err := row.Scan(&h); err != nil {
				return err
			}
			return nil

		})
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	isParamExist := err == nil // not sql.ErrNoRows

	// if no data then parameter must exist in workset
	if !isData && !isParamExist {
		return 0, errors.New("parameter: " + param.Name + " must exist in workset: " + wm.Set.Name)
	}

	// if this is merge then insert ot update workset_parameter
	// if this is replace then delete existing workset_parameter_txt and delete-insert workset_parameter
	if !isReplaceMeta {

		// parameter not exist in workset: do insert
		if !isParamExist {
			err = TrxUpdate(trx,
				"INSERT INTO workset_parameter (set_id, parameter_hid, sub_count) VALUES ("+
					sId+", "+spHid+", "+strconv.Itoa(param.SubCount)+")")
			if err != nil {
				return 0, err
			}
		}

	} else { // repalce existing parameter metadata: delete text and delete-insert workset_parameter

		err = TrxUpdate(trx, "DELETE FROM workset_parameter_txt WHERE set_id = "+sId+" AND parameter_hid = "+spHid)
		if err != nil {
			return 0, err
		}
		err = TrxUpdate(trx, "DELETE FROM workset_parameter WHERE set_id = "+sId+" AND parameter_hid = "+spHid)
		if err != nil {
			return 0, err
		}
		err = TrxUpdate(trx,
			"INSERT INTO workset_parameter (set_id, parameter_hid, sub_count) VALUES ("+
				sId+", "+spHid+", "+strconv.Itoa(param.SubCount)+")")
		if err != nil {
			return 0, err
		}
	}

	// delete and insert into workset parameter text: parameter value notes
	for j := range param.Txt {

		if lId, ok := langDef.IdByCode(param.Txt[j].LangCode); ok { // if language code valid

			slId := strconv.Itoa(lId)

			if !isReplaceMeta {
				err = TrxUpdate(trx,
					"DELETE FROM workset_parameter_txt"+
						" WHERE set_id = "+sId+
						" AND parameter_hid = "+spHid+
						" AND lang_id = "+slId)
				if err != nil {
					return 0, err
				}
			}
			err = TrxUpdate(trx,
				"INSERT INTO workset_parameter_txt (set_id, parameter_hid, lang_id, note)"+
					" SELECT "+
					sId+", "+" parameter_hid, "+slId+", "+toQuotedOrNullMax(param.Txt[j].Note, noteDbMax)+
					" FROM workset_parameter"+
					" WHERE set_id = "+sId+
					" AND parameter_hid = "+spHid)
			if err != nil {
				return 0, err
			}
		}
	}

	// "unlock" workset for parameter values write: restore original value of is_readonly=0
	wm.Set.UpdateDateTime = helper.MakeDateTime(time.Now())
	err = TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = 0,"+
			" update_dt = "+toQuoted(wm.Set.UpdateDateTime)+
			" WHERE set_id = "+strconv.Itoa(setId))
	if err != nil {
		return 0, err
	}

	return paramHid, nil
}
