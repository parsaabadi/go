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

// CopyParameterFromRun copy parameter metadata and parameter values into workset from model run.
//
// If parameter already exist in destination workset then error returned.
// Destination workset must be in read-write state.
// Source model run must be completed, run status one of: s=success, x=exit, e=error.
func CopyParameterFromRun(dbConn *sql.DB, modelDef *ModelMeta, wst *WorksetRow, paramName string, rst *RunRow) error {

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if wst == nil || wst.SetId <= 0 {
		return errors.New("invalid (empty) destination workset")
	}
	if paramName == "" {
		return errors.New("invalid (empty) parameter name")
	}
	if rst == nil || rst.RunId <= 0 {
		return errors.New("invalid (empty) model run")
	}
	if !IsRunCompleted(rst.Status) {
		return errors.New("error: model run is not completed: " + modelDef.Model.Name + ": " + rst.Name + ": " + rst.Status)
	}

	// find model parameter hId by name
	i, ok := modelDef.ParamByName(paramName)
	if !ok {
		return errors.New("model: " + modelDef.Model.Name + " parameter " + paramName + " not found")
	}
	pm := modelDef.Param[i]

	// copy parameter metadata and values from model run into workset inside of transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err = dbCopyParameterFromRun(trx, wst, &pm, rst); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// CopyParameterFromWorkset copy parameter metadata and parameter values from one workset to another.
//
// If parameter already exist in destination workset then error returned.
// Destination workset must be in read-write state, source workset must be read-only.
func CopyParameterFromWorkset(dbConn *sql.DB, modelDef *ModelMeta, dstWs *WorksetRow, paramName string, srcWs *WorksetRow) error {

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if srcWs == nil || srcWs.SetId <= 0 || srcWs.ModelId != modelDef.Model.ModelId {
		return errors.New("invalid (empty) source workset")
	}
	if paramName == "" {
		return errors.New("invalid (empty) parameter name")
	}
	if dstWs == nil || dstWs.SetId <= 0 || dstWs.ModelId != modelDef.Model.ModelId {
		return errors.New("invalid (empty) destination workset")
	}

	// find model parameter hId by name
	i, ok := modelDef.ParamByName(paramName)
	if !ok {
		return errors.New("model: " + modelDef.Model.Name + " parameter " + paramName + " not found")
	}
	pm := modelDef.Param[i]

	// copy parameter metadata and values  from one workset to another inside of transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err = dbCopyParameterFromWorkset(trx, srcWs, &pm, dstWs); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// dbCopyParameterFromRun copy workset parameter metadata and values into destination workset from model run.
// It does copy as part of transaction.
// If parameter already exist in destination workset then error returned.
func dbCopyParameterFromRun(trx *sql.Tx, wst *WorksetRow, pm *ParamMeta, rst *RunRow) error {

	// "lock" workset to prevent update or use by the model
	mId := strconv.Itoa(wst.ModelId)

	err := TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = is_readonly + 1"+
			" WHERE model_id = "+mId+" AND set_name = "+toQuoted(wst.Name))
	if err != nil {
		return err
	}

	// check if workset exist and not readonly
	var setId, nRd int
	err = TrxSelectFirst(trx,
		"SELECT set_id, is_readonly FROM workset_lst WHERE model_id = "+mId+" AND set_name = "+toQuoted(wst.Name),
		func(row *sql.Row) error {
			if err := row.Scan(&setId, &nRd); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return errors.New("failed to copy: destination workset not found: " + wst.Name)
	case err != nil:
		return err
	case nRd != 1:
		return errors.New("failed to copy: destination workset is read-only: " + wst.Name)
	}

	// return error if parameter already exist in workset_parameter
	sDstId := strconv.Itoa(setId)
	sHid := strconv.Itoa(pm.ParamHid)

	err = TrxUpdate(trx, "UPDATE workset_parameter"+
		" SET parameter_hid = "+sHid+
		" WHERE set_id = "+sDstId+" AND parameter_hid = "+sHid)
	if err != nil {
		return err
	}

	err = TrxSelectFirst(trx,
		"SELECT parameter_hid FROM workset_parameter WHERE set_id = "+sDstId+" AND parameter_hid = "+sHid,
		func(row *sql.Row) error {
			var n int
			if err := row.Scan(&n); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err != nil && err != sql.ErrNoRows:
		return err
	case err != sql.ErrNoRows: // must be sql.ErrNoRows
		return errors.New("failed to copy, destination workset already contains parameter: " + wst.Name + ": " + pm.Name)
	}

	// get base run id of source run
	srId := strconv.Itoa(rst.RunId)

	var baseRunId int
	err = TrxSelectFirst(trx,
		"SELECT base_run_id FROM run_parameter"+" WHERE run_id = "+srId+" AND parameter_hid = "+sHid,
		func(row *sql.Row) error {
			if err := row.Scan(&baseRunId); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return errors.New("failed to copy, model run not found or does not contain parameter: " + rst.Name + ": " + pm.Name)
	case err != nil:
		return err
	case baseRunId <= 0:
		return errors.New("failed to copy, model run not found or invalid base run id: " + rst.Name + ": " + pm.Name)
	}

	// add parameter to the list of workset parameters
	err = TrxUpdate(trx,
		"INSERT INTO workset_parameter (set_id, parameter_hid, sub_count)"+
			" SELECT "+sDstId+", parameter_hid, sub_count"+
			" FROM run_parameter"+
			" WHERE run_id = "+srId+" AND parameter_hid = "+sHid)
	if err != nil {
		return err
	}

	// copy parameter metadata from model run to workset
	err = TrxUpdate(trx,
		"INSERT INTO workset_parameter_txt (set_id, parameter_hid, lang_id, note)"+
			" SELECT "+sDstId+", parameter_hid, lang_id, note"+
			" FROM run_parameter_txt"+
			" WHERE run_id = "+srId+" AND parameter_hid = "+sHid)
	if err != nil {
		return err
	}

	// do copy parameter values from base model run
	sDim := ""
	for k := range pm.Dim {
		sDim += pm.Dim[k].Name + ", "
	}

	err = TrxUpdate(trx,
		"INSERT INTO "+pm.DbSetTable+
			" (set_id, sub_id, "+sDim+"param_value)"+
			" SELECT "+sDstId+", sub_id, "+sDim+"param_value"+
			" FROM "+pm.DbRunTable+
			" WHERE run_id = "+strconv.Itoa(baseRunId))
	if err != nil {
		return err
	}

	// "unlock" workset before commit: restore original value of is_readonly=0
	err = TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = 0,"+
			" update_dt = "+toQuoted(helper.MakeDateTime(time.Now()))+
			" WHERE set_id = "+sDstId)
	if err != nil {
		return err
	}
	return nil
}

// dbCopyParameterFromWorkset copy workset parameter metadata and values from one workset to another.
// It does copy as part of transaction.
// If parameter already exist in destination workset then error returned.
func dbCopyParameterFromWorkset(trx *sql.Tx, srcWs *WorksetRow, pm *ParamMeta, dstWs *WorksetRow) error {

	// "lock" destination workset to prevent update or use by the model
	mId := strconv.Itoa(dstWs.ModelId)

	err := TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = is_readonly + 1"+
			" WHERE model_id = "+mId+" AND set_name = "+toQuoted(dstWs.Name))
	if err != nil {
		return err
	}

	// check if workset exist and not readonly
	var setId, nRd int
	err = TrxSelectFirst(trx,
		"SELECT set_id, is_readonly FROM workset_lst WHERE model_id = "+mId+" AND set_name = "+toQuoted(dstWs.Name),
		func(row *sql.Row) error {
			if err := row.Scan(&setId, &nRd); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return errors.New("failed to copy: destination workset not found: " + dstWs.Name)
	case err != nil:
		return err
	case nRd != 1:
		return errors.New("failed to copy: destination workset is read-only: " + dstWs.Name)
	}
	sDstId := strconv.Itoa(setId)

	// return error if parameter already exist in workset_parameter
	sHid := strconv.Itoa(pm.ParamHid)

	err = TrxUpdate(trx, "UPDATE workset_parameter"+
		" SET parameter_hid = "+sHid+
		" WHERE set_id = "+sDstId+" AND parameter_hid = "+sHid)
	if err != nil {
		return err
	}

	err = TrxSelectFirst(trx,
		"SELECT parameter_hid FROM workset_parameter WHERE set_id = "+sDstId+" AND parameter_hid = "+sHid,
		func(row *sql.Row) error {
			var n int
			if err := row.Scan(&n); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err != nil && err != sql.ErrNoRows:
		return err
	case err != sql.ErrNoRows: // must be sql.ErrNoRows
		return errors.New("failed to copy, destination workset already contains parameter: " + dstWs.Name + ": " + pm.Name)
	}

	// "lock" source workset to prevent update or use by the model
	err = TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = is_readonly + 1"+
			" WHERE model_id = "+mId+" AND set_name = "+toQuoted(srcWs.Name))
	if err != nil {
		return err
	}

	// check if source workset exist and is readonly
	srcBaseId := 0
	err = TrxSelectFirst(trx,
		"SELECT set_id, base_run_id, is_readonly FROM workset_lst WHERE model_id = "+mId+" AND set_name = "+toQuoted(srcWs.Name),
		func(row *sql.Row) error {
			var rId sql.NullInt64
			if err := row.Scan(&setId, &rId, &nRd); err != nil {
				return err
			}
			if rId.Valid {
				srcBaseId = int(rId.Int64)
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return errors.New("failed to copy: source workset not found: " + srcWs.Name)
	case err != nil:
		return err
	case nRd <= 1:
		return errors.New("failed to copy: source workset is not read-only: " + srcWs.Name)
	}
	sSrcId := strconv.Itoa(setId)
	sBaseId := strconv.Itoa(srcBaseId)

	// find is parameter in source workset or in a base run of source workset
	isFromRun := false
	var n int

	err = TrxSelectFirst(trx,
		"SELECT parameter_hid FROM workset_parameter WHERE set_id = "+sSrcId+" AND parameter_hid = "+sHid,
		func(row *sql.Row) error {
			if err := row.Scan(&n); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err != nil && err != sql.ErrNoRows:
		return err
	case err == sql.ErrNoRows:
		isFromRun = true
	}
	if isFromRun && srcBaseId <= 0 {
		return errors.New("failed to copy, source workset does not conatin parameter and not based on model run: " + srcWs.Name + ": " + pm.Name)
	}

	// add parameter to the list of destination workset parameters
	q := ""
	if isFromRun {
		q = "INSERT INTO workset_parameter (set_id, parameter_hid, sub_count)" +
			" SELECT " + sDstId + ", parameter_hid, sub_count" +
			" FROM run_parameter" +
			" WHERE run_id = " + sBaseId + " AND parameter_hid = " + sHid
	} else {
		q = "INSERT INTO workset_parameter (set_id, parameter_hid, sub_count)" +
			" SELECT " + sDstId + ", parameter_hid, sub_count" +
			" FROM workset_parameter" +
			" WHERE set_id = " + sSrcId + " AND parameter_hid = " + sHid
	}

	err = TrxUpdate(trx, q)
	if err != nil {
		return err
	}

	// copy parameter metadata from source workset of from base run to destination workset
	if isFromRun {
		q = "INSERT INTO workset_parameter_txt (set_id, parameter_hid, lang_id, note)" +
			" SELECT " + sDstId + ", parameter_hid, lang_id, note" +
			" FROM run_parameter_txt" +
			" WHERE run_id = " + sBaseId + " AND parameter_hid = " + sHid
	} else {
		q = "INSERT INTO workset_parameter_txt (set_id, parameter_hid, lang_id, note)" +
			" SELECT " + sDstId + ", parameter_hid, lang_id, note" +
			" FROM workset_parameter_txt" +
			" WHERE set_id = " + sSrcId + " AND parameter_hid = " + sHid
	}

	err = TrxUpdate(trx, q)
	if err != nil {
		return err
	}

	// do copy parameter values from base model run
	sDim := ""
	for k := range pm.Dim {
		sDim += pm.Dim[k].Name + ", "
	}

	if isFromRun {
		q = "INSERT INTO " + pm.DbSetTable +
			" (set_id, sub_id, " + sDim + "param_value)" +
			" SELECT " + sDstId + ", sub_id, " + sDim + "param_value" +
			" FROM " + pm.DbRunTable +
			" WHERE run_id = " +
			"(" +
			"SELECT base_run_id FROM run_parameter WHERE run_id = " + sBaseId + " AND parameter_hid = " + sHid +
			")"
	} else {
		q = "INSERT INTO " + pm.DbSetTable +
			" (set_id, sub_id, " + sDim + "param_value)" +
			" SELECT " + sDstId + ", sub_id, " + sDim + "param_value" +
			" FROM " + pm.DbSetTable +
			" WHERE set_id = " + sSrcId
	}

	err = TrxUpdate(trx, q)
	if err != nil {
		return err
	}

	// "unlock" source workset before commit: restore original value of is_readonly=1
	err = TrxUpdate(trx,
		"UPDATE workset_lst SET is_readonly = 1 WHERE set_id = "+sSrcId)
	if err != nil {
		return err
	}

	// "unlock" destination workset before commit: restore original value of is_readonly=0
	err = TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = 0,"+
			" update_dt = "+toQuoted(helper.MakeDateTime(time.Now()))+
			" WHERE set_id = "+sDstId)

	return err // return last error, if any
}
