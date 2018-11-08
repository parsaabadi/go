// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.openmpp.org/ompp/helper"
)

// WriteParameter insert or update parameter values in workset or insert parameter values into model run.
//
// If this is model run update (layout.IsToRun is true) then model run must exist and be in completed state (i.e. success or error state).
// Model run should not already contain parameter values: parameter can be inserted only once in model run and cannot be updated after.
//
// If workset already contain parameter values then values updated else inserted.
// If only "page" of workset parameter rows supplied (layout.IsPage is true)
// then each row deleted by primary key before insert else all rows deleted by one delete by set id.
//
// Double format is used for float model types digest calculation, if non-empty format supplied
func WriteParameter(dbConn *sql.DB, modelDef *ModelMeta, layout *WriteParamLayout, cellLst *list.List) error {

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata, look like model not found")
	}
	if layout == nil {
		return errors.New("invalid (empty) write layout")
	}
	if layout.Name == "" {
		return errors.New("invalid (empty) parameter name")
	}
	if layout.ToId <= 0 {
		if layout.IsToRun {
			return errors.New("invalid destination run id: " + strconv.Itoa(layout.ToId))
		}
		return errors.New("invalid destination set id: " + strconv.Itoa(layout.ToId))
	}
	if layout.SubCount <= 0 {
		return errors.New("invalid number of parameter sub-vaules: " + strconv.Itoa(layout.SubCount))
	}
	if cellLst == nil {
		return errors.New("invalid (empty) parameter values")
	}

	// find parameter id by name
	var param *ParamMeta
	if k, ok := modelDef.ParamByName(layout.Name); ok {
		param = &modelDef.Param[k]
	} else {
		return errors.New("parameter not found: " + layout.Name)
	}

	// do insert or update parameter in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if layout.IsToRun {
		err = doWriteRunParameter(trx, modelDef, param, layout.ToId, layout.SubCount, cellLst, layout.DoubleFmt)
	} else {
		err = doWriteSetParameter(trx, param, layout.ToId, layout.SubCount, layout.IsPage, cellLst)
	}
	if err != nil {
		trx.Rollback()
		return err
	}

	trx.Commit()
	return nil
}

// doWriteRunParameter insert parameter values into model run.
// It does insert as part of transaction
// Model run must exist and be in completed state (i.e. success or error state).
// Model run should not already contain parameter values: parameter can be inserted only once in model run and cannot be updated after.
// Double format is used for float model types digest calculation, if non-empty format supplied
func doWriteRunParameter(
	trx *sql.Tx, modelDef *ModelMeta, param *ParamMeta, runId int, subCount int, cellLst *list.List, doubleFmt string,
) error {

	// start run update
	srId := strconv.Itoa(runId)
	err := TrxUpdate(trx,
		"UPDATE run_lst SET update_dt = "+toQuoted(helper.MakeDateTime(time.Now()))+" WHERE run_id = "+srId)
	if err != nil {
		return err
	}

	// check if model run exist and status is completed
	st := ""
	err = TrxSelectFirst(trx,
		"SELECT status FROM run_lst WHERE run_id = "+srId,
		func(row *sql.Row) error {
			if err := row.Scan(&st); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return errors.New("model run not found, id: " + srId)
	case err != nil:
		return err
	}
	if st != DoneRunStatus && st != ExitRunStatus && st != ErrorRunStatus {
		return errors.New("model run not completed, id: " + srId)
	}

	// check if parameter values not already exist for that run
	sHid := strconv.Itoa(param.ParamHid)
	n := 0
	err = TrxSelectFirst(trx,
		"SELECT COUNT(*) FROM run_parameter"+" WHERE run_id = "+srId+" AND parameter_hid = "+sHid,
		func(row *sql.Row) error {
			if err := row.Scan(&n); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err != nil && err != sql.ErrNoRows:
		return err
	}
	if n > 0 {
		return errors.New("model run with id: " + srId + " already contain parameter values " + param.Name)
	}

	// calculate parameter digest
	digest, err := digestParameter(modelDef, param, cellLst, doubleFmt)
	if err != nil {
		return err
	}

	// insert into run_parameter with digest and current run id as base run id
	err = TrxUpdate(trx,
		"INSERT INTO run_parameter (run_id, parameter_hid, base_run_id, sub_count, run_digest)"+
			" VALUES ("+
			srId+", "+sHid+", "+srId+", "+strconv.Itoa(subCount)+", "+toQuoted(digest)+")")
	if err != nil {
		return err
	}

	// find base run by digest, it must exist
	nBase := 0
	err = TrxSelectFirst(trx,
		"SELECT MIN(run_id) FROM run_parameter"+
			" WHERE parameter_hid = "+sHid+
			" AND run_digest = "+toQuoted(digest),
		func(row *sql.Row) error {
			if err := row.Scan(&nBase); err != nil {
				return err
			}
			return nil
		})
	switch {
	// case err == sql.ErrNoRows: it must exist
	case err != nil:
		return err
	}

	// if parameter values already exist then update base run id
	// else insert new parameter values into model run
	if runId != nBase {

		err = TrxUpdate(trx,
			"UPDATE run_parameter SET base_run_id = "+strconv.Itoa(nBase)+
				" WHERE parameter_hid = "+sHid+
				" AND run_id = "+srId)
		if err != nil {
			return err
		}
	} else { // insert new parameter values into model run

		// make sql to insert parameter values into model run
		// prepare put() closure to convert each cell into insert sql statement parameters
		q := makeSqlInsertParamValue(param.DbRunTable, "run_id", param.Dim, runId)
		put := makePutInsertParamValue(param, subCount, cellLst)

		// execute sql insert using put() above for each row
		if err = TrxUpdateStatement(trx, q, put); err != nil {
			return errors.New("insert parameter failed: " + param.Name + " " + err.Error())
		}
	}

	return nil
}

// digestParameter retrun digest of parameter values.
// Double format is used for float model types digest calculation, if non-empty format supplied
func digestParameter(modelDef *ModelMeta, param *ParamMeta, cellLst *list.List, doubleFmt string) (string, error) {

	// start from name and metadata digest
	hMd5 := md5.New()
	_, err := hMd5.Write([]byte("parameter_name,parameter_digest\n"))
	if err != nil {
		return "", err
	}
	_, err = hMd5.Write([]byte(param.Name + "," + param.Digest + "\n"))
	if err != nil {
		return "", err
	}

	// append digest of accumulator(s) cells
	var pc CellParam
	if err = digestCells(hMd5, modelDef, param.Name, pc, cellLst, doubleFmt); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hMd5.Sum(nil)), nil // retrun digest as hex string
}

// doWriteSetParameter insert or update parameter values in workset.
// It does insert as part of transaction
// If workset already contain parameter values then values updated else inserted.
func doWriteSetParameter(trx *sql.Tx, param *ParamMeta, setId int, subCount int, isPage bool, cellLst *list.List) error {

	// start workset update
	sId := strconv.Itoa(setId)
	err := TrxUpdate(trx,
		"UPDATE workset_lst"+
			" SET is_readonly = is_readonly + 1, update_dt = "+toQuoted(helper.MakeDateTime(time.Now()))+
			" WHERE set_id = "+sId)
	if err != nil {
		return err
	}

	// check if workset exist and not readonly
	nRd := 0
	err = TrxSelectFirst(trx,
		"SELECT is_readonly FROM workset_lst WHERE set_id = "+sId,
		func(row *sql.Row) error {
			if err := row.Scan(&nRd); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return errors.New("workset not found, id: " + sId)
	case err != nil:
		return err
	}
	if nRd != 1 {
		return errors.New("cannot update parameter " + param.Name + ", workset is readonly, id: " + sId)
	}

	// delete existing parameter values
	if !isPage {
		if err = TrxUpdate(trx, "DELETE FROM "+param.DbSetTable+" WHERE set_id = "+sId); err != nil {
			return err
		}
	} else {

		// make sql to delete parameter rows by primary key from workset
		// prepare put() closure to convert each cell into delete sql statement parameters
		q := makeSqlDeleteParamPage(param.DbSetTable, param.Dim, setId)
		put := makePutDeleteParamPage(param, cellLst)

		// execute sql delete using put() above for each row
		if err = TrxUpdateStatement(trx, q, put); err != nil {
			return errors.New("delete parameter failed: " + param.Name + " " + err.Error())
		}
	}

	// make sql to insert parameter values into workset
	// prepare put() closure to convert each cell into insert sql statement parameters
	q := makeSqlInsertParamValue(param.DbSetTable, "set_id", param.Dim, setId)
	put := makePutInsertParamValue(param, subCount, cellLst)

	// execute sql insert using put() above for each row
	if err = TrxUpdateStatement(trx, q, put); err != nil {
		return errors.New("insert parameter failed: " + param.Name + " " + err.Error())
	}

	// update completed: reset readonly status to "read-write"
	err = TrxUpdate(trx, "UPDATE workset_lst SET is_readonly = 0 WHERE set_id = "+sId)
	if err != nil {
		return err
	}
	return nil
}

// make sql to insert parameter values into model run or workset
func makeSqlInsertParamValue(dbTable string, runSetCol string, dims []ParamDimsRow, toId int) string {

	// INSERT INTO ageSex_w2012817 (set_id, sub_id, dim0, dim1, param_value) VALUES (2, ?, ?, ?, ?)
	q := "INSERT INTO " + dbTable +
		" (" + runSetCol + ", sub_id, "

	for k := range dims {
		q += dims[k].Name + ", "
	}

	q += "param_value) VALUES (" + strconv.Itoa(toId) + ", ?, "

	for k := 0; k < len(dims); k++ {
		q += "?, "
	}
	q += "?)"

	return q
}

// prepare put() closure to convert each cell into insert sql statement parameters
func makePutInsertParamValue(param *ParamMeta, subCount int, cellLst *list.List) func() (bool, []interface{}, error) {

	// converter from value into db value
	fv := cvtValue(param)

	//  for each cell put into row of sql statement parameters
	row := make([]interface{}, param.Rank+2)
	c := cellLst.Front()

	put := func() (bool, []interface{}, error) {

		if c == nil {
			return false, nil, nil // end of data
		}

		// convert and check input row
		cell, ok := c.Value.(CellParam)
		if !ok {
			return false, nil, errors.New("invalid type, expected: parameter cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return false, nil, errors.New("invalid size of row buffer, expected: " + strconv.Itoa(n+2))
		}

		if cell.SubId < 0 || cell.SubId >= subCount {
			return false, nil, errors.New("invalid sub-value id: " + strconv.Itoa(cell.SubId) + " parameter: " + param.Name)
		}

		// set sql statement parameter values: sub-value number, dimensions enum, parameter value
		row[0] = cell.SubId

		for k, e := range cell.DimIds {
			row[k+1] = e
		}
		if v, err := fv(cell.IsNull, cell.Value); err == nil {
			row[n+1] = v
		} else {
			return false, nil, err
		}

		// move to next input row and return current row to sql statement
		c = c.Next()
		return true, row, nil
	}
	return put
}

// make sql to delete parameter rows from workset by specified keys: set id, sub-value number and dimension(s) items id
func makeSqlDeleteParamPage(dbTable string, dims []ParamDimsRow, setId int) string {

	// DELETE FROM ageSex_w2012817 WHERE set_id = 2 AND sub_id = ? AND dim0 = ? AND dim1 = ?
	q := "DELETE FROM " + dbTable +
		" WHERE set_id = " + strconv.Itoa(setId) +
		" AND sub_id = ?"

	for k := range dims {
		q += " AND " + dims[k].Name + " = ?"
	}
	return q
}

// prepare put() closure to convert each cell into delete sql statement parameters
func makePutDeleteParamPage(param *ParamMeta, cellLst *list.List) func() (bool, []interface{}, error) {

	// for each cell put into row of sql statement parameters
	row := make([]interface{}, param.Rank+1)
	c := cellLst.Front()

	put := func() (bool, []interface{}, error) {

		if c == nil {
			return false, nil, nil // end of data
		}

		// convert and check input row
		cell, ok := c.Value.(CellParam)
		if !ok {
			return false, nil, errors.New("invalid type, expected: parameter cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+1 {
			return false, nil, errors.New("invalid parameter row size, expected: " + strconv.Itoa(n+1))
		}

		// set sql statement parameter values: sub-value number, dimensions enum, parameter value
		row[0] = cell.SubId

		for k, e := range cell.DimIds {
			row[k+1] = e
		}

		// move to next input row and return current row to sql statement
		c = c.Next()
		return true, row, nil
	}
	return put
}

// cvtValue return converter from source value into db value
// converter does type validation
// for non built-it types validate enum id presense in enum list
// only float parameter values can be NULL, for any other parameter types NULL values rejected.
func cvtValue(param *ParamMeta) func(bool, interface{}) (interface{}, error) {

	// float parameter: check if isNull flag, validate and convert type
	// cell value is nullable for extended parameters only
	var isNullable = param.IsExtendable

	if param.typeOf.IsFloat() {
		return func(isNull bool, src interface{}) (interface{}, error) {
			if isNull && !isNullable || src == nil {
				return nil, errors.New("invalid parameter value, it cannot be NULL")
			}
			if isNull && isNullable {
				return sql.NullFloat64{Float64: 0.0, Valid: false}, nil
			}
			switch src.(type) {
			case float64:
				return sql.NullFloat64{Float64: src.(float64), Valid: !isNull}, nil
			case float32:
				return sql.NullFloat64{Float64: float64(src.(float32)), Valid: !isNull}, nil
			case int:
				return sql.NullFloat64{Float64: float64(src.(int)), Valid: !isNull}, nil
			case uint:
				return sql.NullFloat64{Float64: float64(src.(uint)), Valid: !isNull}, nil
			case int64:
				return sql.NullFloat64{Float64: float64(src.(int64)), Valid: !isNull}, nil
			case uint64:
				return sql.NullFloat64{Float64: float64(src.(uint64)), Valid: !isNull}, nil
			case int32:
				return sql.NullFloat64{Float64: float64(src.(int32)), Valid: !isNull}, nil
			case uint32:
				return sql.NullFloat64{Float64: float64(src.(uint32)), Valid: !isNull}, nil
			case int16:
				return sql.NullFloat64{Float64: float64(src.(int16)), Valid: !isNull}, nil
			case uint16:
				return sql.NullFloat64{Float64: float64(src.(uint16)), Valid: !isNull}, nil
			case int8:
				return sql.NullFloat64{Float64: float64(src.(int8)), Valid: !isNull}, nil
			case uint8:
				return sql.NullFloat64{Float64: float64(src.(uint8)), Valid: !isNull}, nil
			}
			return nil, errors.New("invalid parameter value type, expected: float or double")
		}
	}

	// integer parameter: check value is not null and validate type
	if param.typeOf.IsInt() {
		return func(isNull bool, src interface{}) (interface{}, error) {
			if isNull || src == nil {
				return nil, errors.New("invalid parameter value, it cannot be NULL")
			}
			switch src.(type) {
			case int:
				return src, nil
			case uint:
				return src, nil
			case int64:
				return src, nil
			case uint64:
				return src, nil
			case int32:
				return src, nil
			case uint32:
				return src, nil
			case int16:
				return src, nil
			case uint16:
				return src, nil
			case int8:
				return src, nil
			case uint8:
				return src, nil
			case float64: // from json or oracle (often)
				return src, nil
			case float32: // from json or oracle (unlikely)
				return src, nil
			}
			return nil, errors.New("invalid parameter value type, expected: integer")
		}
	}

	// string parameter: check value is not null and validate type
	if param.typeOf.IsString() {
		return func(isNull bool, src interface{}) (interface{}, error) {
			if isNull || src == nil {
				return nil, errors.New("invalid parameter value, it cannot be NULL")
			}
			switch src.(type) {
			case string:
				return src, nil
			}
			return nil, errors.New("invalid parameter value type, expected: string")
		}
	}

	// boolean is a special case because not all drivers correctly handle conversion to smallint
	if param.typeOf.IsBool() {
		return func(isNull bool, src interface{}) (interface{}, error) {
			if isNull || src == nil {
				return nil, errors.New("invalid parameter value, it cannot be NULL")
			}
			if is, ok := src.(bool); ok && is {
				return 1, nil
			}
			return 0, nil
		}
	}

	// enum-based type: enum id must be in enum list
	return func(isNull bool, src interface{}) (interface{}, error) {

		if isNull || src == nil {
			return nil, errors.New("invalid parameter value, it cannot be NULL")
		}

		// validate type and convert to int
		var iv int
		switch e := src.(type) {
		case int:
			iv = e
		case uint:
			iv = int(e)
		case int64:
			iv = int(e)
		case uint64:
			iv = int(e)
		case int32:
			iv = int(e)
		case uint32:
			iv = int(e)
		case int16:
			iv = int(e)
		case uint16:
			iv = int(e)
		case int8:
			iv = int(e)
		case uint8:
			iv = int(e)
		case float64: // from json or oracle (often)
			iv = int(e)
		case float32: // from json or oracle (unlikely)
			iv = int(e)
		default:
			return nil, errors.New("invalid parameter value type, expected: integer enum")
		}

		// validate enum id: it must be in enum list
		for j := range param.typeOf.Enum {
			if iv == param.typeOf.Enum[j].EnumId {
				return iv, nil
			}
		}
		return nil, errors.New("invalid parameter value type, expected: integer enum id")
	}
}
