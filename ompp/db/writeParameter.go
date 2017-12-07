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
//
// Double format is used for float model types digest calculation, if non-empty format supplied
func WriteParameter(dbConn *sql.DB, modelDef *ModelMeta, layout *WriteParamLayout, subCount int, cellLst *list.List) error {

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
	if subCount <= 0 {
		return errors.New("invalid number of parameter sub-vaules: " + strconv.Itoa(subCount))
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
		if err = doWriteRunParameter(trx, modelDef, param, layout.ToId, subCount, cellLst, layout.DoubleFmt); err != nil {
			trx.Rollback()
			return err
		}
	} else {
		if err = doWriteSetParameter(trx, param, layout.ToId, subCount, cellLst); err != nil {
			trx.Rollback()
			return err
		}
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
		put := makePutInsertParamValue(param, cellLst)

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
func doWriteSetParameter(trx *sql.Tx, param *ParamMeta, setId int, subCount int, cellLst *list.List) error {

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
	if err = TrxUpdate(trx, "DELETE FROM "+param.DbSetTable+" WHERE set_id = "+sId); err != nil {
		return err
	}

	// make sql to insert parameter values into workset
	// prepare put() closure to convert each cell into insert sql statement parameters
	q := makeSqlInsertParamValue(param.DbSetTable, "set_id", param.Dim, setId)
	put := makePutInsertParamValue(param, cellLst)

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

	// INSERT INTO ageSex_w2012817 (set_id, sub_id, dim0, dim1, param_value) VALUES (2, ?, ?, ?)
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
func makePutInsertParamValue(param *ParamMeta, cellLst *list.List) func() (bool, []interface{}, error) {

	// converter from value into db value:
	// boolean is a special case because not all drivers correctly handle conversion to smallint
	fv := func(src interface{}) interface{} { return src }

	if param.typeOf.IsBool() {
		fv = func(src interface{}) interface{} {
			if is, ok := src.(bool); ok && is {
				return 1
			}
			return 0
		}
	}

	// for each cell put into row of sql statement parameters
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

		// set sql statement parameter values: subvalue number, dimensions enum, parameter value
		row[0] = cell.SubId

		for k, e := range cell.DimIds {
			row[k+1] = e
		}
		row[n+1] = fv(cell.Value) // parameter value converted db value

		// move to next input row and return current row to sql statement
		c = c.Next()
		return true, row, nil
	}
	return put
}
