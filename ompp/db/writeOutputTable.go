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

// WriteOutputTable insert output table values (accumulators or expressions) into model run.
// Model run must exist and be in completed state (i.e. success or error state).
// Model run should not already contain output table values: it can be inserted only once in model run and cannot be updated after.
func WriteOutputTable(dbConn *sql.DB, modelDef *ModelMeta, layout *WriteLayout, cellLst *list.List) error {

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata, look like model not found")
	}
	if layout == nil {
		return errors.New("invalid (empty) write layout")
	}
	if layout.Name == "" {
		return errors.New("invalid (empty) output table name")
	}
	if layout.ToId <= 0 {
		return errors.New("invalid destination run id: " + strconv.Itoa(layout.ToId))
	}
	if cellLst == nil {
		return errors.New("invalid (empty) output table values")
	}

	// find output table id by name
	var meta *TableMeta
	if k, ok := modelDef.OutTableByName(layout.Name); ok {
		meta = &modelDef.Table[k]
	} else {
		return errors.New("output table not found: " + layout.Name)
	}

	// do insert or update output table in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err = doWriteOutputTable(trx, meta, layout.IsAccum, layout.ToId, cellLst); err != nil {
		trx.Rollback()
		return err
	}

	trx.Commit()
	return nil
}

// doWriteOutputTable insert output table values (accumulators or expressions) into model run.
// It does insert as part of transaction
// Model run must exist and be in completed state (i.e. success or error state).
// Model run should not already contain output table values: it can be inserted only once in model run and cannot be updated after.
func doWriteOutputTable(trx *sql.Tx, meta *TableMeta, isAcc bool, runId int, cellLst *list.List) error {

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

	// check if output table values not already exist for that run
	n := 0
	err = TrxSelectFirst(trx,
		"SELECT COUNT(*) FROM run_table"+
			" WHERE run_id = "+srId+
			" AND table_hid = "+strconv.Itoa(meta.TableHid),
		func(row *sql.Row) error {
			if err := row.Scan(&n); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows: // unknown error: should never be there
		return errors.New("cannot count output table " + meta.Name + " in model run, id: " + srId)
	case err != nil:
		return err
	}
	if n > 0 {
		return errors.New("model run with id: " + srId + " already contain output table values " + meta.Name)
	}

	// make sql to insert output table accumulators or expressions into model run
	// prepare put() closure to convert each cell into parameters of insert sql statement
	var q string
	var put func() (bool, []interface{}, error)
	if isAcc {
		q = makeSqlAccValueInsert(meta, runId)
		put = makePutAccValueInsert(meta, cellLst)
	} else {
		q = makeSqlExprValueInsert(meta, runId)
		put = makePutExprValueInsert(meta, cellLst)
	}

	// execute sql insert using put() above for each row
	if err = TrxUpdateStatement(trx, q, put); err != nil {
		return err
	}

	// finish adding output table values into model run: update run_table
	// TODO: base run id and digest
	if !isAcc {
		err = TrxUpdate(trx,
			"INSERT INTO run_table (run_id, table_hid, base_run_id, run_digest)"+
				" VALUES ("+
				srId+", "+
				strconv.Itoa(meta.TableHid)+", "+
				srId+", "+
				toQuoted("")+")")
		if err != nil {
			return err
		}
	}
	return nil
}

// make sql to insert output table expressions into model run
func makeSqlExprValueInsert(meta *TableMeta, runId int) string {

	// INSERT INTO salarySex_v2012820
	//   (run_id, expr_id, dim0, dim1, expr_value)
	// VALUES
	//   (2, ?, ?, ?, ?)
	q := "INSERT INTO " + meta.DbExprTable + " (run_id, expr_id, "
	for k := range meta.Dim {
		q += meta.Dim[k].Name + ", "
	}

	q += "expr_value) VALUES (" + strconv.Itoa(runId) + ", ?, "

	for k := 0; k < len(meta.Dim); k++ {
		q += "?, "
	}
	q += "?)"

	return q
}

// prepare put() closure to convert each cell of output expressions into parameters of insert sql statement
func makePutExprValueInsert(meta *TableMeta, cellLst *list.List) func() (bool, []interface{}, error) {

	// for each cell of output expressions put into row of sql statement parameters
	row := make([]interface{}, meta.Rank+2)
	c := cellLst.Front()

	put := func() (bool, []interface{}, error) {

		if c == nil {
			return false, nil, nil // end of data
		}

		// convert and check input row
		cell, ok := c.Value.(CellExpr)
		if !ok {
			return false, nil, errors.New("invalid type, expected: output table expression cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return false, nil, errors.New("invalid size of row buffer, expected: " + strconv.Itoa(n+3))
		}

		// set sql statement parameter values: expression id, dimensions enum
		row[0] = cell.ExprId

		for k, e := range cell.DimIds {
			row[k+1] = e
		}

		// cell value is nullable
		row[n+1] = sql.NullFloat64{Float64: cell.Value.(float64), Valid: !cell.IsNull}

		// move to next input row and return current row to sql statement
		c = c.Next()
		return true, row, nil
	}

	return put
}

// make sql to insert output table accumulators into model run
func makeSqlAccValueInsert(meta *TableMeta, runId int) string {

	// INSERT INTO salarySex_a2012820
	//   (run_id, acc_id, sub_id, dim0, dim1, acc_value)
	// VALUES
	//   (2, ?, ?, ?, ?, ?)
	q := "INSERT INTO " + meta.DbAccTable + " (run_id, acc_id, sub_id, "
	for k := range meta.Dim {
		q += meta.Dim[k].Name + ", "
	}

	q += "acc_value) VALUES (" + strconv.Itoa(runId) + ", ?, ?, "

	for k := 0; k < len(meta.Dim); k++ {
		q += "?, "
	}
	q += "?)"

	return q
}

// prepare put() closure to convert each cell of accumulators into parameters of insert sql statement
func makePutAccValueInsert(meta *TableMeta, cellLst *list.List) func() (bool, []interface{}, error) {

	// for each cell of accumulators put into row of sql statement parameters
	row := make([]interface{}, meta.Rank+3)
	c := cellLst.Front()

	put := func() (bool, []interface{}, error) {

		if c == nil {
			return false, nil, nil // end of data
		}

		// convert and check input row
		cell, ok := c.Value.(CellAcc)
		if !ok {
			return false, nil, errors.New("invalid type, expected: output table accumulator cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+3 {
			return false, nil, errors.New("invalid size of row buffer, expected: " + strconv.Itoa(n+3))
		}

		// set sql statement parameter values: accumulator id and subsample number, dimensions enum
		row[0] = cell.AccId
		row[1] = cell.SubId

		for k, e := range cell.DimIds {
			row[k+2] = e
		}

		// cell value is nullable
		row[n+2] = sql.NullFloat64{Float64: cell.Value.(float64), Valid: !cell.IsNull}

		// move to next input row and return current row to sql statement
		c = c.Next()
		return true, row, nil
	}

	return put
}
