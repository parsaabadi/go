// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"database/sql"
	"errors"
	"strconv"
)

// ReadOutputTable read ouput table page (dimensions and values) from model run results.
//
// If layout.IsAccum true then select accumulator(s) else output expression value(s)
// If layout.ValueName not empty then select only that expression (accumulator) else all expressions (accumulators)
func ReadOutputTable(dbConn *sql.DB, modelDef *ModelMeta, layout *ReadOutTableLayout) (*list.List, error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if layout == nil {
		return nil, errors.New("invalid (empty) page layout")
	}
	if layout.Name == "" {
		return nil, errors.New("invalid (empty) ouput table name")
	}

	// find output table id by name
	var table *TableMeta
	if k, ok := modelDef.OutTableByName(layout.Name); ok {
		table = &modelDef.Table[k]
	} else {
		return nil, errors.New("output table not found: " + layout.Name)
	}

	// find expression or accumulator id by name
	valId := -1

	if layout.ValueName != "" {

		if layout.IsAccum { // find accumulator

			for i := range table.Acc {
				if table.Acc[i].Name == layout.ValueName {
					valId = table.Acc[i].AccId
				}
			}
			if valId < 0 {
				return nil, errors.New("output table accumulator not found: " + layout.Name + " " + layout.ValueName)
			}

		} else { // find expression

			for i := range table.Expr {
				if table.Expr[i].Name == layout.ValueName {
					valId = table.Expr[i].ExprId
				}
			}
			if valId < 0 {
				return nil, errors.New("output table expression not found: " + layout.Name + " " + layout.ValueName)
			}
		}

	}

	// number of accumulator value columns: acc_value or acc0, acc1, acc2...
	accCount := 1
	if layout.IsAccum && layout.IsAllAccum && layout.ValueName == "" {
		accCount = len(table.Acc)
	}

	// check if model run exist and model run completed
	runRow, err := GetRun(dbConn, layout.FromId)
	if err != nil {
		return nil, err
	}
	if runRow == nil {
		return nil, errors.New("model run not found, id: " + strconv.Itoa(layout.FromId))
	}
	if runRow.Status != DoneRunStatus {
		return nil, errors.New("model run not completed successfully, id: " + strconv.Itoa(layout.FromId))
	}

	// make sql to select output table expression(s) from model run:
	//
	//   SELECT expr_id, dim0, dim1, expr_value
	//   FROM salarySex_v2012_820
	//   WHERE run_id =
	//   (
	//     SELECT base_run_id FROM run_table WHERE run_id = 2 AND table_hid = 12345
	//   )
	//   AND expr_id = 3
	//   AND dim1 IN (10, 20, 30, 40)
	//   ORDER BY 1, 2, 3
	//
	// or accumulator(s):
	//
	//   SELECT acc_id, sub_id, dim0, dim1, acc_value
	//   FROM salarySex_a2012_820
	//   WHERE run_id =
	//   (
	//     SELECT base_run_id FROM run_table WHERE run_id = 2 AND table_hid = 12345
	//   )
	//   AND acc_id = 4
	//   AND dim1 IN (10, 20, 30, 40)
	//   ORDER BY 1, 2, 3, 4
	//
	// or all accumulators view:
	//
	//   SELECT sub_id, dim0, dim1, acc0, acc1
	//   FROM salarySex_d2012_820
	//   WHERE run_id =
	//   (
	//     SELECT base_run_id FROM run_table WHERE run_id = 2 AND table_hid = 12345
	//   )
	//   AND dim1 IN (10, 20, 30, 40)
	//   ORDER BY 1, 2, 3
	//
	q := "SELECT"

	if layout.IsAccum {
		if !layout.IsAllAccum {
			q += " acc_id,"
		}
		q += " sub_id"
	} else {
		q += " expr_id"
	}

	for k := range table.Dim {
		q += ", " + table.Dim[k].Name
	}

	if !layout.IsAccum {
		q += ", expr_value FROM " + table.DbExprTable
	} else {
		if !layout.IsAllAccum {
			q += ", acc_value FROM " + table.DbAccTable
		} else {
			if layout.ValueName != "" {
				q += ", " + layout.ValueName
			} else {
				for k := range table.Acc {
					q += ", " + table.Acc[k].Name
				}
			}
			q += " FROM " + table.DbAccAllView
		}
	}

	q += " WHERE run_id =" +
		" (SELECT base_run_id FROM run_table" +
		" WHERE run_id = " + strconv.Itoa(layout.FromId) +
		" AND table_hid = " + strconv.Itoa(table.TableHid) + ")"

	if !layout.IsAllAccum && valId >= 0 {
		if layout.IsAccum {
			q += " AND acc_id = " + strconv.Itoa(valId)
		} else {
			q += " AND expr_id = " + strconv.Itoa(valId)
		}
	}

	// append dimension filters, if specified
	for k := range layout.Filter {

		// find dimension index by name
		dix := -1
		for j := range table.Dim {
			if table.Dim[j].Name == layout.Filter[k].DimName {
				dix = j
				break
			}
		}
		if dix < 0 {
			return nil, errors.New("output table " + table.Name + " does not have dimension " + layout.Filter[k].DimName)
		}

		f, err := makeDimFilter(
			modelDef, &layout.Filter[k], table.Dim[dix].Name, table.Dim[dix].typeOf, "output table "+table.Name)
		if err != nil {
			return nil, err
		}

		q += " AND " + f
	}

	// append order by expr_id or acc_id, sub_id or sub_id
	nExtraCol := 1
	if layout.IsAllAccum && !layout.IsAllAccum {
		nExtraCol = 2 // extra columns: acc_id, sub_id
	}
	q += makeOrderBy(table.Rank, layout.OrderBy, nExtraCol)

	// prepare db-row conversion buffer:
	// acc_id, sub_id, expr_id, dimensions, value or []values
	var n1, n2 int
	d := make([]int, table.Rank)
	var vf sql.NullFloat64
	fa := make([]sql.NullFloat64, accCount)
	var scanBuf []interface{}

	scanBuf = append(scanBuf, &n1)
	if layout.IsAccum && !layout.IsAllAccum {
		scanBuf = append(scanBuf, &n2)
	}
	for k := 0; k < table.Rank; k++ {
		scanBuf = append(scanBuf, &d[k])
	}
	if !layout.IsAccum || !layout.IsAllAccum {
		scanBuf = append(scanBuf, &vf)
	} else {
		for k := 0; k < accCount; k++ {
			scanBuf = append(scanBuf, &fa[k])
		}
	}

	// select cells:
	// expr_id or or sub_id or acc_id and sub_id, dimension(s) enum ids
	// value or all accumulator values and null status
	cLst, err := SelectToList(dbConn, q, layout.Offset, layout.Size,
		func(rows *sql.Rows) (interface{}, error) {

			if err := rows.Scan(scanBuf...); err != nil {
				return nil, err
			}

			// make new cell from conversion buffer
			if layout.IsAccum {

				if !layout.IsAllAccum {
					var ca = CellAcc{cellValue: cellValue{DimIds: make([]int, table.Rank)}}
					ca.AccId = n1
					ca.SubId = n2
					copy(ca.DimIds, d)
					ca.IsNull = !vf.Valid
					ca.Value = 0.0
					if !ca.IsNull {
						ca.Value = vf.Float64
					}
					return ca, nil
				}
				// else all accumulators

				var cl = CellAllAcc{
					DimIds: make([]int, table.Rank),
					IsNull: make([]bool, accCount),
					Value:  make([]float64, accCount)}

				cl.SubId = n1
				copy(cl.DimIds, d)

				for k := 0; k < accCount; k++ {
					cl.IsNull[k] = !fa[k].Valid
					cl.Value[k] = 0.0
					if !cl.IsNull[k] {
						cl.Value[k] = fa[k].Float64
					}
				}
				return cl, nil
			}
			// else output table expression

			var ce = CellExpr{cellValue: cellValue{DimIds: make([]int, table.Rank)}}
			ce.ExprId = n1
			copy(ce.DimIds, d)
			ce.IsNull = !vf.Valid
			ce.Value = 0.0
			if !ce.IsNull {
				ce.Value = vf.Float64
			}
			return ce, nil
		})
	if err != nil {
		return nil, err
	}

	return cLst, nil
}
