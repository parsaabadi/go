// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"database/sql"
	"errors"
)

// CompareOutputTable read output table page (dimensions and values) from model run results.
//
// If layout.IsAccum true then select accumulator(s) else output expression value(s)
// If layout.ValueName not empty then select only that expression (accumulator) else all expressions (accumulators)
func CompareOutputTable(dbConn *sql.DB, modelDef *ModelMeta, layout *CompareTableLayout, runIds []int) (*list.List, *ReadPageLayout, error) {

	// validate parameters
	if modelDef == nil {
		return nil, nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if layout == nil {
		return nil, nil, errors.New("invalid (empty) page layout")
	}
	if layout.Name == "" {
		return nil, nil, errors.New("invalid (empty) output table name")
	}

	// find output table id by name
	var table *TableMeta
	if k, ok := modelDef.OutTableByName(layout.Name); ok {
		table = &modelDef.Table[k]
	} else {
		return nil, nil, errors.New("output table not found: " + layout.Name)
	}

	// translate comparison expression to sql
	var q string
	var err error

	if layout.IsAccum {
		q, err = translateToAccSql(modelDef, table, &layout.CompareLayout, runIds)
	} else {
		q, err = translateToExprSql(modelDef, table, &layout.CompareLayout, runIds)
	}
	if err != nil {
		return nil, nil, err
	}

	// prepare db-row conversion buffer: run_id, dimensions, value
	var runId int
	d := make([]int, table.Rank)
	var vf sql.NullFloat64
	var scanBuf []interface{}

	scanBuf = append(scanBuf, &runId)
	for k := 0; k < table.Rank; k++ {
		scanBuf = append(scanBuf, &d[k])
	}
	scanBuf = append(scanBuf, &vf)

	// select cells:
	// run_id, dimension(s) enum ids, value null status
	cLst, lt, err := SelectToList(dbConn, q, layout.ReadPageLayout,
		func(rows *sql.Rows) (interface{}, error) {

			if err := rows.Scan(scanBuf...); err != nil {
				return nil, err
			}

			// make new cell from conversion buffer
			var c = CellTableCmp{
				cellIdValue: cellIdValue{DimIds: make([]int, table.Rank)},
				RunId:       runId,
			}
			copy(c.DimIds, d)
			c.IsNull = !vf.Valid
			c.Value = 0.0
			if !c.IsNull {
				c.Value = vf.Float64
			}
			return c, nil
		})
	if err != nil {
		return nil, nil, err
	}

	return cLst, lt, nil
}
