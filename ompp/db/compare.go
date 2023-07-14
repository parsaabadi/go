// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"database/sql"
	"errors"
)

// CompareOutputTable read output table page (dimensions and values) and compare multiple runs using calculation.
//
// If cmpLt.IsAggr true then do accumulator(s) aggregation else calculate expression value(s), ex: Expr1[variant] - Expr1[base].
func CompareOutputTable(dbConn *sql.DB, modelDef *ModelMeta, tableLt *ReadTableLayout, cmpLt *CalculateTableLayout, runIds []int) (*list.List, *ReadPageLayout, error) {

	// validate parameters
	if modelDef == nil {
		return nil, nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if tableLt == nil || cmpLt == nil {
		return nil, nil, errors.New("invalid (empty) output table layout or comparison")
	}
	if tableLt.Name == "" {
		return nil, nil, errors.New("invalid (empty) output table name")
	}
	if cmpLt == nil || cmpLt.Calculate == "" {
		return nil, nil, errors.New("invalid (empty) comparison expression")
	}

	// find output table id by name
	var table *TableMeta
	if k, ok := modelDef.OutTableByName(tableLt.Name); ok {
		table = &modelDef.Table[k]
	} else {
		return nil, nil, errors.New("output table not found: " + tableLt.Name)
	}

	// translate comparison calculation to sql
	var q string
	var err error

	if cmpLt.IsAggr {
		q, err = translateToAccSql(table, &tableLt.ReadLayout, &cmpLt.CalculateLayout, runIds)
	} else {
		q, err = translateToExprSql(table, &tableLt.ReadLayout, &cmpLt.CalculateLayout, runIds)
	}
	if err != nil {
		return nil, nil, err
	}

	// prepare db-row scan conversion buffer: run_id, expression id, dimensions, value
	var runId int
	var calcId int
	d := make([]int, table.Rank)
	var vf sql.NullFloat64
	var scanBuf []interface{}

	scanBuf = append(scanBuf, &runId)
	scanBuf = append(scanBuf, &calcId)

	for k := 0; k < table.Rank; k++ {
		scanBuf = append(scanBuf, &d[k])
	}
	scanBuf = append(scanBuf, &vf)

	// select cells:
	// run_id, dimension(s) enum ids, value null status
	cLst, lt, err := SelectToList(dbConn, q, tableLt.ReadPageLayout,
		func(rows *sql.Rows) (interface{}, error) {

			if err := rows.Scan(scanBuf...); err != nil {
				return nil, err
			}

			// make new cell from conversion buffer
			c := CellTableCalc{
				cellIdValue: cellIdValue{DimIds: make([]int, table.Rank)},
				CalcId:      calcId,
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
