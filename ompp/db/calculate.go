// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"database/sql"
	"errors"
)

const CALCULATED_ID_OFFSET = 1200 // calculated exprssion id offset, for example for Expr1 calculated expression id is 1201

// CalculateOutputTable read output table page (dimensions and values) and calculate extra measure(s).
//
// If calcLt.IsAggr true then do accumulator(s) aggregation else calculate expression value(s), ex: Expr1[variant] - Expr1[base].
func CalculateOutputTable(dbConn *sql.DB, modelDef *ModelMeta, tableLt *ReadTableLayout, calcLt []CalculateTableLayout, runIds []int) (*list.List, *ReadPageLayout, error) {

	// validate parameters
	if modelDef == nil {
		return nil, nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if tableLt == nil {
		return nil, nil, errors.New("invalid (empty) output table layout")
	}
	if tableLt.Name == "" {
		return nil, nil, errors.New("invalid (empty) output table name")
	}
	if len(calcLt) <= 0 {
		return nil, nil, errors.New("invalid (empty) calculation expression(s)")
	}

	// find output table id by name
	var table *TableMeta
	if k, ok := modelDef.OutTableByName(tableLt.Name); ok {
		table = &modelDef.Table[k]
	} else {
		return nil, nil, errors.New("output table not found: " + tableLt.Name)
	}

	// translate calculation to sql
	q, err := translateTableCalcToSql(table, &tableLt.ReadLayout, calcLt, runIds)
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

// Translate all output table calculations to sql query, apply dimension filters, selected run id's and order by.
// It can be a multiple runs comparison and base run id is layout.FromId.
// Or simple expression calculation inside of single run or accumulators aggregation inside of single run,
// in that case layout.FromId and runIds[] are merged.
func translateTableCalcToSql(table *TableMeta, readLt *ReadLayout, calcLt []CalculateTableLayout, runIds []int) (string, error) {

	// translate each calculation to sql: CTE and main sql query
	cteSql := []string{}
	mainSql := []string{}

	for k := range calcLt {

		cte := []string{}
		mSql := ""
		cteAcc := ""
		var err error

		if !calcLt[k].IsAggr {
			cte, mSql, _, err = partialTranslateToExprSql(table, readLt, &calcLt[k].CalculateLayout, runIds)
		} else {
			cteAcc, mSql, err = partialTranslateToAccSql(table, readLt, &calcLt[k].CalculateLayout, runIds)
			if err == nil {
				cte = []string{cteAcc}
			}
		}
		if err != nil {
			return "", err
		}

		// merge main body SQL, expected to be unique, skip duplicates
		isFound := false
		for j := 0; !isFound && j < len(mainSql); j++ {
			isFound = mSql == mainSql[j]
		}
		if isFound {
			continue // skip duplicate SQL, it is the same source expression
		}
		mainSql = append(mainSql, mSql)

		// merge CTE sql's, skip identical CTE
		for _, c := range cte {

			isFound = false
			for j := 0; !isFound && j < len(cteSql); j++ {
				isFound = c == cteSql[j]
			}
			if !isFound {
				cteSql = append(cteSql, c)
			}
		}
	}

	// make sql:
	// WITH cte array
	// SELECT main sql for calculation 1
	// WHERE run id IN (....)
	// AND dimension filters
	// UNION ALL
	// SELECT main sql for calculation 2
	// WHERE run id IN (....)
	// AND dimension filters
	// ORDER BY 1, 2,....

	sql := ""
	for k := range cteSql {
		if k > 0 {
			sql += ", " + cteSql[k]
		} else {
			sql += "WITH " + cteSql[k]
		}
	}
	for k := range mainSql {
		if k > 0 {
			sql = sql + " UNION ALL " + mainSql[k]
		} else {
			sql = sql + " " + mainSql[k]
		}
	}

	// append ORDER BY, default order by: run_id, expression id, dimensions
	sql += makeOrderBy(table.Rank, readLt.OrderBy, 2)

	return sql, nil
}
