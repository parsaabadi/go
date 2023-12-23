// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"database/sql"
	"errors"
	"strconv"
)

const CALCULATED_ID_OFFSET = 1200 // calculated exprssion id offset, for example for Expr1 calculated expression id is 1201

// CalculateOutputTable read output table page (dimensions and values) and calculate extra measure(s).
//
// If calcLt.IsAggr true then do accumulator(s) aggregation else calculate expression value(s), ex: Expr1[variant] - Expr1[base].
func CalculateOutputTable(dbConn *sql.DB, modelDef *ModelMeta, tableLt *ReadCalculteTableLayout, runIds []int) (*list.List, *ReadPageLayout, error) {

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
	if len(tableLt.Calculation) <= 0 {
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
	q, err := translateTableCalcToSql(table, &tableLt.ReadLayout, tableLt.Calculation, runIds)
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
	// run_id, calculation id, dimension(s) enum ids, value null status
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

// CalculateMicrodata aggregates microdata using group by attributes as dimensions and calculate aggregated measure(s).
func CalculateMicrodata(dbConn *sql.DB, modelDef *ModelMeta, microLt *ReadCalculteMicroLayout, runIds []int) (*list.List, *ReadPageLayout, error) {

	// validate parameters
	if modelDef == nil {
		return nil, nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if microLt == nil {
		return nil, nil, errors.New("invalid (empty) microdata calculate layout")
	}
	if microLt.Name == "" {
		return nil, nil, errors.New("invalid (empty) microdata entity name")
	}
	if len(microLt.Calculation) <= 0 {
		return nil, nil, errors.New("invalid (empty) calculation expression(s)")
	}

	// find entity by name
	var entity *EntityMeta

	if k, ok := modelDef.EntityByName(microLt.Name); ok {
		entity = &modelDef.Entity[k]
	} else {
		return nil, nil, errors.New("entity not found: " + microLt.Name)
	}

	// find entity generation by entity id, as it is today model run has only one entity generation for each entity
	egLst, err := GetEntityGenList(dbConn, microLt.FromId)
	if err != nil {
		return nil, nil, errors.New("entity generation not found: " + microLt.Name + ": " + strconv.Itoa(microLt.FromId))
	}
	var entityGen *EntityGenMeta

	for k := range egLst {

		if egLst[k].EntityId == entity.EntityId {
			entityGen = &egLst[k]
			break
		}
	}
	if entityGen == nil {
		return nil, nil, errors.New("Error: entity generation not found: " + microLt.Name + ": " + strconv.Itoa(microLt.FromId))
	}

	// find group by microdata attributes by name
	aGroupBy := make([]EntityAttrRow, len(microLt.GroupBy))

	for _, ga := range entityGen.GenAttr {

		aIdx, ok := entity.AttrByKey(ga.AttrId)
		if !ok {
			return nil, nil, errors.New("entity attribute not found by id: " + strconv.Itoa(ga.AttrId) + " " + microLt.Name)
		}
		for j := range microLt.GroupBy {
			if microLt.GroupBy[j] == entity.Attr[aIdx].Name {
				aGroupBy[j] = entity.Attr[aIdx]
				break
			}
		}
	}

	// check: all group by attributes must be found and it must boolean or not built-in
	for k := range aGroupBy {
		if aGroupBy[k].ModelId <= 0 || aGroupBy[k].Name == "" || aGroupBy[k].colName == "" || aGroupBy[k].typeOf == nil {
			return nil, nil, errors.New("entity group by attribute not found by: " + microLt.Name + "." + microLt.GroupBy[k])
		}
		if aGroupBy[k].typeOf.IsBuiltIn() && !aGroupBy[k].typeOf.IsBool() {
			return nil, nil, errors.New("invalid type of entity group by attribute not found by: " + microLt.Name + "." + microLt.GroupBy[k] + " : " + aGroupBy[k].typeOf.Name)
		}
	}

	// translate calculation to sql
	q, err := translateMicroToSql(entity, entityGen, &microLt.ReadLayout, &microLt.CalculateMicroLayout, runIds)
	if err != nil {
		return nil, nil, err
	}

	// prepare db-row scan conversion buffer: run_id, calculation id, group by attributes, value
	// and define conversion function to make new cell from scan buffer
	scanBuf, fc, err := scanSqlRowToCellMicroCalc(entity, aGroupBy)
	if err != nil {
		return nil, nil, err
	}

	// select cells:
	// run_id, calculation id, group by attributes, value and null status
	cLst, lt, err := SelectToList(dbConn, q, microLt.ReadPageLayout,
		func(rows *sql.Rows) (interface{}, error) {

			if e := rows.Scan(scanBuf...); e != nil {
				return nil, e
			}

			// make new cell from conversion buffer
			c := CellMicroCalc{Attr: make([]attrValue, len(aGroupBy)+1)}

			if e := fc(&c); e != nil {
				return nil, e
			}

			return c, nil
		})
	if err != nil {
		return nil, nil, err
	}

	return cLst, lt, nil
}

// Translate all microdata aggregations to sql query, apply group by, dimension filters, selected run id's and order by.
// It can be a multiple runs comparison and base run id is layout.FromId.
// Or simple aggreagtion inside of single run, in that case layout.FromId and runIds[] are merged.
func translateMicroToSql(entity *EntityMeta, entityGen *EntityGenMeta, readLt *ReadLayout, calcLt *CalculateMicroLayout, runIds []int) (string, error) {

	// translate each calculation to sql: CTE and main sql query
	mainSql := []string{}
	isRunCompare := false

	aggrCols, err := makeMicroAggrCols(entity, entityGen, calcLt.GroupBy)
	if err != nil {
		return "", err
	}

	for k := range calcLt.Calculation {

		mSql := ""
		isCmp := false

		mSql, isCmp, err = partialTranslateToMicroSql(entity, entityGen, aggrCols, readLt, &calcLt.Calculation[k], runIds)
		if err != nil {
			return "", err
		}
		if k == 0 {
			isRunCompare = isCmp
		} else {
			if isCmp != isRunCompare {
				return "", errors.New("Error at " + entity.Name + " " + calcLt.Calculation[k].Calculate + ": " + "invalid (or mixed forms) of attribute names used")
			}
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
	}

	cteSql, err := makeMicroCteAggrSql(entity, entityGen, aggrCols, readLt.FromId, runIds, isRunCompare)
	if err != nil {
		return "", errors.New("Error at making CTE for aggregation of " + entity.Name + ": " + err.Error())
	}

	// make sql:
	// WITH cte array
	// SELECT main sql for aggregation 1
	// WHERE attribute filters
	// UNION ALL
	// SELECT main sql for aggregation 2
	// WHERE attribute filters
	// ORDER BY 1, 2,....

	sql := cteSql

	for k := range mainSql {
		if k > 0 {
			sql = sql + " UNION ALL " + mainSql[k]
		} else {
			sql = sql + " " + mainSql[k]
		}
	}

	// append ORDER BY, default order by: run_id, calculation id, group by attributes
	sql += makeOrderBy(len(calcLt.GroupBy), readLt.OrderBy, 2)

	return sql, nil
}

// prepare to scan sql rows and convert each row to CellMicroCalc
// retun scan buffer to be popualted by rows.Scan() and closure to that buffer into CellMicroCalc
func scanSqlRowToCellMicroCalc(entity *EntityMeta, aGroupBy []EntityAttrRow) ([]interface{}, func(*CellMicroCalc) error, error) {

	nGrp := len(aGroupBy)
	scanBuf := make([]interface{}, 3+nGrp) // run id, calculation id, group by attributes, calculated value

	var runId, calcId int
	scanBuf[0] = &runId
	scanBuf[1] = &calcId

	fd := make([]func(interface{}) (attrValue, error), nGrp+1) // conversion functions for group by attributes

	// for each attribute create conversion function by type
	for na, ga := range aGroupBy {

		switch {
		case ga.typeOf.IsBool(): // logical attribute

			var v interface{}
			scanBuf[2+na] = &v

			fd[na] = func(src interface{}) (attrValue, error) {

				av := attrValue{}
				av.IsNull = false // logical attribute expected to be NOT NULL

				is := false
				switch vn := v.(type) {
				case nil: // 2018: unexpected today, may be in the future
					is = false
					av.IsNull = true
				case bool:
					is = vn
				case int64:
					is = vn != 0
				case uint64:
					is = vn != 0
				case int32:
					is = vn != 0
				case uint32:
					is = vn != 0
				case int16:
					is = vn != 0
				case uint16:
					is = vn != 0
				case int8:
					is = vn != 0
				case uint8:
					is = vn != 0
				case uint:
					is = vn != 0
				case float32: // oracle (very unlikely)
					is = vn != 0.0
				case float64: // oracle (often)
					is = vn != 0.0
				case int:
					is = vn != 0
				default:
					return av, errors.New("invalid attribute value type, integer expected: " + entity.Name + "." + ga.Name)
				}
				av.Value = is

				return av, nil
			}

		case ga.typeOf.IsString(): // string attribute, as it is today strings are not microdata dimenions

			return nil, nil, errors.New("invalid group by attribute type: " + ga.typeOf.Name + " : " + entity.Name + "." + ga.Name)

		case ga.typeOf.IsFloat(): // float attribute, can be NULL, as it is today floats cannot are not microdata dimenions

			return nil, nil, errors.New("invalid group by attribute type: " + ga.typeOf.Name + " : " + entity.Name + "." + ga.Name)

		default:
			var v interface{}
			scanBuf[2+na] = &v

			fd[na] = func(src interface{}) (attrValue, error) { return attrValue{IsNull: src == nil, Value: v}, nil }
		}
	}

	// calculated value expected to be float
	var vf sql.NullFloat64
	scanBuf[2+nGrp] = &vf

	fd[nGrp] = func(src interface{}) (attrValue, error) {

		if src == nil {
			return attrValue{IsNull: true}, nil
		}
		if vf.Valid {
			return attrValue{IsNull: false, Value: vf.Float64}, nil
		}
		return attrValue{IsNull: true, Value: 0.0}, nil
	}

	// sql row conevrsion function: convert (run_id, calc_id, group by attributes, calc_value) from scan buffer to cell
	cvt := func(c *CellMicroCalc) error {

		c.RunId = runId
		c.CalcId = calcId

		for k := 0; k < nGrp+1; k++ {
			v, e := fd[k](scanBuf[2+k])
			if e != nil {
				return e
			}
			c.Attr[k] = v
		}
		return nil
	}

	return scanBuf, cvt, nil
}
