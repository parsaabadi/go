// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"strconv"
)

type aggregationColumnExpr struct {
	colName string // column name, ie: ex2
	srcExpr string // source expression, ie: OM_AVG(acc0)
	sqlExpr string // sql expresiion, ie: AVG(M2.acc0)
}

// Parsed aggregation expressions for each nesting level
type levelDef struct {
	level          int                     // nesting level
	fromAlias      string                  // from table alias
	innerAlias     string                  // inner join table alias
	nextInnerAlias string                  // next level inner join table alias
	exprArr        []aggregationColumnExpr // column names and expressions
	firstAccIdx    int                     // first used accumulator index
	accUsageArr    []bool                  // contains true if accumulator used at current level
}

// level parse state
type levelParseState struct {
	levelDef                               // current level
	nextExprNumber int                     // number of aggregation epxpressions
	nextExprArr    []aggregationColumnExpr // aggregation expressions for the next level
}

// Translate output table accumulators calculation into sql query.
// Output column name outColName is optional.
func translateToAccSql(modelDef *ModelMeta, table *TableMeta, outColName string, layout *CalculateLayout, runIds []int) (string, error) {

	srcMsg := table.Name
	if outColName != "" {
		srcMsg += "." + outColName
	}

	// translate output table aggregation expression into sql query
	sql, err := transalteAggrToSql(table, outColName, layout.Calculate)
	if err != nil {
		return "", errors.New("Error at " + srcMsg + ": " + err.Error())
	}

	return sql, nil
}

// Translate output table aggregation expression into sql query.
// Only native accumulators allowed.
// Calculation must return a single value as a result of aggregation, ex.: AVG(acc_value).
// Output column name outColName is optional.
//
//	  SELECT
//	    M1.run_id,
//		   M1.dim0,
//		   M1.dim1,
//	    SUM(M1.acc_value + 0.5 * T2.ex2) AS OutputColName
//	  FROM age_acc M1
//	  INNER JOIN ........
//	  WHERE M1.acc_id = 0
//	  GROUP BY M1.run_id, M1.dim0, M1.dim1
func transalteAggrToSql(table *TableMeta, outColName string, calculateExpr string) (string, error) {

	// clean source calculation from cr lf and unsafe sql quotes
	// return error if unsafe sql or comment found outside of 'quotes', ex.: -- ; DELETE INSERT UPDATE...
	startExpr := cleanSourceExpr(calculateExpr)
	err := errorIfUnsafeSqlOrComment(startExpr)
	if err != nil {
		return "", err
	}

	// translate (substitute) all simple functions: OM_DIV_BY OM_IF...
	startExpr, err = translateAllSimpleFnc(startExpr)
	if err != nil {
		return "", err
	}

	// parse aggregation expression
	levelArr, err := parseAggrCalculation(table, outColName, startExpr)
	if err != nil {
		return "", err
	}

	// build output sql from parser state:
	//   SELECT
	//     M1.run_id, M1.dim0, M1.dim1,
	//     SUM(M1.acc_value + 0.5 * T2.ex2) AS OutputColName
	//   FROM age_acc M1
	//   INNER JOIN ........
	//   WHERE M1.acc_id = 0
	//   GROUP BY M1.run_id, M1.dim0, M1.dim1
	//
	sql, err := makeAggrSql(table, outColName, levelArr)
	if err != nil {
		return "", err
	}

	return sql, nil
}

// Build aggregation sql from parser state.
// Output column name outColName is optional.
func makeAggrSql(table *TableMeta, outColName string, levelArr []levelDef) (string, error) {

	// build output sql for expression:
	//
	// OM_SUM(acc0 + 0.5 * OM_AVG(acc1 + acc4 + 0.1 * (OM_MAX(acc0) - OM_MIN(acc1)) ))
	// =>
	//   SELECT
	//     M1.run_id, M1.dim0, M1.dim1,
	//     SUM(M1.acc_value + 0.5 * T2.ex2) AS OutputColName
	//   FROM age_acc M1
	//   INNER JOIN
	//   (
	//     SELECT
	//       M2.run_id, M2.dim0, M2.dim1,
	//       AVG(M2.acc_value + L2A4.acc4 + 0.1 * (T3.ex31 - T3.ex32)) AS ex2
	//     FROM age_acc M2
	//     INNER JOIN
	//     (
	//       SELECT run_id, dim0, dim1, sub_id, acc_value AS acc4 FROM age_acc WHERE acc_id = 4
	//     ) L2A4
	//     ON (L2A4.run_id = M2.run_id AND L2A4.dim0 = M2.dim0 AND L2A4.dim1 = M2.dim1 AND L2A4.sub_id = M2.sub_id)
	//     INNER JOIN
	//     (
	//       SELECT
	//         M3.run_id, M3.dim0, M3.dim1,
	//         MAX(M3.acc_value) AS ex31,
	//         MIN(L3A1.acc1)    AS ex32
	//       FROM age_acc M3
	//       INNER JOIN
	//       (
	//         SELECT run_id, dim0, dim1, sub_id, acc_value AS acc1 FROM age_acc WHERE acc_id = 1
	//       ) L3A1
	//       ON (L3A1.run_id = M3.run_id AND L3A1.dim0 = M3.dim0 AND L3A1.dim1 = M3.dim1 AND L3A1.sub_id = M3.sub_id)
	//       WHERE M3.acc_id = 0
	//       GROUP BY M3.run_id, M3.dim0, M3.dim1
	//     ) T3
	//     ON (T3.run_id = M2.run_id AND T3.dim0 = M2.dim0 AND T3.dim1 = M2.dim1)
	//     WHERE M2.acc_id = 1
	//     GROUP BY M2.run_id, M2.dim0, M2.dim1
	//   ) T2
	//   ON (T2.run_id = M1.run_id AND T2.dim0 = M1.dim0 AND T2.dim1 = M1.dim1)
	//   WHERE M1.acc_id = 0
	//   GROUP BY M1.run_id, M1.dim0, M1.dim1
	//
	sql := ""

	for nLev, lv := range levelArr {

		// select run_id, dim0,...,sub_id, acc_value
		// from accumulator table where acc_id = first accumulator
		//
		sql += "SELECT " + lv.fromAlias + ".run_id"

		for _, d := range table.Dim {
			sql += ", " + lv.fromAlias + "." + d.colName
		}

		for _, expr := range lv.exprArr {
			sql += ", " + expr.sqlExpr
			if expr.colName != "" {
				sql += " AS " + expr.colName
			}
		}

		sql += " FROM " + table.DbAccTable + " " + lv.fromAlias

		// INNER JON accumulator table for all other accumulators ON run_id, dim0,...,sub_id
		for nAcc, acc := range table.Acc {

			if !lv.accUsageArr[nAcc] || nAcc == lv.firstAccIdx { // skip first accumulator and unused accumulators
				continue
			}
			accAlias := "L" + strconv.Itoa(lv.level) + "A" + strconv.Itoa(nAcc)

			sql += " INNER JOIN (SELECT run_id, "

			for _, d := range table.Dim {
				sql += d.colName + ", "
			}

			sql += "sub_id, acc_value AS " + acc.colName +
				" FROM " + table.DbAccTable +
				" WHERE acc_id = " + strconv.Itoa(acc.AccId) +
				") " + accAlias

			sql += " ON (" + accAlias + ".run_id = " + lv.fromAlias + ".run_id"

			for _, d := range table.Dim {
				sql += " AND " + accAlias + "." + d.colName + " = " + lv.fromAlias + "." + d.colName
			}

			sql += " AND " + accAlias + ".sub_id = " + lv.fromAlias + ".sub_id)"
		}

		if nLev < len(levelArr)-1 { // if not lowest level then continue INNER JOIN down to the next level
			sql += " INNER JOIN ("
		}
	}

	// for each level except of the lowest append:
	//   WHERE acc_id = first accumulator id
	//   GROUP BY run_id, dim0,...
	//   ) ON (run_id, dim0,...)
	for nLev := len(levelArr) - 1; nLev >= 0; nLev-- {

		firstId := 0
		if levelArr[nLev].firstAccIdx >= 0 && levelArr[nLev].firstAccIdx < len(table.Acc) {
			firstId = table.Acc[levelArr[nLev].firstAccIdx].AccId
		}

		sql += " WHERE " + levelArr[nLev].fromAlias + ".acc_id = " + strconv.Itoa(firstId)

		sql += " GROUP BY " + levelArr[nLev].fromAlias + ".run_id"

		for _, d := range table.Dim {
			sql += ", " + levelArr[nLev].fromAlias + "." + d.colName
		}

		if nLev > 0 {

			sql += ") " + levelArr[nLev].innerAlias +
				" ON (" + levelArr[nLev].innerAlias + ".run_id = " + levelArr[nLev-1].fromAlias + ".run_id"

			for _, d := range table.Dim {
				sql += " AND " + levelArr[nLev].innerAlias + "." + d.colName + " = " + levelArr[nLev-1].fromAlias + "." + d.colName
			}

			sql += ")"
		}
	}

	return sql, nil
}

// Parse output table accumulators calculation.
// Output column name outColName is optional.
func parseAggrCalculation(table *TableMeta, outColName string, calculateExpr string) ([]levelDef, error) {

	// start with source expression and column name
	nLevel := 1
	level := levelDef{
		level:          nLevel,
		fromAlias:      "M" + strconv.Itoa(nLevel),
		innerAlias:     "T" + strconv.Itoa(nLevel),
		nextInnerAlias: "T" + strconv.Itoa(nLevel+1),
		exprArr: []aggregationColumnExpr{
			aggregationColumnExpr{
				colName: outColName,
				srcExpr: calculateExpr,
			}},
		accUsageArr: make([]bool, len(table.Acc)),
	}
	levelArr := []levelDef{level}

	lps := &levelParseState{
		levelDef:       level,
		nextExprNumber: 1,
		nextExprArr:    []aggregationColumnExpr{},
	}

	// until any function expressions exist on current level repeat translation:
	//
	//	OM_SUM(acc0 - 0.5 * OM_AVG(acc0))
	//  =>
	//  SUM(acc0 - 0.5 * T2.ex2)
	//	  => function: SUM(argument)
	//	  => argument: acc0 - 0.5 * OM_AVG(acc0)
	//	     => push OM_* functions as expression to the next level:
	//	        => current level sql column: OM_AVG(acc0) => T2.ex2
	//	        => next level expression:    OM_AVG(acc0)
	for {

		for nL := range level.exprArr {

			// parse until source expression not completed
			sqlExpr := level.exprArr[nL].srcExpr

			for {
				// find most left (top level) aggregation function
				fncName, namePos, arg, nAfter, err := findFirstFnc(sqlExpr, aggrFncLst)
				if err != nil {
					return []levelDef{}, err
				}
				if fncName == "" { // all done: no any functions found
					break
				}

				// translate (substitute) aggreagtion function by sql fragment
				t, err := lps.translateAggregationFnc(fncName, arg, sqlExpr)
				if err != nil {
					return []levelDef{}, err
				}

				// replace source
				if nAfter >= len(sqlExpr) {
					sqlExpr = sqlExpr[:namePos] + t
				} else {
					sqlExpr = sqlExpr[:namePos] + t + sqlExpr[nAfter:]
				}
			}
			level.exprArr[nL].sqlExpr = sqlExpr

			// accumultors first pass: collect accumulators usage is current sql expression
			var err error = nil

			nStart := 0
			for nEnd := 0; nStart >= 0 && nEnd >= 0; {

				nStart, nEnd, err = nextUnquoted(sqlExpr, nStart)
				if err != nil {
					return []levelDef{}, err
				}
				if nStart < 0 || nEnd < 0 { // end of source formula
					break
				}

				//  for each accumulator name check if name exist in that unquoted part of sql
				for k := 0; k < len(table.Acc); k++ {

					if findNamePos(sqlExpr[nStart:nEnd], table.Acc[k].Name) >= 0 {
						level.accUsageArr[k] = true
					}
				}

				nStart = nEnd // to the next 'unquoted part' of calculation string
			}
		}

		// accumulators second pass: translate accumulators for all sql expressions
		// replace accumulators names by column name in joined accumulator table:
		//   acc0 => M1.acc_value
		//   acc2 => L1A2.acc2
		for nL := range level.exprArr {

			var e error
			if level.exprArr[nL].sqlExpr, e = lps.processAccumulators(level.exprArr[nL].sqlExpr, table.Acc); e != nil {
				return []levelDef{}, e
			}
		}

		// if any expressions pushed to the next level then continue parsing
		if len(lps.nextExprArr) <= 0 {
			break
		}
		// else push aggregation expressions to the next level

		nLevel++
		level = levelDef{
			level:          nLevel,
			fromAlias:      "M" + strconv.Itoa(nLevel),
			innerAlias:     "T" + strconv.Itoa(nLevel),
			nextInnerAlias: "T" + strconv.Itoa(nLevel+1),
			exprArr:        append([]aggregationColumnExpr{}, lps.nextExprArr...),
			accUsageArr:    make([]bool, len(table.Acc)),
		}
		levelArr = append(levelArr, level)

		lps.levelDef = level
		lps.nextExprArr = []aggregationColumnExpr{}
	}

	return levelArr, nil
}

// push OM_ function to next aggregation level and return column name
func (lps *levelParseState) pushToNextLevel(fncExpr string) string {

	colName := "ex" + strconv.Itoa(lps.nextExprNumber)
	lps.nextExprNumber++

	lps.nextExprArr = append(lps.nextExprArr,
		aggregationColumnExpr{
			colName: colName,
			srcExpr: fncExpr,
		})
	return colName
}

// Translate accumulator names by inserting table alias.
// If this is the first accumulator at this level then do: acc1 => M2.acc_value
// else use joined accumulator table: L1A4.acc4
func (lps *levelParseState) processAccumulators(expr string, accRows []TableAccRow) (string, error) {

	// find index of first used native (not a derived) accumulator
	lps.firstAccIdx = -1
	for k, isUsed := range lps.accUsageArr {
		if isUsed && !accRows[k].IsDerived {
			lps.firstAccIdx = k
			break
		}
	}
	if lps.firstAccIdx < 0 {
		return expr, nil // return source expression as is: no accumulators used in that expression
	}

	// for each 'unquoted' part of expression check if there is any table accumulator name
	// substitute each accumulator name with corresponding sql column name
	var err error = nil
	nStart := 0

	for nEnd := 0; nStart >= 0 && nEnd >= 0; {

		nStart, nEnd, err = nextUnquoted(expr, nStart)
		if err != nil {
			return "", err
		}
		if nStart < 0 || nEnd < 0 { // end of source expression
			break
		}

		// substitute first occurence of accumulator name with sql column name
		// for example: acc0 => M1.acc_value or acc4 => L1A4.acc4
		isFound := false

		for k := 0; !isFound && k < len(accRows); k++ {

			if !lps.accUsageArr[k] || accRows[k].IsDerived { // only native accumulators can be aggregated
				continue
			}

			n := findNamePos(expr[nStart:nEnd], accRows[k].Name)
			if n >= 0 {
				isFound = true

				col := ""
				if k == lps.firstAccIdx {
					col = lps.fromAlias + "." + "acc_value"
				} else {
					col = "L" + strconv.Itoa(lps.level) + "A" + strconv.Itoa(k) + "." + accRows[k].Name
				}
				expr = expr[:nStart+n] + col + expr[nStart+n+len(accRows[k].Name):]
			}
		}

		if !isFound {
			nStart = nEnd // to the next 'unquoted part' of calculation string
		}
	}

	return expr, nil
}
