// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"strconv"
	"strings"
)

// Translate output table expression calculation to sql query.
// Only simple functions allowed in expression calculation.
// Output column name outColName is optional.
func translateToExprSql(modelDef *ModelMeta, table *TableMeta, outColName string, layout *CalculateLayout, runIds []int) (string, error) {

	srcMsg := table.Name
	if outColName != "" {
		srcMsg += "." + outColName
	}

	// clean source calculation from cr lf and unsafe sql quotes
	// return error if unsafe sql or comment found outside of 'quotes', ex.: -- ; DELETE INSERT UPDATE...
	expr := cleanSourceExpr(layout.Calculate)
	if err := errorIfUnsafeSqlOrComment(expr); err != nil {
		return "", errors.New("Error at " + srcMsg + ": " + err.Error())
	}

	// translate (substitute) all simple functions: OM_DIV_BY OM_IF...
	expr, err := translateAllSimpleFnc(expr)
	if err != nil {
		return "", errors.New("Error at " + srcMsg + ": " + err.Error())
	}

	// make sql column names as src0,...,srcN and make sure column names are different from expression names
	exprCount := len(table.Expr)
	srcCols := make([]string, exprCount)

	nU := 0
	for isFound := true; isFound; {
		isFound = false

		for k := 0; !isFound && k < exprCount; k++ {
			srcCols[k] = "src" + strings.Repeat("_", nU) + strconv.Itoa(k)
			for j := 0; !isFound && j < exprCount; j++ {
				isFound = srcCols[k] == table.Expr[j].Name
			}
		}
		if isFound { // column name exist as expression name: use _ undescore to create unique names
			nU++
		}
	}

	// find expression names:
	// it can be Expr0[base] and Expr0[variant],... or just Expr0, Expr1,... without [base] and [variant]
	baseNames := make([]string, exprCount)
	varNames := make([]string, exprCount)
	nameUsage := make([]bool, exprCount)
	baseUsage := make([]bool, exprCount)
	varUsage := make([]bool, exprCount)

	for k := 0; k < exprCount; k++ {
		baseNames[k] = table.Expr[k].Name + "[base]"
		varNames[k] = table.Expr[k].Name + "[variant]"
	}

	// for each 'unquoted' part of formula check if there is any table expression name
	// substitute each table expression name with corresponding sql column name
	/*
		If this is base and variant expression:
			(Expr1[base] + Expr1[variant] + Expr0[variant]) / OM_DIV_BY(Expr0[base])
				==>
			(Expr1[base] + Expr1[variant] + Expr0[variant]) / CASE WHEN ABS(Expr0[base]) > 1.0e-37 THEN Expr0[base] ELSE NULL END
				==>
			(B1.src1 + V1.src1 + V.src0) / CASE WHEN ABS(B.src0) > 1.0e-37 THEN B.src0 ELSE NULL END
		Or single run expression (no base and varinat):
			Expr0 + Expr1
				==>
		    B.src0 + B1.src1
	*/
	isAnyBase := false
	isAnyVar := false
	isSrcOnly := false
	baseMinIdx := -1
	varMinIdx := -1

	nStart := 0
	for nEnd := 0; nStart >= 0 && nEnd >= 0; {

		if nStart, nEnd, err = nextUnquoted(expr, nStart); err != nil {
			return "", errors.New("Error at " + srcMsg + ": " + err.Error())
		}
		if nStart < 0 || nEnd < 0 { // end of source formula
			break
		}

		// substitute all occurences of base expression name with sql column from base or variant CTE
		// for example: Expr1[base] ==> B1.src1
		isFound := false

		for k := 0; !isFound && k < exprCount; k++ {

			n := findNamePos(expr[nStart:nEnd], baseNames[k])
			if n >= 0 {
				isFound = true
				isAnyBase = true
				baseUsage[k] = true
				nameUsage[k] = true

				col := ""
				if baseMinIdx < 0 {
					baseMinIdx = k
					col = "B." + srcCols[k]
				} else {
					col = "B" + strconv.Itoa(k) + "." + srcCols[k]
				}
				expr = expr[:nStart] + strings.ReplaceAll(expr[nStart:nEnd], baseNames[k], col) + expr[nEnd:]
			}
		}

		// substitute all occurences of variant expression name with sql column from base or variant CTE
		// for example: Expr1[variant] ==> V1.src1
		for k := 0; !isFound && k < exprCount; k++ {

			n := findNamePos(expr[nStart:nEnd], varNames[k])
			if n >= 0 {
				isFound = true
				isAnyVar = true
				varUsage[k] = true
				nameUsage[k] = true

				col := ""
				if varMinIdx < 0 {
					varMinIdx = k
					col = "V." + srcCols[k]
				} else {
					col = "V" + strconv.Itoa(k) + "." + srcCols[k]
				}
				expr = expr[:nStart] + strings.ReplaceAll(expr[nStart:nEnd], varNames[k], col) + expr[nEnd:]
			}
		}

		// substitute all occurences of source expression name with sql column from base or variant CTE
		// for example: Expr1 ==> B1.src1
		for k := 0; !isFound && k < exprCount; k++ {
			n := findNamePos(expr[nStart:nEnd], table.Expr[k].Name)
			if n >= 0 {
				isFound = true
				isSrcOnly = true
				nameUsage[k] = true

				col := ""
				if baseMinIdx < 0 {
					baseMinIdx = k
					col = "B." + srcCols[k]
				} else {
					col = "B" + strconv.Itoa(k) + "." + srcCols[k]
				}
				expr = expr[:nStart] + strings.ReplaceAll(expr[nStart:nEnd], table.Expr[k].Name, col) + expr[nEnd:]
			}
		}

		if !isFound {
			nStart = nEnd // to the next 'unquoted part' of calculation string
		}
	}

	// all names must be either with suffixes: Expr0[base], Expr0[variant] or in simple form: Expr0, Expr1
	// [base] and [variant] forms must be used, it cannot be only [base] or only [variant]
	if isSrcOnly && (isAnyBase || isAnyVar) ||
		!isSrcOnly && (isAnyBase && !isAnyVar || !isAnyBase && isAnyVar) ||
		(baseMinIdx < 0 || baseMinIdx >= exprCount) ||
		!isSrcOnly && (varMinIdx < 0 || varMinIdx >= exprCount) {
		return expr, errors.New("Error at " + srcMsg + ": invalid (or mixed forms) of expression names used in: " + layout.Calculate)
	}
	if !isSrcOnly && !isAnyBase && !isAnyVar {
		return expr, errors.New("Error at " + srcMsg + ": there are no expression names found in: " + layout.Calculate)
	}

	/*
		WITH cs0 (run_id, dim0, dim1, src0) AS
		(
			SELECT
				BR.run_id, C.dim0, C.dim1, C.expr_value
			FROM tableName C
			INNER JOIN run_table BR ON (BR.base_run_id = C.run_id AND BR.table_hid = 118)
			WHERE C.expr_id = 0
		),
		cs1 (run_id, dim0, dim1, src1) AS
		(
			SELECT
				BR.run_id, C.dim0, C.dim1, C.expr_value
			FROM tableName C
			INNER JOIN run_table BR ON (BR.base_run_id = C.run_id AND BR.table_hid = 118)
			WHERE C.expr_id = 1
		),
		cr (run_id, dim0, dim1, val) AS
		(
			SELECT
				V.run_id, V.dim0, V.dim1,
				(B1.src1 + V1.src1 + V.src0) / CASE WHEN ABS(B.src0) > 1.0e-37 THEN B.src0 ELSE NULL END
			FROM cs0 B
			INNER JOIN cs1 B1 ON (B1.run_id = B.run_id AND B1.dim0 = B.dim0 AND B1.dim1 = B.dim1)
			INNER JOIN cs0 V ON (V.dim0 = B.dim0 AND V.dim1 = B.dim1)
			INNER JOIN cs1 V1 ON (V1.run_id = V.run_id AND V1.dim0 = B.dim0 AND V1.dim1 = B.dim1)
			WHERE B.run_id = 102
		)
		SELECT
			C.run_id, C.dim0, C.dim1, C.val AS Result
		FROM cr C
		WHERE C.run_id IN (103, 104, 105, 106, 107, 108, 109, 110, 111, 112)
		ORDER BY 1, 2, 3
	*/

	// make CTE column names
	cteHdrCols := "run_id"
	cteBodyCols := "BR.run_id"

	for _, d := range table.Dim {
		cteHdrCols += ", " + d.colName
		cteBodyCols += ", C." + d.colName
	}
	cteBodyCols += ", C.expr_value"

	// add CTEs for source expressions
	sql := ""

	for k, isUsed := range nameUsage {
		if !isUsed {
			continue
		}
		sIdx := strconv.Itoa(k)

		if sql == "" {
			sql += "WITH "
		} else {
			sql += ", "
		}
		sql += "cs" + sIdx + " (" + cteHdrCols + ", " + srcCols[k] + ") AS" +
			" (" +
			"SELECT " + cteBodyCols + " FROM " + table.DbExprTable + " C" +
			" INNER JOIN run_table BR ON (BR.base_run_id = C.run_id AND BR.table_hid = " + strconv.Itoa(table.TableHid) + ")" +
			" WHERE C.expr_id = " + strconv.Itoa(table.Expr[k].ExprId) +
			")"
	}

	// add CTE(s) for value calculation
	bvAlias := "B"
	if !isSrcOnly {
		bvAlias = "V"
	}

	sql += ", cr (run_id"
	for _, d := range table.Dim {
		sql += ", " + d.colName
	}
	sql += ", val) AS (SELECT " + bvAlias + ".run_id"

	for _, d := range table.Dim {
		sql += ", " + bvAlias + "." + d.colName
	}
	sql += ", " + expr +
		" FROM cs" + strconv.Itoa(baseMinIdx) + " B"

	if isSrcOnly {

		// INNER JOIN cs1 B1 ON (B1.run_id = B.run_id AND B1.dim0 = B.dim0 AND B1.dim1 = B.dim1)
		for k := 0; k < exprCount; k++ {
			if k != baseMinIdx && nameUsage[k] {
				alias := "B" + strconv.Itoa(k)
				sql += " INNER JOIN cs" + strconv.Itoa(k) + " " + alias + " ON (" + alias + ".run_id = B.run_id"
				for _, d := range table.Dim {
					sql += " AND " + alias + "." + d.colName + " = B." + d.colName
				}
				sql += ")"
			}
		}
	} else {

		// INNER JOIN cs1 B1 ON (B1.run_id = B.run_id AND B1.dim0 = B.dim0 AND B1.dim1 = B.dim1)
		for k := 0; k < exprCount; k++ {
			if k != baseMinIdx && baseUsage[k] {
				alias := "B" + strconv.Itoa(k)
				sql += " INNER JOIN cs" + strconv.Itoa(k) + " " + alias + " ON (" + alias + ".run_id = B.run_id"
				for _, d := range table.Dim {
					sql += " AND " + alias + "." + d.colName + " = B." + d.colName
				}
				sql += ")"
			}
		}

		// INNER JOIN cs0 V ON (V.dim0 = B.dim0 AND V.dim1 = B.dim1)
		sql += " INNER JOIN cs" + strconv.Itoa(varMinIdx) + " V ON ("
		for k, d := range table.Dim {
			if k > 0 {
				sql += " AND "
			}
			sql += "V." + d.colName + " = B." + d.colName
		}
		sql += ")"

		// INNER JOIN cs1 V1 ON (V1.run_id = V.run_id AND V1.dim0 = B.dim0 AND V1.dim1 = B.dim1)
		for k := 0; k < exprCount; k++ {
			if k != varMinIdx && varUsage[k] {
				alias := "V" + strconv.Itoa(k)
				sql += " INNER JOIN cs" + strconv.Itoa(k) + " " + alias + " ON (" + alias + ".run_id = B.run_id"
				for _, d := range table.Dim {
					sql += " AND " + alias + "." + d.colName + " = B." + d.colName
				}
				sql += ")"
			}
		}

		sql += " WHERE B.run_id = " + strconv.Itoa(layout.FromId)
	}

	sql += ")"

	// build main body select sql
	sql += " SELECT C.run_id"
	for _, d := range table.Dim {
		sql += ", C." + d.colName
	}
	sql += ", C.val"
	if outColName != "" {
		sql += " AS " + outColName // output cloumn name is optional
	}
	sql +=
		" FROM cr C" +
			" WHERE C.run_id IN ("
	if isSrcOnly {
		isFound := false
		for k := 0; !isFound && k < len(runIds); k++ {
			isFound = runIds[k] == layout.FromId
		}
		if !isFound {
			sql += strconv.Itoa(layout.FromId) + ", "
		}
	}
	for k := 0; k < len(runIds); k++ {
		if k > 0 {
			sql += ", "
		}
		sql += strconv.Itoa(runIds[k])
	}
	sql += ")"

	// append dimension enum code filters, if specified
	for k := range layout.Filter {

		// find dimension index by name
		dix := -1
		for j := range table.Dim {
			if table.Dim[j].Name == layout.Filter[k].Name {
				dix = j
				break
			}
		}
		if dix < 0 {
			return "", errors.New("Error at " + srcMsg + ": output table " + table.Name + " does not have dimension " + layout.Filter[k].Name)
		}

		f, err := makeWhereFilter(
			&layout.Filter[k], "C", table.Dim[dix].colName, table.Dim[dix].typeOf, table.Dim[dix].IsTotal, table.Dim[dix].Name, "output table "+table.Name)
		if err != nil {
			return "", errors.New("Error at " + srcMsg + ": " + err.Error())
		}
		sql += " AND " + f
	}

	// append dimension enum id filters, if specified
	for k := range layout.FilterById {

		// find dimension index by name
		dix := -1
		for j := range table.Dim {
			if table.Dim[j].Name == layout.FilterById[k].Name {
				dix = j
				break
			}
		}
		if dix < 0 {
			return "", errors.New("Error at " + srcMsg + ": output table " + table.Name + " does not have dimension " + layout.FilterById[k].Name)
		}

		f, err := makeWhereIdFilter(
			&layout.FilterById[k], "C", table.Dim[dix].colName, table.Dim[dix].typeOf, table.Dim[dix].Name, "output table "+table.Name)
		if err != nil {
			return "", errors.New("Error at " + srcMsg + ": " + err.Error())
		}

		sql += " AND " + f
	}

	// append ORDER BY, default order by: run_id, dimensions
	sql += makeOrderBy(table.Rank, layout.OrderBy, 1)

	return sql, nil
}
