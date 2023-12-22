// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"strconv"
)

// Translate microdata aggregation into sql query.
func translateToMicroSql(
	entity *EntityMeta, entityGen *EntityGenMeta, readLt *ReadLayout, calcLt *CalculateLayout, groupBy []string, runIds []int,
) (string, error) {

	// make sql:
	// WITH cte array
	// SELECT main sql for calculation
	// WHERE run id IN (....)
	// AND dimension filters
	// ORDER BY 1, 2,....
	// default order by: run_id, calculation id, group by attributes

	aggrCols, err := makeMicroAggrCols(entity, entityGen, groupBy)
	if err != nil {
		return "", err
	}

	mainSql, isRunCompare, err := partialTranslateToMicroSql(entity, entityGen, aggrCols, readLt, calcLt, runIds)
	if err != nil {
		return "", err
	}
	cteSql, err := makeMicroCteAggrSql(entity, entityGen, aggrCols, readLt.FromId, runIds, isRunCompare)
	if err != nil {
		return "", errors.New("Error at " + entity.Name + " " + calcLt.Calculate + ": " + err.Error())
	}

	return cteSql + " " + mainSql + makeOrderBy(len(groupBy), readLt.OrderBy, 2), nil
}

// create list of microdata columns, set column names, group by flag and aggregatable flag for float and int attributes
func makeMicroAggrCols(entity *EntityMeta, entityGen *EntityGenMeta, groupBy []string) ([]aggrColulumn, error) {

	// aggregation expression columns: only native numeric attributes can be aggregated
	aggrCols := make([]aggrColulumn, len(entityGen.GenAttr))

	for k := range entityGen.GenAttr {

		// find entity attribute
		aIdx, ok := entity.AttrByKey(entityGen.GenAttr[k].AttrId)
		if !ok {
			return []aggrColulumn{}, errors.New("model entity attribute not found by id: " + entity.Name + ": " + strconv.Itoa(entityGen.GenAttr[k].AttrId))
		}

		isGroup := false
		for j := 0; !isGroup && j < len(groupBy); j++ {
			isGroup = groupBy[j] == entity.Attr[aIdx].Name
		}

		aggrCols[k] = aggrColulumn{
			name:    entity.Attr[aIdx].Name,
			colName: entity.Attr[aIdx].colName,
			isGroup: isGroup,
			isAggr:  entity.Attr[aIdx].typeOf.IsFloat() || entity.Attr[aIdx].typeOf.IsInt(), // only numeric attributes can be aggregated
		}
	}

	// validate group by attributes
	for k := range groupBy {

		isOk := false
		for j := 0; !isOk && j < len(aggrCols); j++ {
			isOk = groupBy[k] == aggrCols[j].name
		}
		if !isOk {
			return []aggrColulumn{}, errors.New("Entity " + entity.Name + " does not have attribute " + groupBy[k])
		}
	}

	return aggrCols, nil
}

// Translate microdata aggregation to sql query, apply dimension filters and selected run id's.
// Return main sql and run comparison flag.
//
// It can be a multiple runs comparison and base run id is layout.FromId
// Or simple expression calculation inside of single run, in that case layout.FromId and runIds[] are merged.
// Only simple functions allowed in expression calculation.
func partialTranslateToMicroSql(
	entity *EntityMeta, entityGen *EntityGenMeta, aggrCols []aggrColulumn, readLt *ReadLayout, calcLt *CalculateLayout, runIds []int,
) (
	string, bool, error,
) {

	// translate microdata aggregation expression into sql query
	//
	// no comparison, aggregation for each run: OM_SUM(Income - 0.5 * OM_AVG(Pension))
	//
	// WITH abase (run_id, entity_key, attr1, attr2, attr3, attr8) AS (.... WHERE RE.run_id IN (219, 221, 222))
	// SELECT
	//   A.run_id, CalcId AS calc_id, A.attr1, A.attr2, A.calc_value
	// FROM
	// (
	//   SELECT
	//     M1.run_id, M1.attr1, M1.attr2,
	//     SUM(M1.attr3 - 0.5 * L1E1.ex1) AS calc_value
	//   FROM abase M1
	//   INNER JOIN ( .... ) ON (run_id, attr1, attr2)
	//   GROUP BY M1.run_id, M1.attr1, M1.attr2
	// ) A
	// WHERE A.attr1 = .....
	//
	// microdata run comparison: OM_AVG( Income[varinat] - (Pension[base] + Salary[base]) )
	//
	// WITH abase (run_id, entity_key, attr1, attr2, attr4, attr8)   AS (.... WHERE RE.run_id = 219),
	//      avar  (run_id, entity_key, attr1, attr2, attr3)          AS (.... WHERE RE.run_id IN (221, 222)),
	// abv (run_id, attr1, attr2, attr4_base, attr8_base, attr3_var) AS (....  FROM abase B INNER JOIN avar V ON (V.entity_key = B.entity_key) )
	// SELECT
	//   A.run_id, CalcId AS calc_id, A.attr1, A.attr2, A.calc_value
	// FROM
	// (
	//   SELECT
	//     M1.run_id, M1.attr1, M1.attr2,
	//     AVG(M1.attr3_var - (M1.attr8_base + M1.attr4_base)) AS calc_value
	//   INNER JOIN ( .... ) ON (run_id, attr1, attr2)
	//   FROM abv M1
	//   GROUP BY M1.run_id, M1.attr1, M1.attr2
	// ) A
	// WHERE A.attr1 = .....
	//
	mainSql, isRunCompare, err := translateMicroCalcToSql(entity, entityGen, aggrCols, calcLt.CalcId, calcLt.Calculate)
	if err != nil {
		return "", false, errors.New("Error at " + entity.Name + " " + calcLt.Calculate + ": " + err.Error())
	}

	// append attribute enum code filters, if specified: A.attr1 =....
	where := ""

	for k := range readLt.Filter {

		// find attribute index by name
		aix := -1
		for j := range entity.Attr {
			if entity.Attr[j].Name == readLt.Filter[k].Name {
				aix = j
				break
			}
		}
		if aix < 0 {
			return "", false, errors.New("Error at " + entity.Name + " " + calcLt.Calculate + ": entity " + entity.Name + " does not have attribute " + readLt.Filter[k].Name)
		}

		f, err := makeWhereFilter(
			&readLt.Filter[k], "A", entity.Attr[aix].colName, entity.Attr[aix].typeOf, false, entity.Attr[aix].Name, "entity "+entity.Name)
		if err != nil {
			return "", false, errors.New("Error at " + entity.Name + " " + calcLt.Calculate + ": " + err.Error())
		}

		if where == "" {
			where = f
		} else {
			where += " AND " + f
		}
	}

	// append attribute enum id filters, if specified
	for k := range readLt.FilterById {

		// find attribute index by name
		aix := -1
		for j := range entity.Attr {
			if entity.Attr[j].Name == readLt.FilterById[k].Name {
				aix = j
				break
			}
		}
		if aix < 0 {
			return "", false, errors.New("Error at " + entity.Name + " " + calcLt.Calculate + ": entity " + entity.Name + " does not have attribute " + readLt.FilterById[k].Name)
		}

		f, err := makeWhereIdFilter(
			&readLt.FilterById[k], "A", entity.Attr[aix].colName, entity.Attr[aix].typeOf, entity.Attr[aix].Name, "entity "+entity.Name)
		if err != nil {
			return "", false, errors.New("Error at " + entity.Name + " " + calcLt.Calculate + ": " + err.Error())
		}

		if where == "" {
			where = f
		} else {
			where += " AND " + f
		}
	}

	// append WHERE to main sql query if where filters not empty
	if where != "" {
		mainSql += " WHERE " + where
	}

	return mainSql, isRunCompare, nil
}

// Translate microdata aggregation into main sql query.
// Calculation must return a single value as a result of aggregation, ex.: AVG(attr1).
//
// Return sql SELECT for value calculation and run comparison flag:
// if true then it is multiple runs comparison else expression calculation inside of a single run(s).
//
// It aggregation for one or multiple runs: OM_SUM(Income - 0.5 * OM_AVG(Pension))
//
//	WITH abase (run_id, entity_key, attr1, attr2, attr3, attr8) AS (.... WHERE RE.run_id IN (219, 221, 222))
//	SELECT
//	  A.run_id, CalcId AS calc_id, A.attr1, A.attr2, A.calc_value
//	FROM
//	(...., SUM(M1.attr3 - 0.5 * L1E1.ex1) AS calc_value
//	  GROUP BY M1.run_id, M1.attr1, M1.attr2
//	) A
//
// microdata run comparison: OM_AVG( Income[varinat] - (Pension[base] + Salary[base]) )
//
//	WITH abase (run_id, entity_key, attr1, attr2, attr4, attr8)   AS (.... WHERE RE.run_id = 219),
//	     avar  (run_id, entity_key, attr1, attr2, attr3)          AS (.... WHERE RE.run_id IN (221, 222)),
//	abv (run_id, attr1, attr2, attr4_base, attr8_base, attr3_var) AS (....  FROM abase B INNER JOIN avar V ON (V.entity_key = B.entity_key) )
//	SELECT
//	  A.run_id, CalcId AS calc_id, A.attr1, A.attr2, A.calc_value
//	FROM
//	(...., AVG(M1.attr3_var - (M1.attr8_base + M1.attr4_base)) AS calc_value
//	  FROM abv M1
//	  GROUP BY M1.run_id, M1.attr1, M1.attr2
//	) A
func translateMicroCalcToSql(
	entity *EntityMeta, entityGen *EntityGenMeta, aggrCols []aggrColulumn, calcId int, calculateExpr string,
) (
	string, bool, error,
) {

	// clean source calculation from cr lf and unsafe sql quotes
	// return error if unsafe sql or comment found outside of 'quotes', ex.: -- ; DELETE INSERT UPDATE...
	// clean source calculation from cr lf and unsafe sql quotes
	// return error if unsafe sql or comment found outside of 'quotes', ex.: -- ; DELETE INSERT UPDATE...
	startExpr := cleanSourceExpr(calculateExpr)
	err := errorIfUnsafeSqlOrComment(startExpr)
	if err != nil {
		return "", false, err
	}

	// translate (substitute) all simple functions: OM_DIV_BY OM_IF...
	startExpr, err = translateAllSimpleFnc(startExpr)
	if err != nil {
		return "", false, err
	}

	// produce attribute column name:
	// if it is not a run comparison:      attr3 => L1A4.attr3
	// or for base and variant attributes: L1A4.attr3_var, L1A4.attr4_base, L1A4.attr8_base
	// length of
	isAnySimple := false
	isAnyBase := false
	isAnyVar := false

	makeAttrColName := func(
		name string, nameIdx int, isSimple, isVar bool, firstAlias string, levelAlias string, isFirst bool,
	) string {

		if !isSimple {
			if isVar {
				isAnyVar = true
				aggrCols[nameIdx].isVar = true
				return firstAlias + "." + aggrCols[nameIdx].colName + "_var" // variant run attribute: attr1[variant] => L1A4.attr1_var
			}
			isAnyBase = true
			aggrCols[nameIdx].isBase = true
			return firstAlias + "." + aggrCols[nameIdx].colName + "_base" // base run attribute: attr1[base] => L1A4.attr1_base
		}
		// else: isSimple name, not a name[base] or name[variant]
		isAnySimple = true
		aggrCols[nameIdx].isBase = true
		return firstAlias + "." + aggrCols[nameIdx].colName // not a run comparison: attr2 => L1A4.attr2
	}

	// parse aggregation expression
	levelArr, err := parseAggrCalculation(aggrCols, startExpr, makeAttrColName)
	if err != nil {
		return "", false, err
	}

	// validate attribute names:
	// all names must be either with suffixes: attr3[base], attr4[variant] or in simple form: attr3, attr4
	// if it is run comparison then both [base] and [variant] forms must be used, it cannot be only [base] or only [variant]

	if isAnySimple && (isAnyBase || isAnyVar) ||
		!isAnySimple && (isAnyBase && !isAnyVar || !isAnyBase && isAnyVar) {
		return "", false, errors.New("invalid (or mixed forms) of attribute names used for aggregation of: " + entity.Name + ": " + calculateExpr)
	}
	if !isAnySimple && !isAnyBase && !isAnyVar {
		return "", false, errors.New("error: there are no attribute names found for aggregation of: " + entity.Name + ": " + calculateExpr)
	}
	isCompare := isAnyBase && isAnyVar

	// build main part of aggregation sql from parser state
	mainSql, err := makeMicroMainAggrSql(entity, entityGen, aggrCols, calcId, levelArr, isCompare)
	if err != nil {
		return "", false, err
	}

	return mainSql, isCompare, nil
}

// Build main part of aggregation sql from parser state.
func makeMicroMainAggrSql(
	entity *EntityMeta, entityGen *EntityGenMeta, aggrCols []aggrColulumn, calcId int, levelArr []levelDef, isRunCompare bool,
) (
	string, error,
) {

	// no comparison, microdata aggregation for each run: OM_SUM(Income - 0.5 * OM_AVG(Pension))
	//
	// -- WITH abase (run_id, entity_key, attr1, attr2, attr3, attr8) AS (....)
	//
	// SELECT
	//   A.run_id, CalcId AS calc_id, A.attr1, A.attr2, A.calc_value
	// FROM
	// (
	//   SELECT
	//     M1.run_id, M1.attr1, M1.attr2,
	//     SUM(M1.attr3 - 0.5 * L1E1.ex1) AS calc_value
	//   FROM abase M1
	//   INNER JOIN
	//   (
	//     SELECT
	//       M2.run_id, M2.attr1, M2.attr2,
	//       AVG(M2.attr8) AS ex1
	//     FROM abase M2
	//     GROUP BY M2.run_id, M2.attr1, M2.attr2
	//   ) L1A1
	//   ON (L1A1.run_id = M1.run_id AND L1A1.attr1 = M1.attr1 AND L1A1.attr2 = M1.attr2)
	//   GROUP BY M1.run_id, M1.attr1, M1.attr2
	// ) A
	//
	// microdata run comparison: OM_AVG( Income[varinat] - (Pension[base] + Salary[base]) )
	//
	// -- WITH abase (run_id, entity_key, attr1, attr2, attr4, attr8) AS (....),
	// -- avar (run_id, entity_key, attr1, attr2, attr3) AS (....),
	// -- abv (run_id, attr1, attr2, attr3_var, attr4_base, attr8_base) AS (....)
	//
	// SELECT
	//   A.run_id, CalcId AS calc_id, A.attr1, A.attr2, A.calc_value
	// FROM
	// (
	//   SELECT
	//     M1.run_id, M1.attr1, M1.attr2,
	//     AVG(M1.attr3_var - (M1.attr8_base + M1.attr4_base)) AS calc_value
	//   FROM abv M1
	//   GROUP BY M1.run_id, M1.attr1, M1.attr2
	// ) A
	//
	vSrc := "abase"
	if isRunCompare {
		vSrc = "abv"
	}

	// SELECT  A.run_id, CalcId AS calc_id, A.attr1, A.attr2, A.calc_value FROM (
	//
	mainSql := "SELECT A.run_id, " + strconv.Itoa(calcId) + " AS calc_id"

	for _, c := range aggrCols {
		if c.isGroup {
			mainSql += ", A." + c.colName
		}
	}
	mainSql += ", A.calc_value FROM ( "

	// main aggregation sql body
	for nLev, lv := range levelArr {

		//   SELECT
		//     M1.run_id, M1.attr1, M1.attr2,
		//     SUM(M1.attr3 - 0.5 * L1E1.ex1) AS calc_value
		//   FROM abase M1
		//   INNER JOIN
		//   (
		mainSql += "SELECT " + lv.fromAlias + ".run_id"

		for _, c := range aggrCols {
			if c.isGroup {
				mainSql += ", " + lv.fromAlias + "." + c.colName
			}
		}

		for _, expr := range lv.exprArr {
			mainSql += ", " + expr.sqlExpr
			if expr.colName != "" {
				mainSql += " AS " + expr.colName
			}
		}

		mainSql += " FROM " + vSrc + " " + lv.fromAlias

		if nLev < len(levelArr)-1 { // if not lowest level then continue INNER JOIN down to the next level
			mainSql += " INNER JOIN ("
		}
	}

	// for each level except of the lowest append:
	//     GROUP BY M2.run_id, M2.attr1, M2.attr2
	//   ) L1A1
	//   ON (L1A1.run_id = M1.run_id AND L1A1.attr1 = M1.attr1 AND L1A1.attr2 = M1.attr2)
	//
	for nLev := len(levelArr) - 1; nLev >= 0; nLev-- {

		mainSql += " GROUP BY " + levelArr[nLev].fromAlias + ".run_id"

		for _, c := range aggrCols {
			if c.isGroup {
				mainSql += ", " + levelArr[nLev].fromAlias + "." + c.colName
			}
		}

		if nLev > 0 {

			mainSql += ") " + levelArr[nLev].innerAlias +
				" ON (" + levelArr[nLev].innerAlias + ".run_id = " + levelArr[nLev-1].fromAlias + ".run_id"

			for _, c := range aggrCols {
				if c.isGroup {
					mainSql += " AND " + levelArr[nLev].innerAlias + "." + c.colName + " = " + levelArr[nLev-1].fromAlias + "." + c.colName
				}
			}

			mainSql += ")"
		}
	}
	mainSql += " ) A"

	return mainSql, nil
}

// Build CTE part of aggregation sql from the list of aggregated attributes.
func makeMicroCteAggrSql(
	entity *EntityMeta, entityGen *EntityGenMeta, aggrCols []aggrColulumn, fromId int, runIds []int, isRunCompare bool,
) (
	string, error,
) {

	// list of column names for CTE header and CTE body, add group by attributes in the order of attributes
	cHdr := "run_id, entity_key"
	cBody := "RE.run_id, C.entity_key"
	isAnyBase := false
	isAnyVar := false

	for _, c := range aggrCols {
		if c.isGroup {
			cHdr += ", " + c.colName
			cBody += ", C." + c.colName
		}
		if c.isBase {
			isAnyBase = true
		}
		if c.isVar {
			isAnyVar = true
		}
	}
	if !isAnyBase || isRunCompare && !isAnyVar {
		return "", errors.New("invalid (or mixed forms) of attribute names used for aggregation of: " + entity.Name)
	}

	// CTE: run comparison or attributes aggreagtion without comparison
	cteSql := ""

	if !isRunCompare { // no comparison, select attributes from the list of model runs

		// WITH abase (run_id, entity_key, attr1, attr2, attr3, attr8)
		// AS
		// (
		//   SELECT
		//     RE.run_id, C.entity_key, C.attr1, C.attr2, C.attr4, C.attr8
		//   FROM Person_gfa43c687 C
		//   INNER JOIN run_entity RE ON (RE.base_run_id = C.run_id AND RE.entity_gen_hid = 201)
		//   WHERE RE.run_id IN (219, 221, 222)
		// )
		//
		cteSql = "WITH abase (" + cHdr

		for k := range aggrCols {
			if aggrCols[k].isBase {
				cteSql += ", " + aggrCols[k].colName
			}
		}

		cteSql += ") AS (SELECT " + cBody

		for k := range aggrCols {
			if aggrCols[k].isBase {
				cteSql += ", C." + aggrCols[k].colName
			}
		}

		cteSql += " FROM " + entityGen.DbEntityTable + " C" +
			" INNER JOIN run_entity RE ON (RE.base_run_id = C.run_id AND RE.entity_gen_hid = " + strconv.Itoa(entityGen.GenHid) + ")" +
			" WHERE"

		if len(runIds) <= 0 {
			cteSql += " RE.run_id = " + strconv.Itoa(fromId)
		} else {
			cteSql += " RE.run_id IN (" + strconv.Itoa(fromId)

			for _, rId := range runIds {
				if rId != fromId {
					cteSql += ", " + strconv.Itoa(rId)
				}
			}
			cteSql += ")"
		}
		cteSql += ")"

		return cteSql, nil
	}
	// else run comparison: select from variant runs join to base run

	// microdata run comparison: OM_AVG( Income[varinat] - (Pension[base] + Salary[base]) )
	//
	// WITH abase (run_id, entity_key, attr1, attr2, attr4, attr8)
	// AS
	// (
	//   SELECT
	// 	   RE.run_id, C.entity_key, C.attr1, C.attr2, C.attr4, C.attr8
	//   FROM Person_gfa43c687 C
	//   INNER JOIN run_entity RE ON (RE.base_run_id = C.run_id AND RE.entity_gen_hid = 201)
	//   WHERE RE.run_id = 219
	// ),
	// avar (run_id, entity_key, attr1, attr2, attr3)
	// AS
	// (
	//   SELECT
	// 	   RE.run_id, C.entity_key, C.attr1, C.attr2, C.attr3
	//   FROM Person_gfa43c687 C
	//   INNER JOIN run_entity RE ON (RE.base_run_id = C.run_id AND RE.entity_gen_hid = 201)
	//   WHERE RE.run_id IN (221, 222)
	// ),
	// abv (run_id, attr1, attr2, attr4_base, attr8_base, attr3_var)
	// AS
	// (
	//   SELECT
	// 	   V.run_id, V.attr1, V.attr2, B.attr4 AS attr4_base, B.attr8 AS attr8_base, V.attr3 AS attr3_var
	//   FROM abase B
	//   INNER JOIN avar V ON (V.entity_key = B.entity_key)
	// )
	//
	cteSql = "WITH abase (" + cHdr

	for k := range aggrCols {
		if aggrCols[k].isBase {
			cteSql += ", " + aggrCols[k].colName
		}
	}

	cteSql += ") AS (SELECT " + cBody

	for k := range aggrCols {
		if aggrCols[k].isBase {
			cteSql += ", C." + aggrCols[k].colName
		}
	}

	cteSql += " FROM " + entityGen.DbEntityTable + " C" +
		" INNER JOIN run_entity RE ON (RE.base_run_id = C.run_id AND RE.entity_gen_hid = " + strconv.Itoa(entityGen.GenHid) + ")" +
		" WHERE RE.run_id = " + strconv.Itoa(fromId) +
		")"

	// variant runs
	cteSql += ", avar (" + cHdr

	for k := range aggrCols {
		if aggrCols[k].isVar {
			cteSql += ", " + aggrCols[k].colName
		}
	}

	cteSql += ") AS (SELECT " + cBody

	for k := range aggrCols {
		if aggrCols[k].isVar {
			cteSql += ", C." + aggrCols[k].colName
		}
	}

	cteSql += " FROM " + entityGen.DbEntityTable + " C" +
		" INNER JOIN run_entity RE ON (RE.base_run_id = C.run_id AND RE.entity_gen_hid = " + strconv.Itoa(entityGen.GenHid) + ")" +
		" WHERE RE.run_id IN ("

	isF := true
	for _, rId := range runIds {
		if rId != fromId {
			if isF {
				isF = false
				cteSql += strconv.Itoa(rId)
			} else {
				cteSql += ", " + strconv.Itoa(rId)
			}
		}
	}
	cteSql += "))"

	// inner join of base and variant runs by entity_key
	cteSql += ", abv (run_id"

	for _, c := range aggrCols {
		if c.isGroup {
			cteSql += ", " + c.colName
		}
	}
	for k := range aggrCols {
		if aggrCols[k].isBase {
			cteSql += ", " + aggrCols[k].colName + "_base"
		}
	}
	for k := range aggrCols {
		if aggrCols[k].isVar {
			cteSql += ", " + aggrCols[k].colName + "_var"
		}
	}

	cteSql += ") AS (SELECT V.run_id"

	for _, c := range aggrCols {
		if c.isGroup {
			cteSql += ", V." + c.colName
		}
	}
	for k := range aggrCols {
		if aggrCols[k].isBase {
			cteSql += ", B." + aggrCols[k].colName + " AS " + aggrCols[k].colName + "_base"
		}
	}
	for k := range aggrCols {
		if aggrCols[k].isVar {
			cteSql += ", V." + aggrCols[k].colName + " AS " + aggrCols[k].colName + "_var"
		}
	}

	cteSql += " FROM abase B" +
		" INNER JOIN avar V ON (V.entity_key = B.entity_key)" +
		")"

	return cteSql, nil
}
