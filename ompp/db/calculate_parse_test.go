// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"strconv"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/helper"
)

func TestCleanSourceExpr(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate-parse.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	srcExpr := kvIni["CleanSource.Src"]

	t.Log("cleanSourceExpr:", srcExpr)

	// cleanup cr lf
	src := srcExpr
	for k := 0; k < 4; k++ {
		src = strings.Replace(src, "\x20", "\r", 1)
		src = strings.Replace(src, "\x20", "\n", 1)
	}
	t.Log("clean cr lf:", src)

	cln := cleanSourceExpr(src)
	if cln != srcExpr {
		t.Error("Fail cr lf:", cln)
	}

	// cleanup unsafe sql 'quotes'
	/*
	  &#x2b9;    697  Modifier Letter Prime
	  &#x2bc;    700  Modifier Letter Apostrophe
	  &#x2c8;    712  Modifier Letter Vertical Line
	  &#x2032;  8242  Prime
	  &#xff07; 65287  Fullwidth Apostrophe
	*/
	src = srcExpr
	cln = srcExpr
	for k := 0; k < 4; k++ {
		src = strings.Replace(src, "\x20", "\u02b9", 1)
		cln = strings.Replace(cln, "\x20", "'", 1)
		src = strings.Replace(src, "\x20", "\u02bc", 1)
		cln = strings.Replace(cln, "\x20", "'", 1)
		src = strings.Replace(src, "\x20", "\u02c8", 1)
		cln = strings.Replace(cln, "\x20", "'", 1)
		src = strings.Replace(src, "\x20", "\u2032", 1)
		cln = strings.Replace(cln, "\x20", "'", 1)
		src = strings.Replace(src, "\x20", "\uff07", 1)
		cln = strings.Replace(cln, "\x20", "'", 1)
	}
	t.Log("unsafe 'quotes':", src)
	t.Log("expected       :", cln)

	if cln != cleanSourceExpr(src) {
		t.Error("Fail 'quotes':", cln)
	}
}

func TestErrorIfUnsafeSqlOrComment(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate-parse.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	srcLst := []string{}
	for k := 0; k < 100; k++ {
		s := kvIni["UnsafeSql.Src_"+strconv.Itoa(k+1)]
		if s != "" {
			srcLst = append(srcLst, s)
		}
	}

	// expected an error for each unsafe sql
	t.Log("Check if error returned for unsafe SQL")
	for _, src := range srcLst {

		t.Log(src)

		if e := errorIfUnsafeSqlOrComment(src); e != nil {
			t.Log("OK:", e)
		} else {
			t.Error("****FAIL:", src)
		}
	}

	// should be no errors because sql safely 'quoted'
	t.Log("Check if no error from 'quoted' SQL")
	for _, src := range srcLst {

		q := ToQuoted(src)
		t.Log(q)

		if e := errorIfUnsafeSqlOrComment(q); e != nil {
			t.Error("****FAIL:", e)
		}
	}
}

func TestTranslateAllSimpleFnc(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate-parse.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	validLst := []struct {
		src   string
		valid string
	}{}
	for k := 0; k < 100; k++ {
		s := kvIni["TranslateSimpleFnc.Src_"+strconv.Itoa(k+1)]
		if s == "" {
			continue
		}
		validLst = append(validLst,
			struct {
				src   string
				valid string
			}{
				src:   s,
				valid: kvIni["TranslateSimpleFnc.Valid_"+strconv.Itoa(k+1)],
			})
	}

	t.Log("Check if all non-aggregation functions translated OK")
	for _, v := range validLst {

		t.Log(v.src)

		r, e := translateAllSimpleFnc(v.src)
		if e != nil {
			t.Fatal(e)
		}

		if r != v.valid {
			t.Error("Expected:", v.valid)
			t.Error("****FAIL:", r)
		} else {
			t.Log("=>", r)
		}
	}
}

func TestTranslateToExprSql(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate-parse.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	modelName := kvIni["TranslateToExprSql.ModelName"]
	modelDigest := kvIni["TranslateToExprSql.ModelDigest"]
	modelSqliteDbPath := kvIni["TranslateToExprSql.DbPath"]
	tableName := kvIni["TranslateToExprSql.TableName"]

	// open source database connection and check is it valid
	cs := MakeSqliteDefaultReadOnly(modelSqliteDbPath)
	t.Log(cs)

	srcDb, _, err := Open(cs, SQLiteDbDriver, false)
	if err != nil {
		t.Fatal(err)
	}
	defer srcDb.Close()

	if err := CheckOpenmppSchemaVersion(srcDb); err != nil {
		t.Fatal(err)
	}

	// get model metadata
	modelDef, err := GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		t.Fatal(err)
	}
	if modelDef == nil {
		t.Errorf("model not found: %s :%s:", modelName, modelDigest)
	}
	t.Log("Model:", modelDef.Model.Name, " ", modelDef.Model.Digest)

	// find output table id by name
	var table *TableMeta
	if k, ok := modelDef.OutTableByName(tableName); ok {
		table = &modelDef.Table[k]
	} else {
		t.Errorf("output table not found: " + tableName)
	}

	t.Log("Check non-aggregation SQL")
	for k := 0; k < 100; k++ {

		cmpExpr := kvIni["TranslateToExprSql.Calculate_"+strconv.Itoa(k+1)]
		if cmpExpr == "" {
			continue
		}
		t.Log("Calculate:", cmpExpr)

		var baseRunId int = 0
		if sVal := kvIni["TranslateToExprSql.BaseRunId_"+strconv.Itoa(k+1)]; sVal != "" {
			if baseRunId, err = strconv.Atoi(sVal); err != nil {
				t.Fatal(err)
			}
		}
		t.Log("base run:", baseRunId)

		runIds := []int{}
		if sVal := kvIni["TranslateToExprSql.RunIds_"+strconv.Itoa(k+1)]; sVal != "" {

			sArr := helper.ParseCsvLine(sVal, ',')
			for j := range sArr {
				if id, err := strconv.Atoi(sArr[j]); err != nil {
					t.Fatal(err)
				} else {
					runIds = append(runIds, id)
				}
			}
		}
		t.Log("run id's:", runIds)

		valid := kvIni["TranslateToExprSql.Valid_"+strconv.Itoa(k+1)]

		readLt := &ReadLayout{
			Name:   tableName,
			FromId: baseRunId,
		}
		cmpLt := CalculateLayout{Calculate: cmpExpr}

		sql, err := translateToExprSql(table, readLt, &cmpLt, runIds)
		if err != nil {
			t.Fatal(err)
		}
		if sql != valid {
			t.Error("Expected:", valid)
			t.Error("****FAIL:", sql)
		} else {
			t.Log("=>", sql)
		}
	}
}

func TestParseAggrCalculation(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate-parse.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	modelName := kvIni["ParseAggrCalculation.ModelName"]
	modelDigest := kvIni["ParseAggrCalculation.ModelDigest"]
	modelSqliteDbPath := kvIni["ParseAggrCalculation.DbPath"]
	tableName := kvIni["ParseAggrCalculation.TableName"]
	entityName := kvIni["ParseAggrCalculation.EntityName"]
	microRunId := 0

	if entityName != "" {
		if sVal := kvIni["ParseAggrCalculation.MicroBaseRunId"]; sVal != "" {
			microRunId, err = strconv.Atoi(sVal)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// open source database connection and check is it valid
	cs := MakeSqliteDefaultReadOnly(modelSqliteDbPath)
	t.Log(cs)

	srcDb, _, err := Open(cs, SQLiteDbDriver, false)
	if err != nil {
		t.Fatal(err)
	}
	defer srcDb.Close()

	if err := CheckOpenmppSchemaVersion(srcDb); err != nil {
		t.Fatal(err)
	}

	// get model metadata
	modelDef, err := GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		t.Fatal(err)
	}
	if modelDef == nil {
		t.Errorf("model not found: %s :%s:", modelName, modelDigest)
	}
	t.Log("Model:", modelDef.Model.Name, " ", modelDef.Model.Digest)

	// find output table id by name
	var table *TableMeta
	if k, ok := modelDef.OutTableByName(tableName); ok {
		table = &modelDef.Table[k]
	} else {
		t.Fatal("output table not found:", tableName)
	}

	// find entity generation by entity name and run id
	entity := &EntityMeta{}
	entityGen := &EntityGenMeta{}

	if entityName != "" {

		// find model entity by entity name
		eIdx, ok := modelDef.EntityByName(entityName)
		if !ok {
			t.Fatal("entity not found:", entityName)
		}
		entity = &modelDef.Entity[eIdx]

		// get list of entity generations for that model run
		egLst, err := GetEntityGenList(srcDb, microRunId)
		if err != nil {
			t.Fatal("Error at get run entities: ", entityName, ": ", microRunId, ": ", err.Error())
		}

		// find entity generation by entity name
		gIdx := -1
		for k := range egLst {

			if egLst[k].EntityId == entity.EntityId {
				gIdx = k
				break
			}
		}
		if gIdx < 0 {
			t.Fatal("Error: model run entity generation not found: ", entityName, ": ", microRunId)
		}

		entityGen = &egLst[gIdx]
	}

	// aggregation expression columns: only native (not a derived) accumulators can be aggregated
	accAggrCols := make([]aggrColumn, len(table.Acc))

	for k := range table.Acc {
		accAggrCols[k] = aggrColumn{
			name:    table.Acc[k].Name,
			colName: table.Acc[k].colName,
			isAggr:  !table.Acc[k].IsDerived, // only native accumulators can be aggregated
		}
	}

	// produce accumulator column name: acc0 => M1.acc_value or acc4 => L1A4.acc4
	makeAccColName := func(
		name string, nameIdx int, isSimple, isVar bool, firstAlias string, levelAccAlias string, isFirstAcc bool,
	) string {

		if isFirstAcc {
			return firstAlias + "." + "acc_value" // first accumulator: acc0 => acc_value
		}
		return levelAccAlias + "." + name // any other accumulator: acc4 => acc4
	}

	validLst := []struct {
		kind    string
		src     string
		groupBy string
		valid   string
	}{}
	for k := 0; k < 100; k++ {
		s := kvIni["ParseAggrCalculation.Src_"+strconv.Itoa(k+1)]
		if s == "" {
			continue
		}
		validLst = append(validLst,
			struct {
				kind    string
				src     string
				groupBy string
				valid   string
			}{
				kind:    kvIni["ParseAggrCalculation.Kind_"+strconv.Itoa(k+1)],
				src:     s,
				groupBy: kvIni["ParseAggrCalculation.GroupBy_"+strconv.Itoa(k+1)],
				valid:   kvIni["ParseAggrCalculation.Valid_"+strconv.Itoa(k+1)],
			})
	}

	// parse aggregation expression
	t.Log("Check if aggregation functions parsed OK")
	for _, v := range validLst {

		t.Log(v.src)

		var r []levelDef
		var e error

		if v.kind == "table" {

			r, e = parseAggrCalculation(accAggrCols, v.src, makeAccColName)
			if e != nil {
				t.Fatal(e)
			}

		} else { // microdata
			if v.kind != "micro" || entityName == "" {
				t.Fatal("Entity name is empty or invalid parse test kind:", v.kind)
			}

			groupBy := helper.ParseCsvLine(v.groupBy, ',')
			if v.groupBy == "" {
				t.Fatal("Group by is empty")
			}
			t.Log("Group by:", groupBy)

			// create list of microdata columns
			attrAggrCols, e := makeMicroAggrCols(entity, entityGen, groupBy)
			if e != nil {
				t.Fatal("Fail to makeMicroAggrCols:", entityName, ":", groupBy)
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
						attrAggrCols[nameIdx].isVar = true
						return firstAlias + "." + attrAggrCols[nameIdx].colName + "_var" // variant run attribute: attr1[variant] => L1A4.attr1_var
					}
					isAnyBase = true
					attrAggrCols[nameIdx].isBase = true
					return firstAlias + "." + attrAggrCols[nameIdx].colName + "_base" // base run attribute: attr1[base] => L1A4.attr1_base
				}
				// else: isSimple name, not a name[base] or name[variant]
				isAnySimple = true
				attrAggrCols[nameIdx].isSimple = true
				return firstAlias + "." + attrAggrCols[nameIdx].colName // not a run comparison: attr2 => L1A4.attr2
			}

			// parse aggregation expression
			r, e = parseAggrCalculation(attrAggrCols, v.src, makeAttrColName)
			if e != nil {
				t.Fatal(e)
			}

			t.Log("isAnySimple: ", isAnySimple)
			t.Log("isAnyBase:   ", isAnyBase)
			t.Log("isAnyVar:    ", isAnyVar)
			t.Log("attrAggrCols:", attrAggrCols)
		}

		s := "" // join sql expressions for all levels
		for k, lv := range r {

			t.Log("[ ", k, " ]")

			t.Log("  level:", lv.level)
			t.Log("  fromAlias:     ", lv.fromAlias)
			t.Log("  innerAlias:    ", lv.innerAlias)
			t.Log("  nextInnerAlias:", lv.nextInnerAlias)
			t.Log("  firstAccIdx:   ", lv.firstAgcIdx)
			t.Log("  accUsageArr:", lv.agcUsageArr)

			for j, ex := range lv.exprArr {
				t.Log("  [ ", j, " ]")
				t.Log("    colName:", ex.colName)
				t.Log("    srcExpr:", ex.srcExpr)
				t.Log("    sqlExpr:", ex.sqlExpr)
				if s != "" {
					s += "--" + strconv.Itoa(k) + "_" + strconv.Itoa(j) + "--"
				}
				s += ex.sqlExpr
			}
		}

		if s != v.valid {
			t.Error("Expected:", v.valid)
			t.Error("****FAIL:", s)
		} else {
			t.Log("=>", s)
		}
	}
}
