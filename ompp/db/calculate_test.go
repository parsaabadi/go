// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"strconv"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/openmpp/go/ompp/config"
)

func TestCleanSourceExpr(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate.ini", "")
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
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate.ini", "")
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
			t.Error("Fail:", src)
		}
	}

	// should be no errors because sql safely 'quoted'
	t.Log("Check if no error from 'quoted' SQL")
	for _, src := range srcLst {

		q := ToQuoted(src)
		t.Log(q)

		if e := errorIfUnsafeSqlOrComment(q); e != nil {
			t.Error("Fail:", e)
		}
	}
}

func TestTranslateAllSimpleFnc(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate.ini", "")
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
			t.Error("Fail:    ", r)
		} else {
			t.Log("=>", r)
		}
	}
}

func TestTranslateToExprSql(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate.ini", "")
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

			sArr := strings.Split(sVal, ",")
			for k := range sArr {
				if id, err := strconv.Atoi(sArr[k]); err != nil {
					t.Fatal(err)
				} else {
					runIds = append(runIds, id)
				}
			}
		}
		t.Log("run id's:", runIds)

		valid := kvIni["TranslateToExprSql.Valid_"+strconv.Itoa(k+1)]

		cmpLt := &CalculateLayout{
			Calculate: cmpExpr,
			ReadLayout: ReadLayout{
				Name:   tableName,
				FromId: baseRunId,
			},
		}

		sql, err := translateToExprSql(table, "", cmpLt, runIds)
		if err != nil {
			t.Fatal(err)
		}
		if sql != valid {
			t.Error("Expected:", valid)
			t.Error("Fail:    ", sql)
		} else {
			t.Log("=>", sql)
		}
	}
}

func TestParseAggrCalculation(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	modelName := kvIni["ParseAggrCalculation.ModelName"]
	modelDigest := kvIni["ParseAggrCalculation.ModelDigest"]
	modelSqliteDbPath := kvIni["ParseAggrCalculation.DbPath"]
	tableName := kvIni["ParseAggrCalculation.TableName"]

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

	validLst := []struct {
		src   string
		name  string
		valid string
	}{}
	for k := 0; k < 100; k++ {
		s := kvIni["ParseAggrCalculation.Src_"+strconv.Itoa(k+1)]
		if s == "" {
			continue
		}
		validLst = append(validLst,
			struct {
				src   string
				name  string
				valid string
			}{
				src:   s,
				name:  kvIni["ParseAggrCalculation.Name_"+strconv.Itoa(k+1)],
				valid: kvIni["ParseAggrCalculation.Valid_"+strconv.Itoa(k+1)],
			})
	}

	t.Log("Check if aggregation functions parsed OK")
	for _, v := range validLst {

		t.Log(v.src)

		r, e := parseAggrCalculation(table, v.name, v.src)
		if e != nil {
			t.Fatal(e)
		}

		s := "" // join sql expressions for all levels
		for k, lv := range r {

			t.Log("[ ", k, " ]")

			t.Log("  level:", lv.level)
			t.Log("  fromAlias:     ", lv.fromAlias)
			t.Log("  innerAlias:    ", lv.innerAlias)
			t.Log("  nextInnerAlias:", lv.nextInnerAlias)
			t.Log("  firstAccIdx:   ", lv.firstAccIdx)
			t.Log("  accUsageArr:", lv.accUsageArr)

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
			t.Error("Fail:    ", s)
		} else {
			t.Log("=>", s)
		}
	}
}

func TestTransalteAccAggrToSql(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	modelName := kvIni["TransalteAccAggrToSql.ModelName"]
	modelDigest := kvIni["TransalteAccAggrToSql.ModelDigest"]
	modelSqliteDbPath := kvIni["TransalteAccAggrToSql.DbPath"]
	tableName := kvIni["TransalteAccAggrToSql.TableName"]

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

	validLst := []struct {
		src   string
		name  string
		valid string
	}{}
	for k := 0; k < 100; k++ {
		s := kvIni["TransalteAccAggrToSql.Src_"+strconv.Itoa(k+1)]
		if s == "" {
			continue
		}
		validLst = append(validLst,
			struct {
				src   string
				name  string
				valid string
			}{
				src:   s,
				name:  kvIni["TransalteAccAggrToSql.Name_"+strconv.Itoa(k+1)],
				valid: kvIni["TransalteAccAggrToSql.Valid_"+strconv.Itoa(k+1)],
			})
	}

	t.Log("Check aggregation SQL")
	for _, v := range validLst {

		t.Log(v.src)

		cteSql, mainSql, e := transalteAccAggrToSql(table, v.name, v.src)
		if e != nil {
			t.Fatal(e)
		}

		sql := ""
		if cteSql != "" {
			sql += "WITH " + cteSql + " "
		}
		sql += mainSql

		if err != nil {
			t.Fatal(err)
		}

		if sql != v.valid {
			t.Error("Expected:", v.valid)
			t.Error("Fail:    ", sql)
		} else {
			t.Log("=>", sql)
		}
	}
}
