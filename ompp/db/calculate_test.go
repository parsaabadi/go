// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/helper"
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
			t.Error("****FAIL:", r)
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
				valid string
			}{
				src:   s,
				valid: kvIni["ParseAggrCalculation.Valid_"+strconv.Itoa(k+1)],
			})
	}

	t.Log("Check if aggregation functions parsed OK")
	for _, v := range validLst {

		t.Log(v.src)

		r, e := parseAggrCalculation(table, v.src)
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
			t.Error("****FAIL:", s)
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
				valid string
			}{
				src:   s,
				valid: kvIni["TransalteAccAggrToSql.Valid_"+strconv.Itoa(k+1)],
			})
	}

	t.Log("Check aggregation SQL")
	for _, v := range validLst {

		t.Log(v.src)

		cteSql, mainSql, e := transalteAccAggrToSql(table, 0, v.src)
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
			t.Error("****FAIL:", sql)
		} else {
			t.Log("=>", sql)
		}
	}
}

func TestTranslateTableCalcToSql(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	modelName := kvIni["TranslateTableCalcToSql.ModelName"]
	modelDigest := kvIni["TranslateTableCalcToSql.ModelDigest"]
	modelSqliteDbPath := kvIni["TranslateTableCalcToSql.DbPath"]
	tableName := kvIni["TranslateTableCalcToSql.TableName"]

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

	t.Log("Check calculation SQL")
	for k := 0; k < 100; k++ {

		calcLt := []CalculateTableLayout{}

		appendToCalc := func(src string, isAggr bool, idOffset int) {

			ce := strings.Split(src, ",")
			for j := range ce {

				c := strings.TrimSpace(ce[j])
				if c[0] == '"' && c[len(c)-1] == '"' {
					c = c[1 : len(c)-1]
				}

				if c != "" {

					calcLt = append(calcLt, CalculateTableLayout{
						CalculateLayout: CalculateLayout{
							Calculate: c,
							CalcId:    idOffset + j,
						},
						IsAggr: isAggr,
					})
					t.Log("Calculate:", c)
					t.Log(tableName, " Is aggregation:", isAggr)
				}
			}
		}

		if cLst := kvIni["TranslateTableCalcToSql.Calculate_"+strconv.Itoa(k+1)]; cLst != "" {
			appendToCalc(cLst, false, CALCULATED_ID_OFFSET)
		}
		if cLst := kvIni["TranslateTableCalcToSql.CalculateAggr_"+strconv.Itoa(k+1)]; cLst != "" {
			appendToCalc(cLst, true, 2*CALCULATED_ID_OFFSET)
		}
		if len(calcLt) <= 0 {
			continue
		}

		baseRunId := 0
		if sVal := kvIni["TranslateTableCalcToSql.BaseRunId_"+strconv.Itoa(k+1)]; sVal != "" {
			baseRunId, err = strconv.Atoi(sVal)
			if err != nil {
				t.Fatal(err)
			}
		}

		runIds := []int{}
		if sVal := kvIni["TranslateTableCalcToSql.RunIds_"+strconv.Itoa(k+1)]; sVal != "" {

			sArr := strings.Split(sVal, ",")
			for j := range sArr {
				if id, err := strconv.Atoi(sArr[j]); err != nil {
					t.Fatal(err)
				} else {
					runIds = append(runIds, id)
				}
			}
		}
		if len(runIds) <= 0 {
			t.Fatal("ERROR: empty run list at TranslateTableCalcToSql.RunIds", k+1)
		}
		t.Log("run id's:", runIds)

		tableLt := &ReadTableLayout{
			ReadLayout: ReadLayout{
				Name: tableName,
			},
		}
		if baseRunId > 0 {
			tableLt.FromId = baseRunId
		}

		sql, e := translateTableCalcToSql(table, &tableLt.ReadLayout, calcLt, runIds)
		if e != nil {
			t.Fatal(e)
		}

		// read valid sql and compare
		valid := kvIni["TranslateTableCalcToSql.Valid_"+strconv.Itoa(k+1)]

		if sql != valid {
			t.Error("Expected:", valid)
			t.Error("****FAIL:", sql)
		} else {
			t.Log("=>", sql)
		}
	}
}

func TestCalculateOutputTable(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.calculate.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	modelName := kvIni["CalculateOutputTable.ModelName"]
	modelDigest := kvIni["CalculateOutputTable.ModelDigest"]
	modelSqliteDbPath := kvIni["CalculateOutputTable.DbPath"]
	tableName := kvIni["CalculateOutputTable.TableName"]

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

	// create csv converter by including all model runs (test only)
	rLst, err := GetRunList(srcDb, modelDef.Model.ModelId)
	if err != nil {
		t.Fatal(err)
	}

	csvCvt := &CellTableCalcConverter{
		CellTableConverter: CellTableConverter{
			ModelDef: modelDef,
			Name:     tableName,
		},
		IsIdCsv:    true,
		IdToDigest: map[int]string{},
		DigestToId: map[string]int{},
	}
	for _, r := range rLst {
		csvCvt.IdToDigest[r.RunId] = r.RunDigest
		csvCvt.DigestToId[r.RunDigest] = r.RunId
	}

	for k := 0; k < 100; k++ {

		calcLt := []CalculateTableLayout{}

		appendToCalc := func(src string, isAggr bool, idOffset int) {

			ce := strings.Split(src, ",")
			for j := range ce {

				c := strings.TrimSpace(ce[j])
				if c[0] == '"' && c[len(c)-1] == '"' {
					c = c[1 : len(c)-1]
				}

				if c != "" {

					calcLt = append(calcLt, CalculateTableLayout{
						CalculateLayout: CalculateLayout{
							Calculate: c,
							CalcId:    idOffset + j,
						},
						IsAggr: isAggr,
					})
					t.Log(calcLt[len(calcLt)-1].CalcId, "Calculate:", c)
					t.Log(tableName, " Is aggregation:", isAggr)
				}
			}
		}

		if cLst := kvIni["CalculateOutputTable.Calculate_"+strconv.Itoa(k+1)]; cLst != "" {
			appendToCalc(cLst, false, CALCULATED_ID_OFFSET)
		}
		if cLst := kvIni["CalculateOutputTable.CalculateAggr_"+strconv.Itoa(k+1)]; cLst != "" {
			appendToCalc(cLst, true, 2*CALCULATED_ID_OFFSET)
		}
		if len(calcLt) <= 0 {
			continue
		}

		runIds := []int{}
		if sVal := kvIni["CalculateOutputTable.RunIds_"+strconv.Itoa(k+1)]; sVal != "" {

			sArr := strings.Split(sVal, ",")
			for j := range sArr {
				if id, err := strconv.Atoi(sArr[j]); err != nil {
					t.Fatal(err)
				} else {
					runIds = append(runIds, id)
				}
			}
		}
		if len(runIds) <= 0 {
			t.Fatal("ERROR: empty run list at CalculateOutputTable.RunIds", k+1)
		}
		t.Log("run id's:", runIds)

		tableLt := &ReadTableLayout{
			ReadLayout: ReadLayout{
				Name:   tableName,
				FromId: runIds[0],
			},
		}

		// read table
		cLst, rdLt, err := CalculateOutputTable(srcDb, modelDef, tableLt, calcLt, runIds)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Row count:", cLst.Len())
		t.Log("Read layout Offset Size IsFullPage IsLastPage:", rdLt.Offset, rdLt.Size, rdLt.IsFullPage, rdLt.IsLastPage)

		// create new output directory and csv file
		csvDir := filepath.Join("testdata", "TestCalculateOutputTable-"+helper.MakeTimeStamp(time.Now()))
		err = os.MkdirAll(csvDir, 0750)
		if err != nil {
			t.Fatal(err)
		}

		err = writeTestToCsvIdFile(csvDir, modelDef, tableName, csvCvt, cLst)
		if err != nil {
			t.Fatal(err)
		}

		// read valid csv input and compare
		// valid := kvIni["CalculateOutputTable.Valid_"+strconv.Itoa(k+1)]
	}
}
