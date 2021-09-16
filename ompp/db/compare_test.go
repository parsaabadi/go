// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"encoding/csv"
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
	kvIni, err := config.NewIni("testdata/test.ompp.db.compare.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	srcExpr := kvIni["TestClean.Src"]

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
	t.Log("clean 'quotes':", src)
	t.Log("expected      :", cln)

	if cln != cleanSourceExpr(src) {
		t.Error("Fail 'quotes':", cln)
	}
}

func TestErrorIfUnsafeSqlOrComment(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.compare.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	srcLst := []string{}
	for k := 0; k < 100; k++ {
		s := kvIni["TestUnsafeSql.Src_"+strconv.Itoa(k+1)]
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
	kvIni, err := config.NewIni("testdata/test.ompp.db.compare.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	validLst := []struct {
		src   string
		valid string
	}{}
	for k := 0; k < 100; k++ {
		s := kvIni["TestTranslateSimpleFnc.Src_"+strconv.Itoa(k+1)]
		if s == "" {
			continue
		}
		validLst = append(validLst,
			struct {
				src   string
				valid string
			}{
				src:   s,
				valid: kvIni["TestTranslateSimpleFnc.Valid_"+strconv.Itoa(k+1)],
			})
	}

	// expected an error for each unsafe sql
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
	kvIni, err := config.NewIni("testdata/test.ompp.db.compare.ini", "")
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

	for k := 0; k < 100; k++ {

		cmpExpr := kvIni["TranslateToExprSql.Comparison_"+strconv.Itoa(k+1)]
		if cmpExpr == "" {
			continue
		}
		t.Log("Comparison:", cmpExpr)

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

		cmpLt := &CompareLayout{
			Comparison: cmpExpr,
			ReadLayout: ReadLayout{
				Name:   tableName,
				FromId: baseRunId,
			},
		}

		sql, err := translateToExprSql(modelDef, table, cmpLt, runIds)
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

func TestCompareOutputTable(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.compare.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	modelName := kvIni["CompareOutputTable.ModelName"]
	modelDigest := kvIni["CompareOutputTable.ModelDigest"]
	modelSqliteDbPath := kvIni["CompareOutputTable.DbPath"]
	tableName := kvIni["CompareOutputTable.TableName"]

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

	csvCvt := CellTableCmpConverter{IdToDigest: map[int]string{}, DigestToId: map[string]int{}}
	for _, r := range rLst {
		csvCvt.IdToDigest[r.RunId] = r.RunDigest
		csvCvt.DigestToId[r.RunDigest] = r.RunId
	}

	// create csv ouput directory
	csvDir := filepath.Join("testdata", "TestCompareOutputTable-"+helper.MakeTimeStamp(time.Now()))
	err = os.MkdirAll(csvDir, 0750)
	if err != nil {
		t.Fatal(err)
	}

	for k := 0; k < 100; k++ {

		cmpExpr := kvIni["CompareOutputTable.Comparison_"+strconv.Itoa(k+1)]
		if cmpExpr == "" {
			continue
		}
		t.Log("Comparison:", cmpExpr)

		isAccum := false
		if sVal := kvIni["CompareOutputTable.IsAcc_"+strconv.Itoa(k+1)]; sVal != "" {
			if isAccum, err = strconv.ParseBool(sVal); err != nil {
				t.Fatal(err)
			}
		}
		t.Log(tableName, " Is accumulators:", isAccum)

		var baseRunId int = 0
		if sVal := kvIni["CompareOutputTable.BaseRunId_"+strconv.Itoa(k+1)]; sVal != "" {
			if baseRunId, err = strconv.Atoi(sVal); err != nil {
				t.Fatal(err)
			}
		}
		t.Log("base run:", baseRunId)

		runIds := []int{}
		if sVal := kvIni["CompareOutputTable.RunIds_"+strconv.Itoa(k+1)]; sVal != "" {

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

		cmpLt := &CompareTableLayout{
			CompareLayout: CompareLayout{
				Comparison: cmpExpr,
				ReadLayout: ReadLayout{
					Name:   tableName,
					FromId: baseRunId,
				},
			},
			IsAccum: isAccum,
		}

		cLst, rdLt, err := CompareOutputTable(srcDb, modelDef, cmpLt, runIds)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Row count:", cLst.Len())
		t.Log("Read layout:", rdLt)

		// create new into csv output file
		err = writeToCsvIdFile(csvDir, modelDef, tableName, csvCvt, cLst)
		if err != nil {
			t.Fatal(err)
		}

		// read valid csv input and compare
		// valid := kvIni["CompareOutputTable.Valid_"+strconv.Itoa(k+1)]
	}
}

// create or truncate csv file and write cell list, using id's, not codes
func writeToCsvIdFile(
	csvDir string,
	modelDef *ModelMeta,
	name string,
	csvCvt CsvConverter,
	cellLst *list.List) error {

	// converter from db cell to csv id row []string
	cvt, err := csvCvt.CsvToIdRow(modelDef, name)
	if err != nil {
		return err
	}

	// create csv file
	fn, err := csvCvt.CsvFileName(modelDef, name, true)
	if err != nil {
		return err
	}

	flag := os.O_CREATE | os.O_TRUNC | os.O_WRONLY

	f, err := os.OpenFile(filepath.Join(csvDir, fn), flag, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	wr := csv.NewWriter(f)

	// write header line: column names
	cs, err := csvCvt.CsvHeader(modelDef, name)
	if err != nil {
		return err
	}
	if err = wr.Write(cs); err != nil {
		return err
	}

	for c := cellLst.Front(); c != nil; c = c.Next() {

		// write cell line: run id, dimension(s) and value
		if err := cvt(c.Value, cs); err != nil {
			return err
		}
		if err := wr.Write(cs); err != nil {
			return err
		}
	}

	// flush and return error, if any
	wr.Flush()
	return wr.Error()
}
