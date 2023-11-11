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

		if cLst := kvIni["CompareOutputTable.Calculate_"+strconv.Itoa(k+1)]; cLst != "" {
			appendToCalc(cLst, false, CALCULATED_ID_OFFSET)
		}
		if cLst := kvIni["CompareOutputTable.CalculateAggr_"+strconv.Itoa(k+1)]; cLst != "" {
			appendToCalc(cLst, true, 2*CALCULATED_ID_OFFSET)
		}
		if len(calcLt) <= 0 {
			continue
		}

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
			for j := range sArr {
				if id, err := strconv.Atoi(sArr[j]); err != nil {
					t.Fatal(err)
				} else {
					runIds = append(runIds, id)
				}
			}
		}
		if len(runIds) <= 0 {
			t.Fatal("ERROR: empty run list at CompareOutputTable.RunIds", k+1)
		}
		t.Log("run id's:", runIds)

		tableLt := &ReadTableLayout{
			ReadLayout: ReadLayout{
				Name:   tableName,
				FromId: baseRunId,
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
		csvDir := filepath.Join("testdata", "TestCompareOutputTable-"+helper.MakeTimeStamp(time.Now()))
		err = os.MkdirAll(csvDir, 0750)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Output to:", csvDir)

		err = writeTestToCsvIdFile(csvDir, modelDef, tableName, csvCvt, cLst)
		if err != nil {
			t.Fatal(err)
		}

		// read valid csv input and compare
		// valid := kvIni["CompareOutputTable.Valid_"+strconv.Itoa(k+1)]
	}
}

// create or truncate csv file and write cell list, using id's, not codes
func writeTestToCsvIdFile(
	csvDir string,
	modelDef *ModelMeta,
	name string,
	csvCvt CsvConverter,
	cellLst *list.List) error {

	// converter from db cell to csv id row []string
	cvt, err := csvCvt.ToCsvIdRow()
	if err != nil {
		return err
	}

	// create csv file
	fn, err := csvCvt.CsvFileName()
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
	cs, err := csvCvt.CsvHeader()
	if err != nil {
		return err
	}
	if err = wr.Write(cs); err != nil {
		return err
	}

	for c := cellLst.Front(); c != nil; c = c.Next() {

		// write cell line: run id, dimension(s) and value
		if _, err := cvt(c.Value, cs); err != nil {
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
