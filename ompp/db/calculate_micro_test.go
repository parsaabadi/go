// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"strconv"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/helper"
)

func TestTranslateMicroCalcToSql(t *testing.T) {

	// load ini-file and parse test run options
	kvIni, err := config.NewIni("testdata/test.ompp.db.micro-aggregate.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	modelName := kvIni["TranslateMicroCalcToSql.ModelName"]
	modelDigest := kvIni["TranslateMicroCalcToSql.ModelDigest"]
	modelSqliteDbPath := kvIni["TranslateMicroCalcToSql.DbPath"]
	entityName := kvIni["TranslateMicroCalcToSql.EntityName"]

	baseRunId := 0
	if sVal := kvIni["TranslateMicroCalcToSql.BaseRunId"]; sVal != "" {
		baseRunId, err = strconv.Atoi(sVal)
		if err != nil {
			t.Fatal(err)
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

	// find entity generation by entity name and run id
	entity := &EntityMeta{}
	entityGen := &EntityGenMeta{}

	// find model entity by entity name
	eIdx, ok := modelDef.EntityByName(entityName)
	if !ok {
		t.Fatal("entity not found:", entityName)
	}
	entity = &modelDef.Entity[eIdx]

	// get list of entity generations for that model run
	egLst, err := GetEntityGenList(srcDb, baseRunId)
	if err != nil {
		t.Fatal("Error at get run entities: ", entityName, ": ", baseRunId, ": ", err.Error())
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
		t.Fatal("Error: model run entity generation not found: ", entityName, ": ", baseRunId)
	}

	entityGen = &egLst[gIdx]

	t.Log("Check microdata aggregation SQL")

	for k := 0; k < 100; k++ {
		srcCalc := kvIni["TranslateMicroCalcToSql.Src_"+strconv.Itoa(k+1)]
		if srcCalc == "" {
			continue
		}
		t.Log(srcCalc)

		cteValid := kvIni["TranslateMicroCalcToSql.Cte_"+strconv.Itoa(k+1)]
		mainValid := kvIni["TranslateMicroCalcToSql.Main_"+strconv.Itoa(k+1)]

		sGroupBy := kvIni["TranslateMicroCalcToSql.GroupBy_"+strconv.Itoa(k+1)]
		groupBy := helper.ParseCsvLine(sGroupBy, ',')

		t.Log("Group by: ", groupBy)

		runIds := []int{}
		if sVal := kvIni["TranslateMicroCalcToSql.RunIds_"+strconv.Itoa(k+1)]; sVal != "" {

			sArr := helper.ParseCsvLine(sVal, ',')
			for j := range sArr {
				if id, err := strconv.Atoi(sArr[j]); err != nil {
					t.Fatal(err)
				} else {
					runIds = append(runIds, id)
				}
			}
		}
		t.Log("run id's: ", runIds)

		// create list of microdata columns
		aggrCols, e := makeMicroAggrCols(entity, entityGen, groupBy)
		if e != nil {
			t.Fatal("Fail to makeMicroAggrCols:", entityName, ":", groupBy)
		}

		// Translate microdata aggregation into main sql query.
		mainSql, isCompare, e := translateMicroCalcToSql(entity, entityGen, aggrCols, 2*CALCULATED_ID_OFFSET, srcCalc)
		if e != nil {
			t.Fatal(e)
		}
		t.Log("isCompare:", isCompare)

		// Build CTE part of aggregation sql from the list of aggregated attributes.
		cteSql, e := makeMicroCteAggrSql(entity, entityGen, aggrCols, baseRunId, runIds, isCompare)
		if e != nil {
			t.Fatal(e)
		}

		if cteSql != cteValid {
			t.Error("Expected:", cteValid)
			t.Error("****FAIL:", cteSql)
		}
		if mainSql != mainValid {
			t.Error("Expected:", mainValid)
			t.Error("****FAIL:", mainSql)
		}

		if mainSql == mainValid && cteSql == cteValid {
			t.Log("=>", cteSql)
			t.Log("=>", mainSql)
		}
	}
}
