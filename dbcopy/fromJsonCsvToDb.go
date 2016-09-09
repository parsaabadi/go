// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"database/sql"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	omppLog "go.openmpp.org/ompp/log"
)

// fromModelJsonToDb reads model metadata from json file and insert it into database.
func fromModelJsonToDb(dbConn *sql.DB, dbFacet db.Facet, inpDir string, modelName string) (*db.ModelMeta, error) {

	// restore model metadta
	var modelDef db.ModelMeta
	isExist, err := helper.FromJsonFile(filepath.Join(inpDir, modelName+".model.json"), &modelDef)
	if err != nil {
		return nil, err
	}
	if !isExist {
		return nil, errors.New("model not found: " + modelName)
	}
	if modelDef.Model.Name != modelName {
		return nil, errors.New("model name: " + modelName + " not found in .json file")
	}
	if err = modelDef.Setup(); err != nil {
		return nil, err
	}

	// insert model metadata into destination database if not exists
	if err = db.UpdateModel(dbConn, dbFacet, &modelDef); err != nil {
		return nil, err
	}

	// insert, update or delete model default profile
	var modelProfile db.ProfileMeta
	isExist, err = helper.FromJsonFile(filepath.Join(inpDir, modelName+".profile.json"), &modelProfile)
	if err != nil {
		return nil, err
	}
	if isExist && modelProfile.Name == modelName { // if this is profile default model profile then do update
		if err = db.UpdateProfile(dbConn, &modelProfile); err != nil {
			return nil, err
		}
	}

	return &modelDef, nil
}

// fromLangTextJsonToDb reads languages, model text and model groups from json file and insert it into database.
func fromLangTextJsonToDb(dbConn *sql.DB, modelDef *db.ModelMeta, inpDir string) (*db.LangList, error) {

	// restore language list from json and if exist then update db tables
	langDef := &db.LangList{}
	isExist, err := helper.FromJsonFile(filepath.Join(inpDir, modelDef.Model.Name+".lang.json"), langDef)
	if err != nil {
		return nil, err
	}
	if isExist {
		langDef.Setup()
		if err = db.UpdateLanguage(dbConn, langDef); err != nil {
			return nil, err
		}
	}

	// get full list of languages
	langDef, err = db.GetLanguages(dbConn)
	if err != nil {
		return nil, err
	}

	// restore text data from json and if exist then update db tables
	var modelTxt db.ModelTxtMeta
	isExist, err = helper.FromJsonFile(filepath.Join(inpDir, modelDef.Model.Name+".text.json"), &modelTxt)
	if err != nil {
		return nil, err
	}
	if isExist {
		if err = db.UpdateModelText(dbConn, modelDef, langDef, &modelTxt); err != nil {
			return nil, err
		}
	}

	// restore model groups and groups text (description, notes) from json and if exist then update db tables
	var modelGroup db.GroupMeta
	isExist, err = helper.FromJsonFile(filepath.Join(inpDir, modelDef.Model.Name+".group.json"), &modelGroup)
	if err != nil {
		return nil, err
	}
	if isExist {
		if err = db.UpdateModelGroup(dbConn, modelDef, langDef, &modelGroup); err != nil {
			return nil, err
		}
	}

	return langDef, nil
}

// fromTaskJsonToDb reads modeling tasks and tasks run history from json file and insert it into database.
func fromTaskJsonToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangList, inpDir string, runIdMap map[int]int, setIdMap map[int]int) error {

	var tl db.TaskList
	isExist, err := helper.FromJsonFile(filepath.Join(inpDir, modelDef.Model.Name+".task.json"), &tl)
	if err != nil {
		return err
	}
	if isExist && len(tl.Lst) > 0 {
		omppLog.Log("Modeling tasks: ", len(tl.Lst))

		if err = db.UpdateTaskList(dbConn, modelDef, langDef, &tl, runIdMap, setIdMap); err != nil {
			return err
		}
	}
	return nil
}

// fromCsvRunToDb read all model runs (parameters, output tables, modeling tasks) from csv and json files,
// convert it to db cells and insert into database
func fromCsvRunToDb(dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangList, inpDir string) (map[int]int, error) {

	// get model run metadata
	var rl db.RunList
	isExist, err := helper.FromJsonFile(filepath.Join(inpDir, modelDef.Model.Name+".run.json"), &rl)
	if err != nil {
		return nil, err
	}
	if !isExist || len(rl.Lst) <= 0 {
		return make(map[int]int), nil // exit if no model runs
	}

	// save model run list
	// update incoming run id's with actual new run id created in database
	runIdMap, err := db.UpdateRunList(dbConn, modelDef, langDef, &rl)
	if err != nil {
		return nil, err
	}

	// read all run parameters, output accumulators and expressions from csv files
	for k := range rl.Lst {

		// check if run subdir exist
		srcId := idKeyByValue(rl.Lst[k].Run.RunId, runIdMap)
		omppLog.Log("Model run from ", srcId, " to ", rl.Lst[k].Run.RunId)

		csvDir := filepath.Join(inpDir, "run_"+strconv.Itoa(srcId))
		if _, err := os.Stat(csvDir); err != nil {
			return nil, errors.New("run directory not found: " + strconv.Itoa(srcId) + " " + rl.Lst[k].Run.Name)
		}

		layout := db.WriteLayout{ToId: rl.Lst[k].Run.RunId, IsToRun: true}

		// restore run parameters: all model parameters must be included in the run
		for j := range modelDef.Param {

			// read parameter values from csv file
			var cell db.Cell
			cLst, err := fromCsvFile(csvDir, modelDef, modelDef.Param[j].Name, &cell)
			if err != nil {
				return nil, err
			}
			if cLst == nil || cLst.Len() <= 0 {
				return nil, errors.New("run: " + strconv.Itoa(srcId) + " " + rl.Lst[k].Run.Name + " parameter empty: " + modelDef.Param[j].Name)
			}

			// insert parameter values in model run
			layout.Name = modelDef.Param[j].Name

			if err = db.WriteParameter(dbConn, modelDef, &layout, cLst); err != nil {
				return nil, err
			}
		}

		// restore run output tables accumulators and expressions
		for j := range modelDef.Table {

			// read output table accumulator(s) values from csv file
			var ca db.CellAcc
			acLst, err := fromCsvFile(csvDir, modelDef, modelDef.Table[j].Name, &ca)
			if err != nil {
				return nil, err
			}

			// insert accumulator(s) values in model run
			layout.Name = modelDef.Table[j].Name
			layout.IsAccum = true

			if err = db.WriteOutputTable(dbConn, modelDef, &layout, acLst); err != nil {
				return nil, err
			}

			// read output table expression(s) values from csv file
			var ce db.CellExpr
			ecLst, err := fromCsvFile(csvDir, modelDef, modelDef.Table[j].Name, &ce)
			if err != nil {
				return nil, err
			}

			// insert expression(s) values in model run
			layout.IsAccum = false

			if err = db.WriteOutputTable(dbConn, modelDef, &layout, ecLst); err != nil {
				return nil, err
			}
		}
	}

	return runIdMap, nil
}

// fromCsvWorksetToDb read all worksets parameters from csv and json files,
// convert it to db cells and insert into database
// update set id's and base run id's with actual id in database
func fromCsvWorksetToDb(dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangList, inpDir string, runIdMap map[int]int) (map[int]int, error) {

	// get workset metadata
	var wl db.WorksetList
	isExist, err := helper.FromJsonFile(filepath.Join(inpDir, modelDef.Model.Name+".set.json"), &wl)
	if err != nil {
		return nil, err
	}
	if !isExist || len(wl.Lst) <= 0 {
		return make(map[int]int), nil // no worksets
	}

	// save model set list
	// update incoming set id's with actual new set id created in database
	// update incoming base run id's with actual run id in database
	setIdMap, err := db.UpdateWorksetList(dbConn, modelDef, langDef, runIdMap, &wl)
	if err != nil {
		return nil, err
	}

	// read all workset parameters from csv files
	for k := range wl.Lst {

		// check if workset subdir exist
		srcId := idKeyByValue(wl.Lst[k].Set.SetId, setIdMap)
		omppLog.Log("Workset from ", srcId, " to ", wl.Lst[k].Set.SetId)

		csvDir := filepath.Join(inpDir, "set_"+strconv.Itoa(srcId))
		if _, err := os.Stat(csvDir); err != nil {
			return nil, errors.New("workset directory not found: " + strconv.Itoa(srcId) + " " + wl.Lst[k].Set.Name)
		}

		// restore workset parameters
		layout := db.WriteLayout{ToId: wl.Lst[k].Set.SetId}

		for j := range wl.Lst[k].Param {

			// read parameter values from csv file
			var cell db.Cell
			cLst, err := fromCsvFile(csvDir, modelDef, wl.Lst[k].Param[j].Name, &cell)
			if err != nil {
				return nil, err
			}
			if cLst == nil || cLst.Len() <= 0 {
				return nil, errors.New("workset: " + strconv.Itoa(srcId) + " " + wl.Lst[k].Set.Name + " parameter empty: " + wl.Lst[k].Param[j].Name)
			}

			// insert or update parameter values in workset
			layout.Name = wl.Lst[k].Param[j].Name

			err = db.WriteParameter(dbConn, modelDef, &layout, cLst)
			if err != nil {
				return nil, err
			}
		}
	}

	return setIdMap, nil
}

// fromCsvFile read parameter or output table csv file and convert it to list of db cells
func fromCsvFile(csvDir string, modelDef *db.ModelMeta, name string, cell db.CsvConverter) (*list.List, error) {

	// converter from csv row []string to db cell
	cvt, err := cell.CsvToCell(modelDef, name)
	if err != nil {
		return nil, err
	}

	// open csv file
	fn, err := cell.CsvFileName(modelDef, name)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(filepath.Join(csvDir, fn))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rd := csv.NewReader(f)
	rd.TrimLeadingSpace = true

	// read csv file and convert and append lines into cell list
	cLst := list.New()
	isFirst := true
ReadFor:
	for {
		row, err := rd.Read()
		switch {
		case err == io.EOF:
			break ReadFor
		case err != nil:
			return nil, err
		}

		// skip header line
		if isFirst {
			isFirst = false
			continue
		}

		// convert and append cell to cell list
		c, err := cvt(row)
		if err != nil {
			return nil, err
		}
		cLst.PushBack(c)
	}

	return cLst, nil
}

// return key id by value id from in (key, value) idMap or -1 if not found
func idKeyByValue(idValue int, idMap map[int]int) int {
	for key, val := range idMap {
		if val == idValue {
			return key
		}
	}
	return -1
}
