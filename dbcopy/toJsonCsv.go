// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"database/sql"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	omppLog "go.openmpp.org/ompp/log"
)

// toModelJsonFile convert model metadata to json and write into json files.
func toModelJsonFile(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string) error {

	// get list of languages
	langDef, err := db.GetLanguages(dbConn)
	if err != nil {
		return err
	}

	// get model text (description and notes) in all languages
	modelTxt, err := db.GetModelText(dbConn, modelDef.Model.ModelId, "")
	if err != nil {
		return err
	}

	// get model parameter and output table groups (description and notes) in all languages
	modelGroup, err := db.GetModelGroup(dbConn, modelDef.Model.ModelId, "")
	if err != nil {
		return err
	}

	// get model profile: default model profile is profile where name = model name
	modelName := modelDef.Model.Name
	modelProfile, err := db.GetProfile(dbConn, modelName)
	if err != nil {
		return err
	}

	// save into model json files
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".model.json"), &modelDef); err != nil {
		return err
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".lang.json"), &langDef); err != nil {
		return err
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".text.json"), &modelTxt); err != nil {
		return err
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".group.json"), &modelGroup); err != nil {
		return err
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelName+".profile.json"), &modelProfile); err != nil {
		return err
	}
	return nil
}

// toTaskJsonFile convert modeling tasks and tasks run history to json and write into json files
func toTaskJsonFile(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string) error {

	// get all modeling tasks and successfully completed tasks run history
	taskLst, err := db.GetTaskList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// save tasks and tasks run history into json
	if taskLst != nil {
		if len(taskLst.Lst) > 0 {
			omppLog.Log("Modeling tasks: ", len(taskLst.Lst))
		}
	}
	if err := helper.ToJsonFile(filepath.Join(outDir, modelDef.Model.Name+".task.json"), taskLst); err != nil {
		return err
	}
	return nil
}

// toCsvRuns write all model runs parameters and output tables into csv files, each run in separate subdirectory
func toCsvRunFile(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, doubleFmt string) error {

	// get all successfully completed model runs
	rl, err := db.GetRunList(dbConn, modelDef, true, "")
	if err != nil {
		return err
	}

	// read all run parameters, output accumulators and expressions and dump it into csv files
	for k := range rl.Lst {

		// create run subdir under model dir
		omppLog.Log("Model run ", rl.Lst[k].Run.RunId)

		csvDir := filepath.Join(outDir, "run_"+strconv.Itoa(rl.Lst[k].Run.RunId))
		err = os.MkdirAll(csvDir, 0750)
		if err != nil {
			return err
		}

		layout := &db.ReadLayout{FromId: rl.Lst[k].Run.RunId}

		// write all parameters into csv file
		for j := range modelDef.Param {

			layout.Name = modelDef.Param[j].Name

			cLst, err := db.ReadParameter(dbConn, modelDef, layout)
			if err != nil {
				return err
			}
			if cLst.Len() <= 0 { // parameter data must exist for all parameters
				return errors.New("missing run parameter values " + layout.Name + " run id: " + strconv.Itoa(layout.FromId))
			}

			var cp db.Cell
			err = toCsvFile(csvDir, modelDef, modelDef.Param[j].Name, cp, cLst, doubleFmt)
			if err != nil {
				return err
			}
		}

		// write all output tables into csv file
		for j := range modelDef.Table {

			// write output table expression values into csv file
			layout.Name = modelDef.Table[j].Name
			layout.IsAccum = false

			cLst, err := db.ReadOutputTable(dbConn, modelDef, layout)
			if err != nil {
				return err
			}

			var ec db.CellExpr
			err = toCsvFile(csvDir, modelDef, modelDef.Table[j].Name, ec, cLst, doubleFmt)
			if err != nil {
				return err
			}

			// write output table accumulators into csv file
			layout.IsAccum = true

			cLst, err = db.ReadOutputTable(dbConn, modelDef, layout)
			if err != nil {
				return err
			}

			var ac db.CellAcc
			err = toCsvFile(csvDir, modelDef, modelDef.Table[j].Name, ac, cLst, doubleFmt)
			if err != nil {
				return err
			}
		}
	}

	// save model runs into json
	if err := helper.ToJsonFile(filepath.Join(outDir, modelDef.Model.Name+".run.json"), &rl); err != nil {
		return err
	}

	return nil
}

// toCsvWorksets write all readonly worksets into csv files, each set in separate subdirectory
func toCsvWorksetFile(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, doubleFmt string) error {

	// get all readonly worksets
	wl, err := db.GetWorksetList(dbConn, modelDef, true, "")
	if err != nil {
		return err
	}

	// read all workset parameters and dump it into csv files
	for k := range wl.Lst {

		// create workset subdir under model dir
		setId := wl.Lst[k].Set.SetId
		omppLog.Log("Workset ", setId)

		csvDir := filepath.Join(outDir, "set_"+strconv.Itoa(setId))
		err = os.MkdirAll(csvDir, 0750)
		if err != nil {
			return err
		}

		layout := &db.ReadLayout{FromId: setId, IsFromSet: true}

		// write parameter into csv file
		for j := range wl.Lst[k].Param {

			layout.Name = wl.Lst[k].Param[j].Name

			cLst, err := db.ReadParameter(dbConn, modelDef, layout)
			if err != nil {
				return err
			}
			if cLst.Len() <= 0 { // parameter data must exist for all parameters
				return errors.New("missing workset parameter values " + layout.Name + " set id: " + strconv.Itoa(layout.FromId))
			}

			var cp db.Cell
			err = toCsvFile(csvDir, modelDef, modelDef.Param[j].Name, cp, cLst, doubleFmt)
			if err != nil {
				return err
			}
		}
	}

	// save model worksets into json
	if err := helper.ToJsonFile(filepath.Join(outDir, modelDef.Model.Name+".set.json"), wl); err != nil {
		return err
	}
	return nil
}

// toCsvFile convert parameter or output table values and write into csvDir/fileName.csv file.
func toCsvFile(
	csvDir string, modelDef *db.ModelMeta, name string, cell db.CsvConverter, cellLst *list.List, doubleFmt string) error {

	// converter from db cell to csv row []string
	cvt, err := cell.CsvToRow(modelDef, name, doubleFmt)
	if err != nil {
		return err
	}

	// create csv file
	fn, err := cell.CsvFileName(modelDef, name)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(filepath.Join(csvDir, fn), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	wr := csv.NewWriter(f)

	// write header line: column names
	cs, err := cell.CsvHeader(modelDef, name)
	if err != nil {
		return err
	}
	if err = wr.Write(cs); err != nil {
		return err
	}

	for c := cellLst.Front(); c != nil; c = c.Next() {

		// write cell line: dimension(s) and value
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
