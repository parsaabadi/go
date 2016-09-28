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

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	omppLog "go.openmpp.org/ompp/log"
)

// copy model from database into text json and csv files
func dbToText(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(config.DbConnectionStr), runOpts.String(config.DbDriverName))
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	nv, err := db.OpenmppSchemaVersion(srcDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid database, likely not an openM++ database")
	}

	// get model metadata
	modelDef, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = modelDef.Model.Name // set model name: it can be empty and only model digest specified

	// create new output directory, use modelName subdirectory
	outDir := filepath.Join(runOpts.String(outputDirArgKey), modelName)
	err = os.MkdirAll(outDir, 0750)
	if err != nil {
		return err
	}

	// write model definition to json file
	if err = toModelJsonFile(srcDb, modelDef, outDir); err != nil {
		return err
	}

	// write all model run data into csv files: parameters, output expressions and accumulators
	dblFmt := runOpts.String(doubleFmtArgKey)
	if err = toRunTextFileList(srcDb, modelDef, outDir, dblFmt); err != nil {
		return err
	}

	// write all readonly workset data into csv files: input parameters
	if err = toWorksetTextFileList(srcDb, modelDef, outDir, dblFmt); err != nil {
		return err
	}

	// write all modeling tasks and task run history to json files
	if err = toTaskJsonFileList(srcDb, modelDef, outDir); err != nil {
		return err
	}

	// pack model metadata, run results and worksets into zip
	if runOpts.Bool(zipArgKey) {
		zipPath, err := helper.PackZip(outDir, "")
		if err != nil {
			return err
		}
		omppLog.Log("Packed ", zipPath)
	}

	return nil
}

// copy workset from database into text json and csv files
func dbToTextWorkset(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// get workset name and id
	setName := runOpts.String(config.SetName)
	setId := runOpts.Int(config.SetId, 0)

	// conflicting options: use set id if positive else use set name
	if runOpts.IsExist(config.SetName) && runOpts.IsExist(config.SetId) {
		if setId > 0 {
			omppLog.Log("dbcopy options conflict. Using set id: ", setId, " ignore set name: ", setName)
			setName = ""
		} else {
			omppLog.Log("dbcopy options conflict. Using set name: ", setName, " ignore set id: ", setId)
			setId = 0
		}
	}

	if setId < 0 || setId == 0 && setName == "" {
		return errors.New("dbcopy invalid argument(s) for set id: " + runOpts.String(config.SetId) + " and/or set name: " + runOpts.String(config.SetName))
	}

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(config.DbConnectionStr), runOpts.String(config.DbDriverName))
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	nv, err := db.OpenmppSchemaVersion(srcDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid database, likely not an openM++ database")
	}

	// get model metadata
	modelDef, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = modelDef.Model.Name // set model name: it can be empty and only model digest specified

	// get workset metadata by id or name
	var wsRow *db.WorksetRow
	if setId > 0 {
		if wsRow, err = db.GetWorkset(srcDb, setId); err != nil {
			return err
		}
		if wsRow == nil {
			return errors.New("workset not found, set id: " + strconv.Itoa(setId))
		}
	} else {
		if wsRow, err = db.GetWorksetByName(srcDb, modelDef.Model.ModelId, setName); err != nil {
			return err
		}
		if wsRow == nil {
			return errors.New("workset not found: " + setName)
		}
	}

	ws, err := db.GetWorksetFull(srcDb, modelDef, wsRow, "") // get full workset metadata
	if err != nil {
		return err
	}

	// check: workset must be readonly
	if !ws.Set.IsReadonly {
		return errors.New("workset must be readonly: " + strconv.Itoa(wsRow.SetId) + " " + wsRow.Name)
	}

	// create new workset output directory "root"
	// later this "root" combined with set name subdirectory: root/setName
	outDir := ""
	if runOpts.IsExist(config.ParamDir) {
		outDir = filepath.Clean(runOpts.String(config.ParamDir))
	} else {
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName)
	}
	err = os.MkdirAll(outDir, 0750)
	if err != nil {
		return err
	}

	// write all readonly workset data into csv files: input parameters
	dblFmt := runOpts.String(doubleFmtArgKey)
	if err = toWorksetTextFile(srcDb, modelDef, ws, outDir, dblFmt); err != nil {
		return err
	}

	// pack model metadata, run results and worksets into zip
	if runOpts.Bool(zipArgKey) {
		zipPath, err := helper.PackZip(outDir, "")
		if err != nil {
			return err
		}
		omppLog.Log("Packed ", zipPath)
	}

	return nil
}

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

// toRunTextFileList write all model runs parameters and output tables into csv files, each run in separate subdirectory
func toRunTextFileList(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, doubleFmt string) error {

	// get all successfully completed model runs
	rl, err := db.GetRunFullList(dbConn, modelDef, true, "")
	if err != nil {
		return err
	}

	// read all run parameters, output accumulators and expressions and dump it into csv files
	for k := range rl {
		err = toRunTextFile(dbConn, modelDef, &rl[k], outDir, doubleFmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// toRunTextFile write model run metadata, parameters and output tables into csv files, in separate subdirectory
func toRunTextFile(dbConn *sql.DB, modelDef *db.ModelMeta, meta *db.RunMeta, outDir string, doubleFmt string) error {

	// create run subdir under model dir
	runId := meta.Run.RunId
	omppLog.Log("Model run ", runId, " ", meta.Run.Name)

	csvName := "run." + strconv.Itoa(runId) + "." + helper.ToAlphaNumeric(meta.Run.Name)
	csvDir := filepath.Join(outDir, csvName)

	err := os.MkdirAll(csvDir, 0750)
	if err != nil {
		return err
	}

	layout := &db.ReadLayout{FromId: runId}

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

	// save model run metadata into json
	if err := helper.ToJsonFile(filepath.Join(outDir, modelDef.Model.Name+"."+csvName+".json"), meta); err != nil {
		return err
	}
	return nil
}

// toWorksetTextFileList write all readonly worksets into csv files, each set in separate subdirectory
func toWorksetTextFileList(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, doubleFmt string) error {

	// get all readonly worksets
	wl, err := db.GetWorksetFullList(dbConn, modelDef, true, "")
	if err != nil {
		return err
	}

	// read all workset parameters and dump it into csv files
	for k := range wl {
		err = toWorksetTextFile(dbConn, modelDef, &wl[k], outDir, doubleFmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// toWorksetTextFile write workset into csv file, in separate subdirectory
func toWorksetTextFile(dbConn *sql.DB, modelDef *db.ModelMeta, meta *db.WorksetMeta, outDir string, doubleFmt string) error {

	// create workset subdir under output dir
	setId := meta.Set.SetId
	omppLog.Log("Workset ", setId, " ", meta.Set.Name)

	csvName := "set." + strconv.Itoa(setId) + "." + helper.ToAlphaNumeric(meta.Set.Name)
	csvDir := filepath.Join(outDir, csvName)

	err := os.MkdirAll(csvDir, 0750)
	if err != nil {
		return err
	}

	layout := &db.ReadLayout{FromId: setId, IsFromSet: true}

	// write parameter into csv file
	for j := range meta.Param {

		layout.Name = meta.Param[j].Name

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

	// save model workset metadata into json
	if err := helper.ToJsonFile(filepath.Join(outDir, modelDef.Model.Name+"."+csvName+".json"), meta); err != nil {
		return err
	}
	return nil
}

// toTaskJsonFileList convert all successfully completed tasks and tasks run history to json and write into json files
func toTaskJsonFileList(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string) error {

	// get all modeling tasks and successfully completed tasks run history
	tl, err := db.GetTaskFullList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	for k := range tl {
		if err := toTaskJsonFile(dbConn, modelDef, &tl[k], outDir); err != nil {
			return err
		}
	}
	return nil
}

// toTaskJsonFile convert modeling task and task run history to json and write into json file
func toTaskJsonFile(dbConn *sql.DB, modelDef *db.ModelMeta, meta *db.TaskMeta, outDir string) error {

	omppLog.Log("Modeling task ", meta.Task.TaskId, " ", meta.Task.Name)

	err := helper.ToJsonFile(filepath.Join(
		outDir,
		modelDef.Model.Name+".task."+strconv.Itoa(meta.Task.TaskId)+"."+helper.ToAlphaNumeric(meta.Task.Name)+".json"),
		meta)
	return err
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
