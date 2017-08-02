// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// copy model run from database into text json and csv files
func dbToTextRun(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// get model run name and id
	runName := runOpts.String(runNameArgKey)
	runId := runOpts.Int(runIdArgKey, 0)

	// conflicting options: use run id if positive else use run name
	if runOpts.IsExist(runNameArgKey) && runOpts.IsExist(runIdArgKey) {
		if runId > 0 {
			omppLog.Log("dbcopy options conflict. Using run id: ", runId, " ignore run name: ", runName)
			runName = ""
		} else {
			omppLog.Log("dbcopy options conflict. Using run name: ", runName, " ignore run id: ", runId)
			runId = 0
		}
	}

	if runId < 0 || runId == 0 && runName == "" {
		return errors.New("dbcopy invalid argument(s) for run id: " + runOpts.String(runIdArgKey) + " and/or run name: " + runOpts.String(runNameArgKey))
	}

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(dbConnStrArgKey), runOpts.String(dbDriverArgKey))
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

	// get model run metadata by id or name
	var runRow *db.RunRow
	var outDir string
	if runId > 0 {
		if runRow, err = db.GetRun(srcDb, runId); err != nil {
			return err
		}
		if runRow == nil {
			return errors.New("model run not found, id: " + strconv.Itoa(runId))
		}
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".run."+strconv.Itoa(runId))
	} else {
		if runRow, err = db.GetRunByName(srcDb, modelDef.Model.ModelId, runName); err != nil {
			return err
		}
		if runRow == nil {
			return errors.New("model run not found: " + runName)
		}
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".run."+runName)
	}

	// run must be completed: status success, error or exit
	if runRow.Status != db.DoneRunStatus && runRow.Status != db.ExitRunStatus && runRow.Status != db.ErrorRunStatus {
		return errors.New("model run not completed: " + strconv.Itoa(runRow.RunId) + " " + runRow.Name)
	}

	// get full model run metadata
	meta, err := db.GetRunFull(srcDb, runRow, "")
	if err != nil {
		return err
	}

	// create new "root" output directory for model run metadata
	// for csv files this "root" combined as root/run.1234.runName
	err = os.MkdirAll(outDir, 0750)
	if err != nil {
		return err
	}

	// write model run metadata into json, parameters and output result values into csv files
	dblFmt := runOpts.String(doubleFormatArgKey)
	isIdCsv := runOpts.Bool(useIdCsvArgKey)
	isWriteUtf8bom := runOpts.Bool(useUtf8CsvArgKey)
	if err = toRunText(srcDb, modelDef, meta, outDir, dblFmt, isIdCsv, isWriteUtf8bom); err != nil {
		return err
	}

	// pack model run metadata and results into zip
	if runOpts.Bool(zipArgKey) {
		zipPath, err := helper.PackZip(outDir, "")
		if err != nil {
			return err
		}
		omppLog.Log("Packed ", zipPath)
	}

	return nil
}

// toRunListText write all model runs parameters and output tables into csv files, each run in separate subdirectory
func toRunListText(
	dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, doubleFmt string, isIdCsv bool, isWriteUtf8bom bool) error {

	// get all successfully completed model runs
	rl, err := db.GetRunFullList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// read all run parameters, output accumulators and expressions and dump it into csv files
	for k := range rl {
		err = toRunText(dbConn, modelDef, &rl[k], outDir, doubleFmt, isIdCsv, isWriteUtf8bom)
		if err != nil {
			return err
		}
	}
	return nil
}

// toRunText write model run metadata, parameters and output tables into csv files, in separate subdirectory
func toRunText(
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	meta *db.RunMeta,
	outDir string,
	doubleFmt string,
	isIdCsv bool,
	isWriteUtf8bom bool) error {

	// convert db rows into "public" format
	runId := meta.Run.RunId
	omppLog.Log("Model run ", runId, " ", meta.Run.Name)

	pub, err := meta.ToPublic(dbConn, modelDef)
	if err != nil {
		return err
	}

	// create run subdir under model dir
	csvName := "run." + strconv.Itoa(runId) + "." + helper.ToAlphaNumeric(pub.Name)
	csvDir := filepath.Join(outDir, csvName)

	err = os.MkdirAll(csvDir, 0750)
	if err != nil {
		return err
	}

	paramLt := &db.ReadParamLayout{ReadLayout: db.ReadLayout{FromId: runId}}

	// write all parameters into csv file
	for j := range modelDef.Param {

		paramLt.Name = modelDef.Param[j].Name

		cLst, err := db.ReadParameter(dbConn, modelDef, paramLt)
		if err != nil {
			return err
		}
		if cLst.Len() <= 0 { // parameter data must exist for all parameters
			return errors.New("missing run parameter values " + paramLt.Name + " run id: " + strconv.Itoa(paramLt.FromId))
		}

		var pc db.CellParam
		err = toCsvCellFile(csvDir, modelDef, paramLt.Name, pc, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom)
		if err != nil {
			return err
		}
	}

	// write all output tables into csv file
	tblLt := &db.ReadOutTableLayout{ReadLayout: db.ReadLayout{FromId: runId}}

	for j := range modelDef.Table {

		// write output table expression values into csv file
		tblLt.Name = modelDef.Table[j].Name
		tblLt.IsAccum = false
		tblLt.IsAllAccum = false

		cLst, err := db.ReadOutputTable(dbConn, modelDef, tblLt)
		if err != nil {
			return err
		}

		var ec db.CellExpr
		err = toCsvCellFile(csvDir, modelDef, tblLt.Name, ec, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom)
		if err != nil {
			return err
		}

		// write output table accumulators into csv file
		tblLt.IsAccum = true
		tblLt.IsAllAccum = false

		cLst, err = db.ReadOutputTable(dbConn, modelDef, tblLt)
		if err != nil {
			return err
		}

		var ac db.CellAcc
		err = toCsvCellFile(csvDir, modelDef, tblLt.Name, ac, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom)
		if err != nil {
			return err
		}

		// write all accumulators view into csv file
		tblLt.IsAccum = true
		tblLt.IsAllAccum = true

		cLst, err = db.ReadOutputTable(dbConn, modelDef, tblLt)
		if err != nil {
			return err
		}

		var al db.CellAllAcc
		err = toCsvCellFile(csvDir, modelDef, tblLt.Name, al, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom)
		if err != nil {
			return err
		}
	}

	// save model run metadata into json
	if err := helper.ToJsonFile(filepath.Join(outDir, modelDef.Model.Name+"."+csvName+".json"), pub); err != nil {
		return err
	}
	return nil
}
