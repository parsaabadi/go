// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// copy model run from database into text json and csv files
func dbToTextRun(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(dbConnStrArgKey), runOpts.String(dbDriverArgKey))
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	if err := db.CheckOpenmppSchemaVersion(srcDb); err != nil {
		return err
	}

	// get model metadata
	modelDef, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = modelDef.Model.Name // set model name: it can be empty and only model digest specified

	// find model run metadata by id, run digest or name
	runId, runDigest, runName := runIdDigestNameFromOptions(runOpts)
	if runId < 0 || runId == 0 && runName == "" && runDigest == "" {
		return errors.New("dbcopy invalid argument(s) run id: " + runOpts.String(runIdArgKey) + ", run name: " + runOpts.String(runNameArgKey) + ", run digest: " + runOpts.String(runDigestArgKey))
	}
	runRow, e := findModelRunByIdDigestName(srcDb, modelDef.Model.ModelId, runId, runDigest, runName)
	if e != nil {
		return e
	}
	if runRow == nil {
		return errors.New("model run not found: " + runOpts.String(runIdArgKey) + " " + runOpts.String(runNameArgKey) + " " + runOpts.String(runDigestArgKey))
	}

	// check is this run belong to the model
	if runRow.ModelId != modelDef.Model.ModelId {
		return errors.New("model run " + strconv.Itoa(runRow.RunId) + " " + runRow.Name + " " + runRow.RunDigest + " does not belong to model " + modelName + " " + modelDigest)
	}

	// run must be completed: status success, error or exit
	if !db.IsRunCompleted(runRow.Status) {
		return errors.New("model run not completed: " + strconv.Itoa(runRow.RunId) + " " + runRow.Name)
	}

	// get full model run metadata
	meta, err := db.GetRunFullText(srcDb, runRow, "")
	if err != nil {
		return err
	}

	// create new "root" output directory for model run metadata
	// for csv files this "root" combined as root/run.1234.runName
	var outDir string
	switch {
	case runId > 0:
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".run."+strconv.Itoa(runId))
	case runDigest != "":
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".run."+helper.CleanPath(runDigest))
	default:
		// if not run id and not digest then run name
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".run."+helper.CleanPath(runRow.Name))
	}

	err = os.MkdirAll(outDir, 0750)
	if err != nil {
		return err
	}

	// use of run and set id's in directory names:
	// do this by default or if use id name = true
	// only if use id name = false then do not use id's in directory names
	isUseIdNames := !runOpts.IsExist(useIdNamesArgKey) || runOpts.Bool(useIdNamesArgKey)

	// write model run metadata into json, parameters and output result values into csv files
	dblFmt := runOpts.String(doubleFormatArgKey)
	isIdCsv := runOpts.Bool(useIdCsvArgKey)
	isWriteUtf8bom := runOpts.Bool(useUtf8CsvArgKey)
	if err = toRunText(srcDb, modelDef, meta, outDir, dblFmt, isIdCsv, isWriteUtf8bom, isUseIdNames); err != nil {
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
	dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, doubleFmt string, isIdCsv bool, isWriteUtf8bom bool, isUseIdNames bool) error {

	// get all successfully completed model runs
	rl, err := db.GetRunFullTextList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// read all run parameters, output accumulators and expressions and dump it into csv files
	for k := range rl {
		err = toRunText(dbConn, modelDef, &rl[k], outDir, doubleFmt, isIdCsv, isWriteUtf8bom, isUseIdNames)
		if err != nil {
			return err
		}
	}
	return nil
}

// toRunText write model run metadata, parameters and output tables into csv files, in separate subdirectory
// by default file name and directory name include run id: modelName.run.1234.RunName
// user can explicitly disable it by IdNames=false
func toRunText(
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	meta *db.RunMeta,
	outDir string,
	doubleFmt string,
	isIdCsv bool,
	isWriteUtf8bom bool,
	isUseIdNames bool) error {

	// convert db rows into "public" format
	runId := meta.Run.RunId
	omppLog.Log("Model run ", runId, " ", meta.Run.Name)

	pub, err := meta.ToPublic(dbConn, modelDef)
	if err != nil {
		return err
	}

	// create run subdir under model dir
	var csvName string
	if !isUseIdNames {
		csvName = "run." + helper.CleanPath(pub.Name)
	} else {
		csvName = "run." + strconv.Itoa(runId) + "." + helper.CleanPath(pub.Name)
	}
	csvDir := filepath.Join(outDir, csvName)

	err = os.MkdirAll(csvDir, 0750)
	if err != nil {
		return err
	}

	paramLt := &db.ReadParamLayout{ReadLayout: db.ReadLayout{FromId: runId}}

	// write all parameters into csv file
	for j := range modelDef.Param {

		paramLt.Name = modelDef.Param[j].Name

		cLst, _, err := db.ReadParameter(dbConn, modelDef, paramLt)
		if err != nil {
			return err
		}
		if cLst.Len() <= 0 { // parameter data must exist for all parameters
			return errors.New("missing run parameter values " + paramLt.Name + " run id: " + strconv.Itoa(paramLt.FromId))
		}

		var pc db.CellParam
		err = toCsvCellFile(
			csvDir, modelDef, paramLt.Name, false, pc, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom, "", "")
		if err != nil {
			return err
		}
	}

	// write all output tables into csv file
	tblLt := &db.ReadTableLayout{ReadLayout: db.ReadLayout{FromId: runId}}

	for j := range modelDef.Table {

		// write output table expression values into csv file
		tblLt.Name = modelDef.Table[j].Name
		tblLt.IsAccum = false
		tblLt.IsAllAccum = false

		cLst, _, err := db.ReadOutputTable(dbConn, modelDef, tblLt)
		if err != nil {
			return err
		}

		var ec db.CellExpr
		err = toCsvCellFile(
			csvDir, modelDef, tblLt.Name, false, ec, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom, "", "")
		if err != nil {
			return err
		}

		// write output table accumulators into csv file
		tblLt.IsAccum = true
		tblLt.IsAllAccum = false

		cLst, _, err = db.ReadOutputTable(dbConn, modelDef, tblLt)
		if err != nil {
			return err
		}

		var ac db.CellAcc
		err = toCsvCellFile(
			csvDir, modelDef, tblLt.Name, false, ac, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom, "", "")
		if err != nil {
			return err
		}

		// write all accumulators view into csv file
		tblLt.IsAccum = true
		tblLt.IsAllAccum = true

		cLst, _, err = db.ReadOutputTable(dbConn, modelDef, tblLt)
		if err != nil {
			return err
		}

		var al db.CellAllAcc
		err = toCsvCellFile(
			csvDir, modelDef, tblLt.Name, false, al, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom, "", "")
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

// check dbcopy run options and return only one of: run id, run digest or name to find model run.
func runIdDigestNameFromOptions(runOpts *config.RunOptions) (int, string, string) {

	// from dbcopy options get model run id and/or run digest and/or run name
	runId := runOpts.Int(runIdArgKey, 0)
	runDigest := runOpts.String(runDigestArgKey)
	runName := runOpts.String(runNameArgKey)

	// conflicting options: use run id if positive else use run digest if not empty else run name
	if runOpts.IsExist(runIdArgKey) && (runOpts.IsExist(runNameArgKey) || runOpts.IsExist(runDigestArgKey)) {
		if runId > 0 {
			if runName != "" {
				omppLog.Log("dbcopy options conflict. Using run id: ", runId, " ignore run name: ", runName)
			}
			if runDigest != "" {
				omppLog.Log("dbcopy options conflict. Using run id: ", runId, " ignore run digest: ", runDigest)
			}
			runName = ""
			runDigest = ""
		} else {
			if runDigest != "" {
				omppLog.Log("dbcopy options conflict. Using run digest: ", runDigest, " ignore run id: ", runId)
				if runName != "" {
					omppLog.Log("dbcopy options conflict. Using run digest: ", runDigest, " ignore run name: ", runName)
					runName = ""
				}
			} else {
				omppLog.Log("dbcopy options conflict. Using run name: ", runName, " ignore run id: ", runId)
			}
			runId = 0
		}
	}
	if runName != "" && runDigest != "" {
		omppLog.Log("dbcopy options conflict. Using run digest: ", runDigest, " ignore run name: ", runName)
		runName = ""
	}

	return runId, runDigest, runName
}

// find model run metadata by id, run digest or name, retun run_lst db row or nil if model run not found.
func findModelRunByIdDigestName(dbConn *sql.DB, modelId, runId int, runDigest, runName string) (*db.RunRow, error) {

	switch {
	case runId > 0:
		return db.GetRun(dbConn, runId)
	case runDigest != "":
		return db.GetRunByDigest(dbConn, runDigest)
	default:
		// if not run id and not digest then run name
		return db.GetRunByName(dbConn, modelId, runName)
	}
}
