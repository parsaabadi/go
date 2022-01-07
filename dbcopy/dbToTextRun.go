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
	cs, dn := db.IfEmptyMakeDefaultReadOnly(modelName, runOpts.String(fromSqliteArgKey), runOpts.String(dbConnStrArgKey), runOpts.String(dbDriverArgKey))

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
	runId, runDigest, runName, isFirst, isLast := runIdDigestNameFromOptions(runOpts)
	if runId < 0 || runId == 0 && runName == "" && runDigest == "" && !isFirst && !isLast {
		return errors.New("dbcopy invalid argument(s) run id: " + runOpts.String(runIdArgKey) + ", run name: " + runOpts.String(runNameArgKey) + ", run digest: " + runOpts.String(runDigestArgKey))
	}
	runRow, e := findModelRunByIdDigestName(srcDb, modelDef.Model.ModelId, runId, runDigest, runName, isFirst, isLast)
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
	var outDir, csvName string
	switch {
	case runId > 0:
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".run."+strconv.Itoa(runId))
	case runDigest != "":
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".run."+helper.CleanPath(runDigest))
	case runName == "" && isFirst:
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".first.run")
		csvName = "first.run"
	case runName == "" && isLast:
		outDir = filepath.Join(runOpts.String(outputDirArgKey), modelName+".last.run")
		csvName = "last.run"
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
	isWriteAccum := !runOpts.Bool(noAccumCsv)

	if err = toRunText(srcDb, modelDef, meta, outDir, csvName, dblFmt, isIdCsv, isWriteUtf8bom, isUseIdNames, isWriteAccum); err != nil {
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
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	outDir string,
	doubleFmt string,
	isIdCsv bool,
	isWriteUtf8bom bool,
	isUseIdNames bool,
	isWriteAccum bool) error {

	// get all successfully completed model runs
	rl, err := db.GetRunFullTextList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// read all run parameters, output accumulators and expressions and dump it into csv files
	for k := range rl {
		err = toRunText(dbConn, modelDef, &rl[k], outDir, "", doubleFmt, isIdCsv, isWriteUtf8bom, isUseIdNames, isWriteAccum)
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
	csvName string,
	doubleFmt string,
	isIdCsv bool,
	isWriteUtf8bom bool,
	isUseIdNames bool,
	isWriteAccum bool) error {

	// convert db rows into "public" format
	runId := meta.Run.RunId
	omppLog.Log("Model run ", runId, " ", meta.Run.Name)

	pub, err := meta.ToPublic(dbConn, modelDef)
	if err != nil {
		return err
	}

	// create run subdir under model dir
	switch {
	case csvName == "" && !isUseIdNames:
		csvName = "run." + helper.CleanPath(pub.Name)
	case csvName == "" && isUseIdNames:
		csvName = "run." + strconv.Itoa(runId) + "." + helper.CleanPath(pub.Name)
	}
	csvDir := filepath.Join(outDir, csvName)

	err = os.MkdirAll(csvDir, 0750)
	if err != nil {
		return err
	}

	paramLt := &db.ReadParamLayout{ReadLayout: db.ReadLayout{FromId: runId}}
	cvtParam := db.CellParamConverter{DoubleFmt: doubleFmt}

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

		err = toCsvCellFile(
			csvDir, modelDef, paramLt.Name, false, cvtParam, cLst, isIdCsv, isWriteUtf8bom, "", "")
		if err != nil {
			return err
		}
	}

	// write output tables into csv file, if the table included in run results
	tblLt := &db.ReadTableLayout{ReadLayout: db.ReadLayout{FromId: runId}}
	cvtExpr := db.CellExprConverter{DoubleFmt: doubleFmt, IsIdHeader: isIdCsv}
	cvtAcc := db.CellAccConverter{DoubleFmt: doubleFmt, IsIdHeader: isIdCsv}
	cvtAll := db.CellAllAccConverter{DoubleFmt: doubleFmt, ValueName: ""}

	for j := range modelDef.Table {

		// check if table exist in model run results
		var isFound bool
		for k := range meta.Table {
			isFound = meta.Table[k].TableHid == modelDef.Table[j].TableHid
			if isFound {
				break
			}
		}
		if !isFound {
			continue // skip table: it is suppressed and not in run results
		}

		// write output table expression values into csv file
		tblLt.Name = modelDef.Table[j].Name
		tblLt.IsAccum = false
		tblLt.IsAllAccum = false

		cLst, _, err := db.ReadOutputTable(dbConn, modelDef, tblLt)
		if err != nil {
			return err
		}

		err = toCsvCellFile(
			csvDir, modelDef, tblLt.Name, false, cvtExpr, cLst, isIdCsv, isWriteUtf8bom, "", "")
		if err != nil {
			return err
		}

		// write output table accumulators into csv file
		if isWriteAccum {

			tblLt.IsAccum = true
			tblLt.IsAllAccum = false

			cLst, _, err = db.ReadOutputTable(dbConn, modelDef, tblLt)
			if err != nil {
				return err
			}

			err = toCsvCellFile(
				csvDir, modelDef, tblLt.Name, false, cvtAcc, cLst, isIdCsv, isWriteUtf8bom, "", "")
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

			err = toCsvCellFile(
				csvDir, modelDef, tblLt.Name, false, cvtAll, cLst, isIdCsv, isWriteUtf8bom, "", "")
			if err != nil {
				return err
			}
		}
	}

	// save model run metadata into json
	if err := helper.ToJsonFile(filepath.Join(outDir, modelDef.Model.Name+"."+csvName+".json"), pub); err != nil {
		return err
	}
	return nil
}

// check dbcopy run options and return only one of: run id, run digest or name to find model run.
func runIdDigestNameFromOptions(runOpts *config.RunOptions) (int, string, string, bool, bool) {

	// from dbcopy options get model run id and/or run digest and/or run name
	runId := runOpts.Int(runIdArgKey, 0)
	runDigest := runOpts.String(runDigestArgKey)
	runName := runOpts.String(runNameArgKey)
	isFirst := runOpts.Bool(runFirstArgKey)
	isLast := runOpts.Bool(runLastArgKey)

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
	if isFirst && isLast {
		omppLog.Log("dbcopy options conflict: '-", runFirstArgKey, "' flag should not be combined with '-", runLastArgKey)
		isFirst = false
	}
	if isFirst && (runId > 0 || runDigest != "") {
		omppLog.Log("dbcopy options conflict: '-", runFirstArgKey, "' flag should not be combined with '-", runIdArgKey, "' or '-", runDigestArgKey, "'")
		isFirst = false
	}
	if isLast && (runId > 0 || runDigest != "") {
		omppLog.Log("dbcopy options conflict: '-", runLastArgKey, "' flag should not be combined with '-", runIdArgKey, "' or '-", runDigestArgKey, "'")
		isLast = false
	}
	if isLast && (runId > 0 || runDigest != "") {
		omppLog.Log("dbcopy options conflict: '-", runLastArgKey, "' flag should not be combined with '-", runIdArgKey, "' or '-", runDigestArgKey, "'")
		isLast = false
	}

	return runId, runDigest, runName, isFirst, isLast
}

// find model run metadata by id, run digest, run name or last run, retun run_lst db row or nil if model run not found.
func findModelRunByIdDigestName(dbConn *sql.DB, modelId, runId int, runDigest, runName string, isFirst, isLast bool) (*db.RunRow, error) {

	switch {
	case runId > 0:
		return db.GetRun(dbConn, runId)
	case runDigest != "":
		return db.GetRunByDigest(dbConn, runDigest)
	case isLast && runName != "":
		return db.GetLastRunByName(dbConn, modelId, runName)
	case isLast && runName == "":
		return db.GetLastRun(dbConn, modelId)
	case isFirst && runName == "":
		return db.GetFirstRun(dbConn, modelId)
	default:
		// if not run id and not run digest and not last run and not first run any name
		// then first run by name
		return db.GetRunByName(dbConn, modelId, runName)
	}
}
