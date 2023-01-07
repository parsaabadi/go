// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// toRunCsv write model run metadata, parameters and output tables into csv files, in separate subdirectory
func toRunCsv(
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	meta *db.RunMeta,
	outDir string,
	doubleFmt string,
	isIdCsv bool,
	isWriteUtf8bom bool,
	isUseIdNames bool,
	isNextRun bool,
	isAllInOne bool,
	isWriteAcc bool,
	isWriteMicro bool) error {

	// create run subdir under model dir
	runId := meta.Run.RunId
	omppLog.Log("Model run ", runId, " ", meta.Run.Name)

	// make output directory as one of:
	// all_model_runs, run.Name_Of_the_Run, run.NN.Name_Of_the_Run
	var csvTop string
	if isAllInOne {
		csvTop = filepath.Join(outDir, "all_model_runs")
	} else {
		if !isUseIdNames {
			csvTop = filepath.Join(outDir, "run."+helper.CleanPath(meta.Run.Name))
		} else {
			csvTop = filepath.Join(outDir, "run."+strconv.Itoa(runId)+"."+helper.CleanPath(meta.Run.Name))
		}
	}
	paramCsvDir := filepath.Join(csvTop, "parameters")
	tableCsvDir := filepath.Join(csvTop, "output-tables")
	microCsvDir := filepath.Join(csvTop, "microdata")
	nMd := len(meta.EntityGen)

	err := os.MkdirAll(paramCsvDir, 0750)
	if err != nil {
		return err
	}
	err = os.MkdirAll(tableCsvDir, 0750)
	if err != nil {
		return err
	}
	if isWriteMicro && nMd > 0 {
		err = os.MkdirAll(microCsvDir, 0750)
		if err != nil {
			return err
		}
	}

	// if this is "all-in-one" output then first column is run id or run name
	var firstCol, firstVal string
	if isAllInOne {
		if isIdCsv {
			firstCol = "run_id"
			firstVal = strconv.Itoa(runId)
		} else {
			firstCol = "run_name"
			firstVal = meta.Run.Name
		}
	}

	// write all parameters into csv file
	nP := len(modelDef.Param)
	omppLog.Log("  Parameters: ", nP)
	logT := time.Now().Unix()

	for j := 0; j < nP; j++ {

		cvtParam := &db.CellParamConverter{
			ModelDef:  modelDef,
			Name:      modelDef.Param[j].Name,
			IsIdCsv:   isIdCsv,
			DoubleFmt: doubleFmt,
		}
		paramLt := db.ReadParamLayout{ReadLayout: db.ReadLayout{
			Name:   modelDef.Param[j].Name,
			FromId: runId,
		}}

		logT = omppLog.LogIfTime(logT, logPeriod, "    ", j, " of ", nP, ": ", paramLt.Name)

		err = toCellCsvFile(dbConn, modelDef, paramLt, cvtParam, isNextRun && isAllInOne, paramCsvDir, isWriteUtf8bom, firstCol, firstVal)
		if err != nil {
			return err
		}
	}

	// write each run parameter value notes into parameterName.LANG.md file
	if !isAllInOne {
		for j := range meta.Param {

			paramName := ""
			for i := range meta.Param[j].Txt {

				if meta.Param[j].Txt[i].LangCode != "" && meta.Param[j].Txt[i].Note != "" {

					// find parameter by name if this is a first note for that parameter
					if paramName == "" {
						k, ok := modelDef.ParamByHid(meta.Param[j].ParamHid)
						if !ok {
							return errors.New("parameter not found by Hid: " + strconv.Itoa(meta.Param[j].ParamHid))
						}
						paramName = modelDef.Param[k].Name
					}

					// write notes into parameterName.LANG.md file
					err = toDotMdFile(
						paramCsvDir,
						paramName+"."+meta.Param[j].Txt[i].LangCode,
						isWriteUtf8bom, meta.Param[j].Txt[i].Note)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	// write output tables into csv file, if the table included in run results
	nT := len(modelDef.Table)
	omppLog.Log("  Tables: ", nT)

	for j := 0; j < nT; j++ {

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
		tblLt := db.ReadTableLayout{
			ReadLayout: db.ReadLayout{
				Name:   modelDef.Table[j].Name,
				FromId: runId,
			},
		}
		ctc := db.CellTableConverter{
			ModelDef: modelDef,
			Name:     modelDef.Table[j].Name,
		}
		cvtExpr := &db.CellExprConverter{CellTableConverter: ctc, IsIdCsv: isIdCsv, DoubleFmt: doubleFmt}
		cvtAcc := &db.CellAccConverter{CellTableConverter: ctc, IsIdCsv: isIdCsv, DoubleFmt: doubleFmt}
		cvtAll := &db.CellAllAccConverter{CellTableConverter: ctc, IsIdCsv: isIdCsv, DoubleFmt: doubleFmt, ValueName: ""}

		logT = omppLog.LogIfTime(logT, logPeriod, "    ", j, " of ", nT, ": ", tblLt.Name)

		err = toCellCsvFile(dbConn, modelDef, tblLt, cvtExpr, isNextRun && isAllInOne, tableCsvDir, isWriteUtf8bom, firstCol, firstVal)
		if err != nil {
			return err
		}

		// write output table accumulators into csv file
		if isWriteAcc {

			tblLt.IsAccum = true
			tblLt.IsAllAccum = false

			logT = omppLog.LogIfTime(logT, logPeriod, "    ", j, " of ", nT, ": ", tblLt.Name, " accumulators")

			err = toCellCsvFile(dbConn, modelDef, tblLt, cvtAcc, isNextRun && isAllInOne, tableCsvDir, isWriteUtf8bom, firstCol, firstVal)
			if err != nil {
				return err
			}

			// write all accumulators view into csv file
			tblLt.IsAccum = true
			tblLt.IsAllAccum = true

			logT = omppLog.LogIfTime(logT, logPeriod, "    ", j, " of ", nT, ": ", tblLt.Name, " all accumulators")

			err = toCellCsvFile(dbConn, modelDef, tblLt, cvtAll, isNextRun && isAllInOne, tableCsvDir, isWriteUtf8bom, firstCol, firstVal)
			if err != nil {
				return err
			}
		}
	}

	// write microdata into csv file, if there is any microdata for that model run and microadata write enabled
	if isWriteMicro && nMd > 0 {

		omppLog.Log("  Microdata: ", nMd)

		for j := 0; j < nMd; j++ {

			eId := meta.EntityGen[j].EntityId
			eIdx, isFound := modelDef.EntityByKey(eId)
			if !isFound {
				return errors.New("error: entity not found by Id: " + strconv.Itoa(eId) + " " + meta.EntityGen[j].GenDigest)
			}

			cvtMicro := &db.CellMicroConverter{
				ModelDef:  modelDef,
				Name:      modelDef.Entity[eIdx].Name,
				RunDef:    meta,
				GenDigest: meta.EntityGen[j].GenDigest,
				IsIdCsv:   isIdCsv,
				DoubleFmt: doubleFmt,
			}
			microLt := db.ReadMicroLayout{
				ReadLayout: db.ReadLayout{
					Name:   modelDef.Entity[eIdx].Name,
					FromId: runId,
				},
				GenDigest: meta.EntityGen[j].GenDigest,
			}

			logT = omppLog.LogIfTime(logT, logPeriod, "    ", j, " of ", nMd, ": ", microLt.Name)

			err = toCellCsvFile(dbConn, modelDef, microLt, cvtMicro, isNextRun && isAllInOne, microCsvDir, isWriteUtf8bom, firstCol, firstVal)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
