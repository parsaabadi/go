// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// write all model run data into csv files: parameters, output expressions and accumulators
func toRunListCsv(
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	outDir string,
	doubleFmt string,
	isIdCsv bool,
	isWriteUtf8bom bool) error {

	// get all successfully completed model runs
	rl, err := db.GetRunFullList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// read all run parameters, output accumulators and expressions and dump it into csv files
	for k := range rl {
		err = toRunCsv(dbConn, modelDef, &rl[k], outDir, doubleFmt, isIdCsv, isWriteUtf8bom)
		if err != nil {
			return err
		}
	}

	// write model run rows into csv
	row := make([]string, 10)

	idx := 0
	err = toCsvFile(
		outDir,
		"run_lst.csv",
		isWriteUtf8bom,
		[]string{
			"run_id", "model_id", "run_name", "sub_count",
			"sub_started", "sub_completed", "create_dt", "status",
			"update_dt", "run_digest"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(rl) {
				row[0] = strconv.Itoa(rl[idx].Run.RunId)
				row[1] = strconv.Itoa(rl[idx].Run.ModelId)
				row[2] = rl[idx].Run.Name
				row[3] = strconv.Itoa(rl[idx].Run.SubCount)
				row[4] = strconv.Itoa(rl[idx].Run.SubStarted)
				row[5] = strconv.Itoa(rl[idx].Run.SubCompleted)
				row[6] = rl[idx].Run.CreateDateTime
				row[7] = rl[idx].Run.Status
				row[8] = rl[idx].Run.UpdateDateTime
				row[9] = rl[idx].Run.Digest
				idx++
				return false, row, nil
			}
			return true, row, nil // end of model run rows
		})
	if err != nil {
		return errors.New("failed to write model run into csv " + err.Error())
	}

	// write model run text rows into csv
	row = make([]string, 4)

	idx = 0
	j := 0
	err = toCsvFile(
		outDir,
		"run_txt.csv",
		isWriteUtf8bom,
		[]string{"run_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(rl) { // end of run rows
				return true, row, nil
			}

			// if end of current run texts then find next run with text rows
			if j < 0 || j >= len(rl[idx].Txt) {
				j = 0
				for {
					idx++
					if idx < 0 || idx >= len(rl) { // end of run rows
						return true, row, nil
					}
					if len(rl[idx].Txt) > 0 {
						break
					}
				}
			}

			// make model run text []string row
			row[0] = strconv.Itoa(rl[idx].Txt[j].RunId)
			row[1] = rl[idx].Txt[j].LangCode
			row[2] = rl[idx].Txt[j].Descr

			if rl[idx].Txt[j].Note == "" { // empty "" string is NULL
				row[3] = "NULL"
			} else {
				row[3] = rl[idx].Txt[j].Note
			}
			j++
			return false, row, nil
		})
	if err != nil {
		return errors.New("failed to write model run text into csv " + err.Error())
	}

	// convert run option map to array of (id,key,value) rows
	var kvArr [][]string
	k := 0
	for j := range rl {
		for key, val := range rl[j].Opts {
			kvArr = append(kvArr, make([]string, 3))
			kvArr[k][0] = strconv.Itoa(rl[j].Run.RunId)
			kvArr[k][1] = key
			kvArr[k][2] = val
			k++
		}
	}

	// write model run option rows into csv
	row = make([]string, 3)

	idx = 0
	err = toCsvFile(
		outDir,
		"run_option.csv",
		isWriteUtf8bom,
		[]string{"run_id", "option_key", "option_value"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(kvArr) {
				row = kvArr[idx]
				idx++
				return false, row, nil
			}
			return true, row, nil // end of run rows
		})
	if err != nil {
		return errors.New("failed to write model run text into csv " + err.Error())
	}

	// write run parameter rows into csv
	row = make([]string, 3)

	idx = 0
	j = 0
	err = toCsvFile(
		outDir,
		"run_parameter.csv",
		isWriteUtf8bom,
		[]string{"run_id", "parameter_hid", "sub_count"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(rl) { // end of model run rows
				return true, row, nil
			}

			// if end of current run parameters then find next run with parameter rows
			if j < 0 || j >= len(rl[idx].Param) {
				j = 0
				for {
					idx++
					if idx < 0 || idx >= len(rl) { // end of run rows
						return true, row, nil
					}
					if len(rl[idx].Param) > 0 {
						break
					}
				}
			}

			// make run parameter []string row
			row[0] = strconv.Itoa(rl[idx].Run.RunId)
			row[1] = strconv.Itoa(rl[idx].Param[j].ParamHid)
			row[2] = strconv.Itoa(rl[idx].Param[j].SubCount)
			j++
			return false, row, nil
		})
	if err != nil {
		return errors.New("failed to write run parameters into csv " + err.Error())
	}

	// write parameter value notes rows into csv
	row = make([]string, 4)

	idx = 0
	pix := 0
	j = 0
	err = toCsvFile(
		outDir,
		"run_parameter_txt.csv",
		isWriteUtf8bom,
		[]string{"run_id", "parameter_hid", "lang_code", "note"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(rl) { // end of model run rows
				return true, row, nil
			}

			// if end of current run parameter text then find next run with parameter text rows
			if pix < 0 || pix >= len(rl[idx].Param) || j < 0 || j >= len(rl[idx].Param[pix].Txt) {

				j = 0
				for {
					if 0 <= pix && pix < len(rl[idx].Param) {
						pix++
					}
					if pix < 0 || pix >= len(rl[idx].Param) {
						idx++
						pix = 0
					}
					if idx < 0 || idx >= len(rl) { // end of model run rows
						return true, row, nil
					}
					if pix >= len(rl[idx].Param) { // end of run parameter text rows for that run
						continue
					}
					if len(rl[idx].Param[pix].Txt) > 0 {
						break
					}
				}
			}

			// make run parameter text []string row
			row[0] = strconv.Itoa(rl[idx].Param[pix].Txt[j].RunId)
			row[1] = strconv.Itoa(rl[idx].Param[pix].Txt[j].ParamHid)
			row[2] = rl[idx].Param[pix].Txt[j].LangCode

			if rl[idx].Param[pix].Txt[j].Note == "" { // empty "" string is NULL
				row[3] = "NULL"
			} else {
				row[3] = rl[idx].Param[pix].Txt[j].Note
			}
			j++
			return false, row, nil
		})
	if err != nil {
		return errors.New("failed to write model run parameter text into csv " + err.Error())
	}

	return nil
}

// toRunCsv write model run metadata, parameters and output tables into csv files, in separate subdirectory
func toRunCsv(
	dbConn *sql.DB,
	modelDef *db.ModelMeta,
	meta *db.RunMeta,
	outDir string,
	doubleFmt string,
	isIdCsv bool,
	isWriteUtf8bom bool) error {

	// create run subdir under model dir
	runId := meta.Run.RunId
	omppLog.Log("Model run ", runId, " ", meta.Run.Name)

	csvDir := filepath.Join(outDir, "run."+strconv.Itoa(runId)+"."+helper.ToAlphaNumeric(meta.Run.Name))

	err := os.MkdirAll(csvDir, 0750)
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
		err = toCsvCellFile(csvDir, modelDef, paramLt.Name, pc, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom)
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
		err = toCsvCellFile(csvDir, modelDef, tblLt.Name, ec, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom)
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
		err = toCsvCellFile(csvDir, modelDef, tblLt.Name, ac, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom)
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
		err = toCsvCellFile(csvDir, modelDef, tblLt.Name, al, cLst, doubleFmt, isIdCsv, "", isWriteUtf8bom)
		if err != nil {
			return err
		}
	}

	return nil
}
