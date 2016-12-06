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
func toRunListCsv(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, doubleFmt string, isIdCsv bool) error {

	// get all successfully completed model runs
	rl, err := db.GetRunFullList(dbConn, modelDef.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// read all run parameters, output accumulators and expressions and dump it into csv files
	for k := range rl {
		err = toRunCsv(dbConn, modelDef, &rl[k], outDir, doubleFmt, isIdCsv)
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
	row = make([]string, 5)

	idx = 0
	j := 0
	err = toCsvFile(
		outDir,
		"run_txt.csv",
		[]string{"run_id", "lang_id", "lang_code", "descr", "note"},
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
			row[1] = strconv.Itoa(rl[idx].Txt[j].LangId)
			row[2] = rl[idx].Txt[j].LangCode
			row[3] = rl[idx].Txt[j].Descr

			if rl[idx].Txt[j].Note == "" { // empty "" string is NULL
				row[4] = "NULL"
			} else {
				row[4] = rl[idx].Txt[j].Note
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

	// write parameter value notes rows into csv
	row = make([]string, 5)

	idx = 0
	pix := 0
	j = 0
	err = toCsvFile(
		outDir,
		"run_parameter_txt.csv",
		[]string{"run_id", "parameter_hid", "lang_id", "lang_id", "note"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(rl) { // end of model run rows
				return true, row, nil
			}

			// if end of current run parameter text then find next run with parameter text rows
			if pix < 0 || pix >= len(rl[idx].ParamTxt) || j < 0 || j >= len(rl[idx].ParamTxt[pix].Txt) {

				j = 0
				for {
					if 0 <= pix && pix < len(rl[idx].ParamTxt) {
						pix++
					}
					if pix < 0 || pix >= len(rl[idx].ParamTxt) {
						idx++
						pix = 0
					}
					if idx < 0 || idx >= len(rl) { // end of model run rows
						return true, row, nil
					}
					if pix >= len(rl[idx].ParamTxt) { // end of run parameter text rows for that run
						continue
					}
					if len(rl[idx].ParamTxt[pix].Txt) > 0 {
						break
					}
				}
			}

			// make run parameter text []string row
			row[0] = strconv.Itoa(rl[idx].ParamTxt[pix].Txt[j].RunId)
			row[1] = strconv.Itoa(rl[idx].ParamTxt[pix].Txt[j].ParamHid)
			row[2] = strconv.Itoa(rl[idx].ParamTxt[pix].Txt[j].LangId)
			row[3] = rl[idx].ParamTxt[pix].Txt[j].LangCode

			if rl[idx].ParamTxt[pix].Txt[j].Note == "" { // empty "" string is NULL
				row[4] = "NULL"
			} else {
				row[4] = rl[idx].ParamTxt[pix].Txt[j].Note
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
	dbConn *sql.DB, modelDef *db.ModelMeta, meta *db.RunMeta, outDir string, doubleFmt string, isIdCsv bool) error {

	// create run subdir under model dir
	runId := meta.Run.RunId
	omppLog.Log("Model run ", runId, " ", meta.Run.Name)

	csvDir := filepath.Join(outDir, "run."+strconv.Itoa(runId)+"."+helper.ToAlphaNumeric(meta.Run.Name))

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
		err = toCsvCellFile(csvDir, modelDef, layout.Name, cp, cLst, doubleFmt, isIdCsv)
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
		err = toCsvCellFile(csvDir, modelDef, layout.Name, ec, cLst, doubleFmt, isIdCsv)
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
		err = toCsvCellFile(csvDir, modelDef, layout.Name, ac, cLst, doubleFmt, isIdCsv)
		if err != nil {
			return err
		}
	}

	return nil
}
