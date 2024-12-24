// Copyright OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// get output table all accumulators (including derived) and write run results into csv or tsv file.
func tableAllAcc(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find model run
	msg, run, err := findRun(srcDb, modelId, runOpts.String(runArgKey), runOpts.Int(runIdArgKey, 0), runOpts.Bool(runFirstArgKey), runOpts.Bool(runLastArgKey))
	if err != nil {
		return errors.New("Error at get model run: " + msg + " " + err.Error())
	}
	if run == nil {
		return errors.New("Error: model run not found")
	}

	// get model metadata
	meta, err := db.GetModelById(srcDb, modelId)
	if err != nil {
		return errors.New("Error at get model metadata by id: " + strconv.Itoa(modelId) + ": " + err.Error())
	}

	// write output table all accumulators to csv or tsv file
	name := runOpts.String(tableArgKey)
	fp := ""

	if theCfg.isConsole {
		omppLog.Log("Do ", theCfg.action, " ", name)
	} else {

		fp = theCfg.fileName
		if fp == "" {
			fp = name + ".acc-all" + extByKind()
		}
		fp = filepath.Join(theCfg.dir, fp)

		omppLog.Log("Do ", theCfg.action, ": "+fp)
	}

	return tableRunAllAcc(srcDb, meta, name, run, runOpts, fp)
}

// read output table all accumulators (including derived) and write run results into csv or tsv file.
// Csv file header: sub_id,dim0,dim1,....,acc0,acc1,....
func tableRunAllAcc(srcDb *sql.DB, meta *db.ModelMeta, name string, run *db.RunRow, runOpts *config.RunOptions, path string) error {

	if run == nil {
		return errors.New("Error: model run not found")
	}
	if run.Status != db.DoneRunStatus {
		return errors.New("Error: model run not completed successfully: " + run.Name)
	}
	if name == "" {
		return errors.New("Invalid (empty) output table name")
	}
	if meta == nil {
		return errors.New("Invalid (empty) model metadata")
	}
	_, ok := meta.OutTableByName(name)
	if !ok {
		return errors.New("Error: model output table not found: " + name)
	}

	// make csv header
	// create converter from db cell into csv row []string
	var err error
	hdr := []string{}
	var cvtRow func(interface{}, []string) (bool, error)

	cvtAllAcc := &db.CellAllAccConverter{CellTableConverter: db.CellTableConverter{
		ModelDef:    meta,
		Name:        name,
		IsIdCsv:     theCfg.isIdCsv,
		DoubleFmt:   theCfg.doubleFmt,
		IsNoZeroCsv: runOpts.Bool(noZeroArgKey),
		IsNoNullCsv: runOpts.Bool(noNullArgKey),
	}}

	tblLt := db.ReadTableLayout{
		ReadLayout: db.ReadLayout{
			Name:   name,
			FromId: run.RunId,
		},
		IsAccum:    true,
		IsAllAccum: true,
	}

	if theCfg.isNoLang || theCfg.isIdCsv {

		hdr, err = cvtAllAcc.CsvHeader()
		if err != nil {
			return errors.New("Failed to make output table csv header: " + name + ": " + err.Error())
		}
		if theCfg.isIdCsv {
			cvtRow, err = cvtAllAcc.ToCsvIdRow()
		} else {
			cvtRow, err = cvtAllAcc.ToCsvRow()
		}
		if err != nil {
			return errors.New("Failed to create output table converter to csv: " + name + ": " + err.Error())
		}

	} else { // get language-specific metadata

		langDef, err := db.GetLanguages(srcDb)
		if err != nil {
			return errors.New("Error at get language-specific metadata: " + err.Error())
		}
		txt, err := db.GetModelText(srcDb, meta.Model.ModelId, theCfg.lang, true)
		if err != nil {
			return errors.New("Error at get model text metadata: " + err.Error())
		}

		cvtLoc := &db.CellAllAccLocaleConverter{
			CellAllAccConverter: *cvtAllAcc,
			Lang:                theCfg.lang,
			LangDef:             langDef,
			EnumTxt:             txt.TypeEnumTxt,
			AccTxt:              txt.TableAccTxt,
		}

		hdr, err = cvtLoc.CsvHeader()
		if err != nil {
			return errors.New("Failed to make output table csv header: " + name + ": " + err.Error())
		}
		cvtRow, err = cvtLoc.ToCsvRow()
		if err != nil {
			return errors.New("Failed to create output table converter to csv: " + name + ": " + err.Error())
		}
	}

	// start csv output to file or console
	f, csvWr, err := createCsvWriter(path)
	if err != nil {
		return err
	}
	isFile := f != nil

	defer func() {
		if isFile {
			f.Close()
		}
	}()

	// write csv header
	if err := csvWr.Write(hdr); err != nil {
		return errors.New("Error at csv write: " + name + ": " + err.Error())
	}

	// convert cell into []string and write line into csv file
	cs := make([]string, len(hdr))

	cvtWr := func(c interface{}) (bool, error) {

		// if converter return empty line then skip it
		isNotEmpty := false
		var e2 error = nil

		if isNotEmpty, e2 = cvtRow(c, cs); e2 != nil {
			return false, e2
		}
		if !isNotEmpty {
			return true, nil
		}

		e2 = csvWr.Write(cs)
		return e2 == nil, e2
	}

	// read output table accumulators
	_, err = db.ReadOutputTableTo(srcDb, meta, &tblLt, cvtWr)
	if err != nil {
		return errors.New("Error at output table output: " + name + ": " + err.Error())
	}

	csvWr.Flush() // flush csv to response

	return nil
}
