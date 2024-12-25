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

// get entity microdata values and write run results into csv or tsv file.
// Csv file header is key and names of attribute: key,AgeGroup,Income,....
func microdataValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find model run
	msg, run, err := findRun(srcDb, modelId, runOpts.String(runArgKey), runOpts.Int(runIdArgKey, 0), runOpts.Bool(runFirstArgKey), runOpts.Bool(runLastArgKey))
	if err != nil {
		return errors.New("Error at get model run: " + msg + " " + err.Error())
	}
	if run == nil {
		return errors.New("Error: model run not found")
	}
	if run.Status != db.DoneRunStatus {
		return errors.New("Error: model run not completed successfully: " + run.Name)
	}

	// get model metadata
	meta, err := db.GetModelById(srcDb, modelId)
	if err != nil || meta == nil {
		return errors.New("Error at get model metadata by id: " + strconv.Itoa(modelId) + ": " + err.Error())
	}

	// write microdata values to csv or tsv file
	name := runOpts.String(entityArgKey)
	if name == "" {
		return errors.New("Invalid (empty) model entity name")
	}
	fp := ""

	if theCfg.isConsole {
		omppLog.Log("Do ", theCfg.action, " ", name)
	} else {

		fp = theCfg.fileName
		if fp == "" {
			fp = name + extByKind()
		}
		fp = filepath.Join(theCfg.dir, fp)

		omppLog.Log("Do ", theCfg.action, ": "+fp)
	}

	return microdataRunValue(srcDb, meta, name, run, runOpts, fp)
}

// read entity microdata values and write run results into csv or tsv file.
func microdataRunValue(srcDb *sql.DB, meta *db.ModelMeta, name string, run *db.RunRow, runOpts *config.RunOptions, path string) error {

	if name == "" {
		return errors.New("Invalid (empty) model entity name")
	}
	if meta == nil {
		return errors.New("Invalid (empty) model metadata")
	}
	if run == nil {
		return errors.New("Invalid (empty) model run metadata")
	}

	// find model entity
	eIdx, ok := meta.EntityByName(name)
	if !ok {
		return errors.New("Error: model entity not found: " + name)
	}
	ent := &meta.Entity[eIdx]

	// find entity generation by entity id, as it is today model run has only one entity generation for each entity
	egLst, err := db.GetEntityGenList(srcDb, run.RunId)
	if err != nil || len(egLst) <= 0 {
		return errors.New("Error: not found any microdata in model run: " + run.Name)
	}

	gIdx := -1
	for k := range egLst {

		if egLst[k].EntityId == ent.EntityId {
			gIdx = k
			break
		}
	}
	if gIdx < 0 {
		return errors.New("Error: not found generation of entity: " + name + " in model run: " + run.Name)
	}

	// make csv header
	// create converter from db cell into csv row []string
	hdr := []string{}
	var cvtRow func(interface{}, []string) (bool, error)

	cvtMicro := &db.CellMicroConverter{CellEntityConverter: db.CellEntityConverter{
		ModelDef:    meta,
		Name:        name,
		EntityGen:   &egLst[gIdx],
		IsIdCsv:     theCfg.isIdCsv,
		DoubleFmt:   theCfg.doubleFmt,
		IsNoZeroCsv: runOpts.Bool(noZeroArgKey),
		IsNoNullCsv: runOpts.Bool(noNullArgKey),
	}}
	microLt := db.ReadMicroLayout{
		ReadLayout: db.ReadLayout{
			Name:   name,
			FromId: run.RunId,
		},
		GenDigest: egLst[gIdx].GenDigest,
	}

	if theCfg.isNoLang || theCfg.isIdCsv {

		hdr, err = cvtMicro.CsvHeader()
		if err != nil {
			return errors.New("Failed to make microdata csv header: " + name + ": " + err.Error())
		}
		if theCfg.isIdCsv {
			cvtRow, err = cvtMicro.ToCsvIdRow()
		} else {
			cvtRow, err = cvtMicro.ToCsvRow()
		}
		if err != nil {
			return errors.New("Failed to create microdata converter to csv: " + name + ": " + err.Error())
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

		cvtLoc := &db.CellMicroLocaleConverter{
			CellMicroConverter: *cvtMicro,
			Lang:               theCfg.lang,
			LangDef:            langDef,
			EnumTxt:            txt.TypeEnumTxt,
			AttrTxt:            txt.EntityAttrTxt,
		}

		hdr, err = cvtLoc.CsvHeader()
		if err != nil {
			return errors.New("Failed to make microdata csv header: " + name + ": " + err.Error())
		}
		cvtRow, err = cvtLoc.ToCsvRow()
		if err != nil {
			return errors.New("Failed to create microdata converter to csv: " + name + ": " + err.Error())
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

	// read entity microdata
	_, err = db.ReadMicrodataTo(srcDb, meta, &microLt, cvtWr)
	if err != nil {
		return errors.New("Error at microdata output: " + name + ": " + err.Error())
	}

	csvWr.Flush() // flush csv to response

	return nil
}
