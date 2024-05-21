// Copyright OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"strconv"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
)

// get model run paratemer values and write run results into csv or json files.
func parameterValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find model run
	msg, run, err := findRun(srcDb, modelId, runOpts.String(runArgKey), runOpts.Int(runIdArgKey, 0), runOpts.Bool(runFirstArgKey), runOpts.Bool(runLastArgKey))
	if err != nil {
		return errors.New("Error at get model run: " + msg + " " + err.Error())
	}
	if run == nil {
		return errors.New("Error: model run not found")
	}
	if run.Status != db.DoneRunStatus {
		return errors.New("Error: model run not completed successfully: " + msg)
	}

	// get model metadata and find parameter
	name := runOpts.String(paramArgKey)
	if name == "" {
		return errors.New("Invalid (empty) parameter name")
	}

	meta, err := db.GetModelById(srcDb, modelId)
	if err != nil {
		return errors.New("Error at get model metadata by id: " + strconv.Itoa(modelId) + ": " + err.Error())
	}

	_, ok := meta.ParamByName(name)
	if !ok {
		return errors.New("Error: model parameter not found: " + name)
	}

	// create cell conveter to csv
	hdr := []string{}
	var cvtRow func(interface{}, []string) (bool, error)

	cvtParam := &db.CellParamConverter{
		ModelDef:  meta,
		Name:      name,
		IsIdCsv:   false,
		DoubleFmt: theCfg.doubleFmt,
	}
	paramLt := db.ReadParamLayout{
		IsFromSet: false,
		ReadLayout: db.ReadLayout{
			Name:   name,
			FromId: run.RunId,
		}}

	// make csv header
	// create converter from db cell into csv row []string
	if theCfg.isNoLang {

		hdr, err = cvtParam.CsvHeader()
		if err != nil {
			return errors.New("Failed to make parameter csv header: " + name + ": " + err.Error())
		}
		cvtRow, err = cvtParam.ToCsvRow()
		if err != nil {
			return errors.New("Failed to create parameter converter to csv: " + name + ": " + err.Error())
		}

	} else { // get language-specific metadata

		langDef, err := db.GetLanguages(srcDb)
		if err != nil {
			return errors.New("Error at get language-specific metadata: " + err.Error())
		}
		txt, err := db.GetModelText(srcDb, modelId, theCfg.lang, true)
		if err != nil {
			return errors.New("Error at get model text metadata: " + err.Error())
		}

		cvtLoc := &db.CellParamLocaleConverter{
			CellParamConverter: *cvtParam,
			Lang:               theCfg.lang,
			LangMeta:           *langDef,
			EnumTxt:            txt.TypeEnumTxt,
		}

		hdr, err = cvtLoc.CsvHeader()
		if err != nil {
			return errors.New("Failed to make parameter csv header: " + name + ": " + err.Error())
		}
		cvtRow, err = cvtLoc.ToCsvRow()
		if err != nil {
			return errors.New("Failed to create parameter converter to csv: " + name + ": " + err.Error())
		}
	}

	// start csv output to file or console
	f, csvWr, err := startCsvWrite(name)
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
		isNotEmpty := true
		var e2 error = nil

		if isNotEmpty, e2 = cvtRow(c, cs); e2 != nil {
			return false, e2
		}
		if isNotEmpty {
			if e2 = csvWr.Write(cs); e2 != nil {
				return false, e2
			}
		}
		return true, nil
	}

	// read parameter values page
	_, err = db.ReadParameterTo(srcDb, meta, &paramLt, cvtWr)
	if err != nil {
		return errors.New("Error at parameter output: " + name + ": " + err.Error())
	}

	csvWr.Flush() // flush csv to response

	return nil
}
