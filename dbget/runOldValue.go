// Copyright OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"time"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// write old compatibilty model run parameters and output tables into csv or tsv files
func runOldValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find first model run
	msg, run, err := findRun(srcDb, modelId, "", 0, true, false)
	if err != nil {
		return errors.New("Error at get model run: " + msg + " " + err.Error())
	}
	if run == nil {
		return errors.New("Error: first model run not found")
	}
	runMeta, err := db.GetRunFull(srcDb, run)
	if err != nil {
		return errors.New("Error at get model run: " + run.Name + " " + err.Error())
	}

	// get model metadata
	meta, err := db.GetModelById(srcDb, modelId)
	if err != nil {
		return errors.New("Error at get model metadata by id: " + strconv.Itoa(modelId) + ": " + err.Error())
	}

	// create output directory and sub directories for parameters and output tables
	// if output directory name not explicitly specified then use run name by default

	csvTop := theCfg.dir
	paramCsvDir := ""
	tableCsvDir := ""

	if theCfg.isConsole {
		omppLog.Log("Do ", theCfg.action)
	} else {

		dirSuffix := "" // if ouput directory not specified then add .no-zero and .no-null suffix

		if csvTop == "" {
			csvTop = filepath.Join(helper.CleanFileName(meta.Model.Name), "old-run."+helper.CleanFileName(run.Name))
			if err = makeOutputDir(csvTop, theCfg.isKeepOutputDir); err != nil {
				return err
			}
			if runOpts.Bool(noZeroArgKey) {
				dirSuffix = dirSuffix + ".no-zero"
			}
			if runOpts.Bool(noNullArgKey) {
				dirSuffix = dirSuffix + ".no-null"
			}
		}
		omppLog.Log("Do ", theCfg.action, ": "+csvTop)

		paramCsvDir = filepath.Join(csvTop, "parameters")
		tableCsvDir = filepath.Join(csvTop, "output-tables"+dirSuffix)

		if err = makeOutputDir(paramCsvDir, theCfg.isKeepOutputDir); err != nil {
			return err
		}
		if err = makeOutputDir(tableCsvDir, theCfg.isKeepOutputDir); err != nil {
			return err
		}
	}

	// write all parameters into csv file
	d := theCfg.dir
	theCfg.dir = paramCsvDir

	nP := len(meta.Param)
	omppLog.Log("  Parameters: ", nP)
	logT := time.Now().Unix()

	for j := 0; j < nP; j++ {

		logT = omppLog.LogIfTime(logT, logPeriod, "    ", j, " of ", nP, ": ", meta.Param[j].Name)

		err = parameterOldOut(srcDb, meta, meta.Param[j].Name, run, false)
		if err != nil {
			return err
		}
	}

	// write output tables into csv file, if the table included in run results
	theCfg.dir = tableCsvDir

	nT := len(runMeta.Table)
	omppLog.Log("  Tables: ", nT)

	for j := 0; j < nT; j++ {

		// check if table exist in model run results
		name := ""
		for k := range meta.Table {
			if meta.Table[k].TableHid == runMeta.Table[j].TableHid {
				name = meta.Table[k].Name
				break
			}
		}
		if name == "" {
			continue // skip table: it is suppressed and not in run results
		}
		logT = omppLog.LogIfTime(logT, logPeriod, "    ", j, " of ", nT, ": ", name)

		err = tableOldOut(srcDb, meta, name, run, runOpts, false)
		if err != nil {
			return err
		}
	}
	theCfg.dir = d

	return nil
}

// write old compatibilty run paratemer values into csv or tsv file
func parameterOldValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find first model run
	msg, run, err := findRun(srcDb, modelId, "", 0, true, false)
	if err != nil {
		return errors.New("Error at get model run: " + msg + " " + err.Error())
	}
	if run == nil {
		return errors.New("Error: first model run not found")
	}

	// get model metadata and find parameter
	meta, err := db.GetModelById(srcDb, modelId)
	if err != nil {
		return errors.New("Error at get model metadata by id: " + strconv.Itoa(modelId) + ": " + err.Error())
	}
	name := runOpts.String(paramArgKey)
	if name == "" {
		return errors.New("Invalid (empty) parameter name")
	}

	return parameterOldOut(srcDb, meta, name, run, true)
}

// write old compatibilty run paratemer values into csv or tsv file
func parameterOldOut(srcDb *sql.DB, meta *db.ModelMeta, name string, run *db.RunRow, isLogAction bool) error {

	// find parameter
	idx, ok := meta.ParamByName(name)
	if !ok {
		return errors.New("Error: model parameter not found: " + name)
	}

	// create compatibility view parameter header: Dim0 Dim1....Value
	hdr := []string{}

	for k := 0; k < meta.Param[idx].Rank; k++ {
		hdr = append(hdr, "Dim"+strconv.Itoa(k))
	}
	hdr = append(hdr, "Value")

	// write to csv rows starting from column 1, skip sub_id column
	return parameterRunValue(srcDb, meta, name, run, true, isLogAction, hdr)

}

// write old compatibilty output table values into csv or tsv file
func tableOldValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find model run
	msg, run, err := findRun(srcDb, modelId, "", 0, true, false)
	if err != nil {
		return errors.New("Error at get model run: " + msg + " " + err.Error())
	}
	if run == nil {
		return errors.New("Error: model run not found")
	}

	// get model metadata and find output table
	meta, err := db.GetModelById(srcDb, modelId)
	if err != nil {
		return errors.New("Error at get model metadata by id: " + strconv.Itoa(modelId) + ": " + err.Error())
	}
	name := runOpts.String(tableArgKey)
	if name == "" {
		return errors.New("Invalid (empty) output tabel name")
	}

	return tableOldOut(srcDb, meta, name, run, runOpts, true)
}

// write old compatibilty output table values into csv or tsv file
func tableOldOut(srcDb *sql.DB, meta *db.ModelMeta, name string, run *db.RunRow, runOpts *config.RunOptions, isLogAction bool) error {

	// find output table
	idx, ok := meta.OutTableByName(name)
	if !ok {
		return errors.New("Error: model output table not found: " + name)
	}

	// create compatibility view output table header: Dim0 Dim1....Value
	// measure dimension is the last, at [rank] postion
	hdr := []string{}

	for k := 0; k < meta.Table[idx].Rank; k++ {
		hdr = append(hdr, "Dim"+strconv.Itoa(k))
	}
	hdr = append(hdr, "Dim"+strconv.Itoa(meta.Table[idx].Rank))
	hdr = append(hdr, "Value")

	// write output table values to csv or tsv file
	return tableRunValue(srcDb, meta, name, run, runOpts, true, isLogAction, hdr)
}
