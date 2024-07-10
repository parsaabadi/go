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

// write model run parameters and output tables into csv or tsv files
func runValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find first model run
	msg, run, err := findRun(srcDb, modelId, runOpts.String(runArgKey), runOpts.Int(runIdArgKey, 0), runOpts.Bool(runFirstArgKey), runOpts.Bool(runLastArgKey))
	if err != nil {
		return errors.New("Error at get model run: " + msg + " " + err.Error())
	}
	if run == nil {
		return errors.New("Error: model run not found")
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

			switch {
			case runOpts.Bool(runFirstArgKey):
				csvTop = filepath.Join(helper.CleanFileName(meta.Model.Name), "first-run")
			case runOpts.Bool(runLastArgKey):
				csvTop = filepath.Join(helper.CleanFileName(meta.Model.Name), "last-run")
			default:
				csvTop = filepath.Join(helper.CleanFileName(meta.Model.Name), "run."+helper.CleanFileName(run.Name))
			}
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

		err = parameterRunValue(srcDb, meta, meta.Param[j].Name, run, false, false, nil)
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

		err = tableRunValue(srcDb, meta, name, run, runOpts, false, false, nil)
		if err != nil {
			return err
		}
	}
	theCfg.dir = d

	return nil
}

func runAllValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {
	return nil
}

func setValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {
	return nil
}

func setAllValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {
	return nil
}
