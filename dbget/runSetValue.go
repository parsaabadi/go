// Copyright OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// write model run parameters and output tables into csv or tsv files
func runValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find model run
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

	// create output directory
	// if output directory name not explicitly specified then use ModelName/run.RunName by default
	runTop := theCfg.dir
	isDefaultTop := theCfg.dir == ""

	if theCfg.isConsole {
		omppLog.Log("Do ", theCfg.action, " ", runMeta.Run.Name)
	} else {

		if runTop == "" {
			switch {
			case runOpts.Bool(runFirstArgKey):
				runTop = filepath.Join(helper.CleanFileName(meta.Model.Name), "first-run")
			case runOpts.Bool(runLastArgKey):
				runTop = filepath.Join(helper.CleanFileName(meta.Model.Name), "last-run")
			default:
				runTop = filepath.Join(helper.CleanFileName(meta.Model.Name), "run."+helper.CleanFileName(runMeta.Run.Name))
			}
			if err = makeOutputDir(runTop, theCfg.isKeepOutputDir); err != nil {
				return err
			}
		}
		omppLog.Log("Do ", theCfg.action, ": "+runTop)
	}

	return runValueOut(srcDb, meta, runMeta, runTop, isDefaultTop, runOpts)
}

// write model run parameters and output tables into csv or tsv files
func runValueOut(srcDb *sql.DB, meta *db.ModelMeta, runMeta *db.RunMeta, runTop string, isDefaultTop bool, runOpts *config.RunOptions) error {

	// create sub directories for parameters and output tables
	paramCsvDir := ""
	tableCsvDir := ""
	if !theCfg.isConsole {

		dirSuffix := "" // if output directory not specified then add .no-zero and .no-null suffix
		if isDefaultTop {
			if runOpts.Bool(noZeroArgKey) {
				dirSuffix = dirSuffix + ".no-zero"
			}
			if runOpts.Bool(noNullArgKey) {
				dirSuffix = dirSuffix + ".no-null"
			}
		}
		paramCsvDir = filepath.Join(runTop, "parameters")
		tableCsvDir = filepath.Join(runTop, "output-tables"+dirSuffix)

		if e := makeOutputDir(paramCsvDir, theCfg.isKeepOutputDir); e != nil {
			return e
		}
		if e := makeOutputDir(tableCsvDir, theCfg.isKeepOutputDir); e != nil {
			return e
		}
	}

	// write all parameters into csv file
	nP := len(meta.Param)
	omppLog.Log("  Parameters: ", nP)
	logT := time.Now().Unix()

	for j := 0; j < nP; j++ {

		logT = omppLog.LogIfTime(logT, logPeriod, "    ", j, " of ", nP, ": ", meta.Param[j].Name)

		fp := ""
		if !theCfg.isConsole {
			fp = filepath.Join(paramCsvDir, meta.Param[j].Name+extByKind())
		}
		e := parameterRunValue(srcDb, meta, meta.Param[j].Name, &runMeta.Run, fp, false, nil)
		if e != nil {
			return e
		}
	}

	// write output tables into csv file, if the table included in run results
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

		fp := ""
		if !theCfg.isConsole {
			fp = filepath.Join(tableCsvDir, name+extByKind())
		}
		e := tableRunValue(srcDb, meta, name, &runMeta.Run, runOpts, fp, false, nil)
		if e != nil {
			return e
		}
	}

	return nil
}

// write all model runs parameters and output tables into csv or tsv files
func runAllValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// get model metadata and run list
	// run list includes all runs, use only sucessfully completed
	meta, err := db.GetModelById(srcDb, modelId)
	if err != nil {
		return errors.New("Error at get model metadata by id: " + strconv.Itoa(modelId) + ": " + err.Error())
	}
	rl, err := db.GetRunList(srcDb, modelId)
	if err != nil {
		return errors.New("Error at get model runs list: " + err.Error())
	}
	rl = slices.DeleteFunc(rl, func(r db.RunRow) bool { return r.Status != db.DoneRunStatus })

	if len(rl) <= 0 {
		omppLog.Log("Do ", theCfg.action, ": ", "there are no completed model runs")
		return nil
	}

	// check if any run name is not unique then use run id's in directory names
	isUseIdNames := false
	for k := range rl {
		for i := range rl {
			if isUseIdNames = i != k && rl[i].Name == rl[k].Name; isUseIdNames {
				break
			}
		}
		if isUseIdNames {
			break
		}
	}

	// create output directory
	// if output directory name not explicitly specified then use ModelName by default
	csvTop := theCfg.dir
	isDefaultTop := !isUseIdNames && theCfg.dir == ""

	if theCfg.isConsole {
		omppLog.Log("Do ", theCfg.action, " ", meta.Model.Name)
	} else {
		if csvTop == "" {
			csvTop = filepath.Join(helper.CleanFileName(meta.Model.Name))
			if err = makeOutputDir(csvTop, theCfg.isKeepOutputDir); err != nil {
				return err
			}
		}
		omppLog.Log("Do ", theCfg.action, ": "+csvTop)
	}

	for _, rm := range rl {

		runMeta, err := db.GetRunFull(srcDb, &rm)
		if err != nil {
			return errors.New("Error at get model run: " + rm.Name + " " + err.Error())
		}
		if runMeta.Run.Status != db.DoneRunStatus {
			continue // unexpected change of model run status
		}
		omppLog.Log("Model run ", rm.RunId, " ", rm.Name)

		// run output directory is: run.Name_Of_the_Run or run.ID.Name_Of_the_Run
		runTop := ""
		if !theCfg.isConsole {
			if !isUseIdNames {
				runTop = filepath.Join(csvTop, "run."+helper.CleanFileName(rm.Name))
			} else {
				runTop = filepath.Join(csvTop, "run."+strconv.Itoa(rm.RunId)+"."+helper.CleanFileName(rm.Name))
			}
			if err = makeOutputDir(runTop, theCfg.isKeepOutputDir); err != nil {
				return err
			}
		}

		err = runValueOut(srcDb, meta, runMeta, runTop, isDefaultTop, runOpts)
		if err != nil {
			return err
		}
	}

	return nil
}

func setValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {
	return nil
}

func setAllValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {
	return nil
}
