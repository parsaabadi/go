// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"flag"
	"os"
	"strings"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	omppLog "go.openmpp.org/ompp/log"
)

// dbcopy config keys to get values from ini-file or command line arguments.
const (
	copyToArgKey      = "dbcopy.To"               // copy to: text=db-to-text, db=text-to-db, db2db=db-to-db
	zipArgKey         = "dbcopy.Zip"              // create output or use as input model.zip
	inputDirArgKey    = "dbcopy.InputDir"         // input dir to read model .json and .csv files
	outputDirArgKey   = "dbcopy.OutputDir"        // output dir to write model .json and .csv files
	toDbConnectionStr = "dbcopy.ToDatabase"       // output db connection string
	toDbDriverName    = "dbcopy.ToDatabaseDriver" // output db driver name, ie: SQLite, odbc, sqlite3
	doubleFmtArgKey   = "OpenM.DoubleFormat"      // convert to string format for float and double
)

func main() {
	defer exitOnPanic() // fatal error handler: log and exit

	// parse command line arguments and ini-file
	_ = flag.String(config.ModelName, "", "model name")
	_ = flag.String(config.ModelNameShort, "", "model name (short of "+config.ModelName+")")
	_ = flag.String(config.ModelDigest, "", "model hash digest")
	_ = flag.String(config.DbConnectionStr, "", "input database connection string")
	_ = flag.String(config.DbDriverName, db.SQLiteDbDriver, "input database driver name")
	_ = flag.String(copyToArgKey, "text", "copy to: `text`=db-to-text, db=text-to-db, db2db=db-to-db")
	_ = flag.String(toDbConnectionStr, "", "output database connection string")
	_ = flag.String(toDbDriverName, db.SQLiteDbDriver, "output database driver name")
	_ = flag.Bool(zipArgKey, false, "create output or use as input model.zip")
	_ = flag.String(inputDirArgKey, "", "input directory to read model .json and .csv files")
	_ = flag.String(outputDirArgKey, "", "output directory for model .json and .csv files")
	_ = flag.String(config.SetName, "", "workset name (set of model input parameters), if specified then copy only this workset")
	_ = flag.String(config.SetNameShort, "", "workset name (short of "+config.SetName+")")
	_ = flag.Int(config.SetId, 0, "workset id (set of model input parameters), if specified then copy only this workset")
	_ = flag.String(config.RunName, "", "model run name, if specified then copy only this run data")
	_ = flag.Int(config.RunId, 0, "model run id, if specified then copy only this run data")
	_ = flag.String(config.ParamDir, "", "path to parameters directory (workset directory)")
	_ = flag.String(config.ParamDirShort, "", "path to parameters directory (short of "+config.ParamDir+")")
	_ = flag.String(doubleFmtArgKey, "%.15g", "convert to string format for float and double")

	runOpts, logOpts, err := config.New()
	if err != nil {
		panic(err)
	}

	omppLog.New(logOpts) // adjust log options

	// model name or model digest is required
	modelName := runOpts.String(config.ModelName)
	modelDigest := runOpts.String(config.ModelDigest)

	if modelName == "" && modelDigest == "" {
		panic("invalid (empty) model name and model digest")
	}
	omppLog.Log("Model ", modelName, " ", modelDigest)

	// check run options:
	// copy single run data, single workset, single task
	// or entire model by default
	isRun := runOpts.IsExist(config.RunName) || runOpts.IsExist(config.RunId)
	isWs := runOpts.IsExist(config.SetName) || runOpts.IsExist(config.SetId)
	isTask := runOpts.IsExist(config.TaskName) || runOpts.IsExist(config.TaskId)

	// do copy operation: entire model by default
	if !isRun && !isWs && !isTask {

		switch strings.ToLower(runOpts.String(copyToArgKey)) {
		case "text":
			err = dbToText(modelName, modelDigest, runOpts)
		case "db":
			err = textToDb(modelName, runOpts)
		case "db2db":
			err = dbToDb(modelName, modelDigest, runOpts)
		default:
			panic("dbcopy invalid argument for copy-to: " + runOpts.String(copyToArgKey))
		}
	}

	// copy single workset
	if isWs {
		switch strings.ToLower(runOpts.String(copyToArgKey)) {
		case "text":
			err = dbToTextWorkset(modelName, modelDigest, runOpts)
		case "db":
			err = textToDbWorkset(modelName, modelDigest, runOpts)
		case "db2db":
			err = dbToDbWorkset(modelName, modelDigest, runOpts)
		default:
			panic("dbcopy invalid argument for copy-to: " + runOpts.String(copyToArgKey))
		}
	}

	// copy single model run
	if isRun {

		omppLog.Log("model run copy under construction, coming soon")
		// TODO: use run digest instead of id
		/*
			switch strings.ToLower(runOpts.String(copyToArgKey)) {
			case "text":
				err = dbToTextRun(modelName, modelDigest, runOpts)
			case "db":
				err = textToDbRun(modelName, modelDigest, runOpts)
			case "db2db":
				err = dbToDbRun(modelName, modelDigest, runOpts)
			default:
				panic("dbcopy invalid argument for copy-to: " + runOpts.String(copyToArgKey))
			}
		*/
	}

	// copy single modeling task
	if isTask {
		omppLog.Log("modeling tsk copy under construction, coming soon")
		// TODO: use run digest instead of id
		/*
			switch strings.ToLower(runOpts.String(copyToArgKey)) {
			case "text":
				err = dbToTextTask(modelName, modelDigest, runOpts)
			case "db":
				err = textToDbTask(modelName, modelDigest, runOpts)
			case "db2db":
				err = dbToDbTask(modelName, modelDigest, runOpts)
			default:
				panic("dbcopy invalid argument for copy-to: " + runOpts.String(copyToArgKey))
			}
		*/
	}
	if err != nil {
		panic(err)
	}

	// compeleted OK
	omppLog.Log("Done.")
	os.Exit(0)
}

// exitOnPanic log error message and exit with return = 2
func exitOnPanic() {
	r := recover()
	if r == nil {
		return // not in panic
	}
	switch e := r.(type) {
	case error:
		omppLog.Log(e.Error())
	case string:
		omppLog.Log(e)
	default:
		omppLog.Log("FAILED")
	}
	os.Exit(2) // final exit
}
