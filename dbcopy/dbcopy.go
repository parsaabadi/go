// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
dbcopy is command line tool for import-export OpenM++ model metadata, input parameters and run results.

Arguments for dbcopy can be specified on command line or through .ini file:
  dbcopy -ini my.ini
Command line arguments take precedence over ini-file options.

Only model name argument does not have default value and must be specified explicitly:
  dbcopy -m modelOne
  dbcopy -OpenM.ModelName modelOne

There are 3 possible copy directions: "text", "db", "db2db" and default is "text".

"text": read from database and save into metadata .json and .csv values (parameters and output tables):
  dbcopy -m modelOne

"db": read from metadata .json and .csv values and insert or update database:
  dbcopy -m modelOne -dbcopy.To db

"db2db": direct copy between two databases:
  dbcopy -m modelOne -dbcopy.To db2db -dbcopy.ToDatabase "Database=dst.sqlite;OpenMode=ReadWrite"

By default entire model data is copied.
It is also possible to copy only: model run results and input parameters,
set of input parameters (workset), modeling task metadata and run history.

To copy only one set of input parameters:
  dbcopy -m redModel -OpenM.SetName Default
  dbcopy -m redModel -s Default

To copy only one model run results and input parameters:
  dbcopy -m modelOne -OpenM.RunId 101
  dbcopy -m modelOne -OpenM.RunName modelOne_2016_09_28_11_38_49_0945_101

To copy only one modeling task metadata and run history:
  dbcopy -m modelOne -OpenM.TaskName taskOne
  dbcopy -m modelOne -OpenM.TaskId 1

It may be convenient to pack (unpack) text files into .zip archive:
  dbcopy -m modelOne -dbcopy.Zip=true
  dbcopy -m modelOne -dbcopy.Zip
  dbcopy -m redModel -OpenM.SetName Default -dbcopy.Zip

By default model name is used to create output directory for text files or as input directory to import from.
It may be a problem on Linux if current directory already contains executable "modelName".

To specify output or input directory for text files:
  dbcopy -m modelOne -dbcopy.OutputDir one
  dbcopy -m redModel -dbcopy.OutputDir red -s Default
  dbcopy -m redModel -dbcopy.InputDir red -dbcopy.To db -dbcopy.ToDatabase "Database=dst.sqlite;OpenMode=ReadWrite"
  dbcopy -m redModel -dbcopy.OutputDir red -s Default

Also in case of input parameters you can use "-OpenM.ParamDir" or "-p" to specify input or output directory:
  dbcopy -m modelOne -OpenM.SetId 2 -OpenM.ParamDir two
  dbcopy -m modelOne -OpenM.SetId 2 -p two
  dbcopy -m redModel -s Default -p 101 -dbcopy.To db -dbcopy.ToDatabase "Database=dst.sqlite;OpenMode=ReadWrite"

OpenM++ using hash digest to compare models, input parameters and output values.
By default float and double values converted into text with "%.15g" format.
It is possible to specify other format for float values digest calculation:
  dbcopy -m redModel -OpenM.DoubleFormat "%.7G" -dbcopy.To db -dbcopy.ToDatabase "Database=dst.sqlite;OpenMode=ReadWrite"

By default dbcopy using SQLite database connection:
  dbcopy -m modelOne
is equivalent of:
  dbcopy -m modelOne -OpenM.DatabaseDriver SQLite -OpenM.Database "Database=modelOne.sqlite; Timeout=86400; OpenMode=ReadWrite;"

Output database connection settings by default are the same as input database,
which may not be suitable because you don't want to write into input database.

To specify output database connection string and driver:
  dbcopy -m modelOne -dbcopy.To db -dbcopy.ToDatabaseDriver SQLite -dbcopy.ToDatabase "Database=dst.sqlite; Timeout=86400; OpenMode=ReadWrite;"
or skip default database driver name "SQLite":
  dbcopy -m modelOne -dbcopy.To db -dbcopy.ToDatabase "Database=dst.sqlite; Timeout=86400; OpenMode=ReadWrite;"

Other supported database drivers are "sqlite3" and "odbc":
  dbcopy -m modelOne -dbcopy.To db -dbcopy.ToDatabaseDriver odbc -dbcopy.ToDatabase "DSN=bigSql"
  dbcopy -m modelOne -dbcopy.To db -dbcopy.ToDatabaseDriver sqlite3 -dbcopy.ToDatabase "file:dst.sqlite?mode=rw"

Also dbcopy support OpenM++ standard log settings (described in wiki at http://www.openmpp.org/wiki/):
  -OpenM.LogToConsole: if true then log to standard output, default: true
  -OpenM.LogToFile:    if true then log to file
  -OpenM.LogFilePath:  path to log file, default = current/dir/exeName.log
  -OpenM.LogUseTs:     if true then use time-stamp in log file name
  -OpenM.LogUsePid:    if true then use pid-stamp in log file name
  -OpenM.LogNoMsgTime: if true then do not prefix log messages with date-time
  -OpenM.LogSql:       if true then log sql statements into log file
*/
package main

import (
	"errors"
	"flag"
	"os"
	"strings"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// dbcopy config keys to get values from ini-file or command line arguments.
const (
	copyToArgKey      = "dbcopy.To"               // copy to: text=db-to-text, db=text-to-db, db2db=db-to-db
	zipArgKey         = "dbcopy.Zip"              // create output or use as input model.zip
	inputDirArgKey    = "dbcopy.InputDir"         // input dir to read model .json and .csv files
	outputDirArgKey   = "dbcopy.OutputDir"        // output dir to write model .json and .csv files
	toDbConnectionStr = "dbcopy.ToDatabase"       // output db connection string
	toDbDriverName    = "dbcopy.ToDatabaseDriver" // output db driver name, ie: SQLite, odbc, sqlite3
	encodingArgKey    = "dbcopy.CodePage"         // code page for converting source files, e.g. windows-1252
)

func main() {
	defer exitOnPanic() // fatal error handler: log and exit

	err := mainBody(os.Args)
	if err != nil {
		omppLog.Log(err.Error())
		os.Exit(1)
	}
	omppLog.Log("Done.") // compeleted OK
}

func mainBody(args []string) error {

	// parse command line arguments and ini-file
	_ = flag.String(config.ModelName, "", "model name")
	_ = flag.String(config.ModelNameShort, "", "model name (short of "+config.ModelName+")")
	_ = flag.String(config.ModelDigest, "", "model hash digest")
	_ = flag.String(config.DbConnectionStr, "", "input database connection string")
	_ = flag.String(config.DbDriverName, db.SQLiteDbDriver, "input database driver name")
	_ = flag.String(copyToArgKey, "text", "copy to: `text`=db-to-text, db=text-to-db, db2db=db-to-db")
	_ = flag.String(toDbConnectionStr, "", "output database connection string")
	_ = flag.String(toDbDriverName, db.SQLiteDbDriver, "output database driver name")
	_ = flag.String(inputDirArgKey, "", "input directory to read model .json and .csv files")
	_ = flag.String(outputDirArgKey, "", "output directory for model .json and .csv files")
	_ = flag.String(config.ParamDir, "", "path to parameters directory (workset directory)")
	_ = flag.String(config.ParamDirShort, "", "path to parameters directory (short of "+config.ParamDir+")")
	_ = flag.Bool(zipArgKey, false, "create output model.zip or use model.zip as input")
	_ = flag.String(config.SetName, "", "workset name (set of model input parameters), if specified then copy only this workset")
	_ = flag.String(config.SetNameShort, "", "workset name (short of "+config.SetName+")")
	_ = flag.Int(config.SetId, 0, "workset id (set of model input parameters), if specified then copy only this workset")
	_ = flag.String(config.RunName, "", "model run name, if specified then copy only this run data")
	_ = flag.Int(config.RunId, 0, "model run id, if specified then copy only this run data")
	_ = flag.String(config.TaskName, "", "modeling task name, if specified then copy only this modeling task data")
	_ = flag.Int(config.TaskId, 0, "modeling task id, if specified then copy only this run modeling task data")
	_ = flag.String(config.DoubleFormat, "%.15g", "convert to string format for float and double")
	_ = flag.String(encodingArgKey, "", "code page to convert source file into utf-8, e.g.: windows-1252")

	runOpts, logOpts, err := config.New(encodingArgKey)
	if err != nil {
		return errors.New("invalid arguments: " + err.Error())
	}

	omppLog.New(logOpts) // adjust log options

	// model name or model digest is required
	modelName := runOpts.String(config.ModelName)
	modelDigest := runOpts.String(config.ModelDigest)

	if modelName == "" && modelDigest == "" {
		return errors.New("invalid (empty) model name and model digest")
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
			return errors.New("dbcopy invalid argument for copy-to: " + runOpts.String(copyToArgKey))
		}
		if err != nil {
			return err
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
			return errors.New("dbcopy invalid argument for copy-to: " + runOpts.String(copyToArgKey))
		}
		if err != nil {
			return err
		}
	}

	// copy single model run
	if isRun {
		switch strings.ToLower(runOpts.String(copyToArgKey)) {
		case "text":
			err = dbToTextRun(modelName, modelDigest, runOpts)
		case "db":
			err = textToDbRun(modelName, modelDigest, runOpts)
		case "db2db":
			err = dbToDbRun(modelName, modelDigest, runOpts)
		default:
			return errors.New("dbcopy invalid argument for copy-to: " + runOpts.String(copyToArgKey))
		}
		if err != nil {
			return err
		}
	}

	// copy single modeling task
	if isTask {
		switch strings.ToLower(runOpts.String(copyToArgKey)) {
		case "text":
			err = dbToTextTask(modelName, modelDigest, runOpts)
		case "db":
			err = textToDbTask(modelName, modelDigest, runOpts)
		case "db2db":
			err = dbToDbTask(modelName, modelDigest, runOpts)
		default:
			return errors.New("dbcopy invalid argument for copy-to: " + runOpts.String(copyToArgKey))
		}
		if err != nil {
			return err
		}
	}

	return err // return nil
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
