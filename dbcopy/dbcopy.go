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
It is also possible to delete entire model or some model data from database (see dbcopy.Delete below).

Copy to "text": read from database and save into metadata .json and .csv values (parameters and output tables):
  dbcopy -m modelOne

Copy to "db": read from metadata .json and .csv values and insert or update database:
  dbcopy -m modelOne -dbcopy.To db

Copy to "db2db": direct copy between two databases:
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
  dbcopy -m modelOne -OpenM.TaskId 1
  dbcopy -m modelOne -OpenM.TaskName taskOne

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

To avoid path conflicts InputDir and OutputDir combined with model name, model run name or name of input parameters set.
For example:
  dbcopy -m redModel -dbcopy.OutputDir red -s Default
will place "Default" input set of parameters into directory one/redModel.set.Default.
You can override this rul by using "-OpenM.ParamDir" or "-p" to specify exact location of parameters set:
  dbcopy -m modelOne -OpenM.SetId 2 -OpenM.ParamDir two
  dbcopy -m modelOne -OpenM.SetId 2 -p two
  dbcopy -m redModel -s Default -p 101 -dbcopy.To db -dbcopy.ToDatabase "Database=dst.sqlite;OpenMode=ReadWrite"

By default parameters and output results .csv files contain codes in dimension column(s), e.g.: Sex=[Male,Female].
If you want to create csv files with numeric id's Sex=[0,1] instead then use ToIdCsv=true option:
  dbcopy -m modelOne -dbcopy.ToIdCsv
  dbcopy -m redModel -dbcopy.ToIdCsv -s Default
  dbcopy -m modelOne -dbcopy.ToIdCsv -OpenM.RunId 101
  dbcopy -m modelOne -dbcopy.ToIdCsv -OpenM.TaskName taskOne

To delete from database entire model, model run results, set of input parameters or modeling task:
  dbcopy -m modelOne -dbcopy.Delete
  dbcopy -m modelOne -dbcopy.Delete -OpenM.RunId 101
  dbcopy -m modelOne -dbcopy.Delete -OpenM.RunName modelOne_2016_09_28_11_38_49_0945_101
  dbcopy -m modelOne -dbcopy.Delete -OpenM.SetId 2
  dbcopy -m modelOne -dbcopy.Delete -s Default
  dbcopy -m modelOne -dbcopy.Delete -OpenM.TaskId 1
  dbcopy -m modelOne -dbcopy.Delete -OpenM.TaskName taskOne

OpenM++ using hash digest to compare models, input parameters and output values.
By default float and double values converted into text with "%.15g" format.
It is possible to specify other format for float values digest calculation:
  dbcopy -m redModel -OpenM.DoubleFormat "%.7G" -dbcopy.To db -dbcopy.ToDatabase "Database=dst.sqlite;OpenMode=ReadWrite"

By default dbcopy using SQLite database connection:
  dbcopy -m modelOne
is equivalent of:
  dbcopy -m modelOne -OpenM.DatabaseDriver SQLite -OpenM.Database "Database=modelOne.sqlite; Timeout=86400; OpenMode=ReadWrite;"

Output database connection settings by default are the same as input database,
which may not be suitable because you don't want to overwrite input database.

To specify output database connection string and driver:
  dbcopy -m modelOne -dbcopy.To db -dbcopy.ToDatabaseDriver SQLite -dbcopy.ToDatabase "Database=dst.sqlite; Timeout=86400; OpenMode=ReadWrite;"
or skip default database driver name "SQLite":
  dbcopy -m modelOne -dbcopy.To db -dbcopy.ToDatabase "Database=dst.sqlite; Timeout=86400; OpenMode=ReadWrite;"

Other supported database drivers are "sqlite3" and "odbc":
  dbcopy -m modelOne -dbcopy.To db -dbcopy.ToDatabaseDriver odbc -dbcopy.ToDatabase "DSN=bigSql"
  dbcopy -m modelOne -dbcopy.To db -dbcopy.ToDatabaseDriver sqlite3 -dbcopy.ToDatabase "file:dst.sqlite?mode=rw"

ODBC dbcopy tested with MySQL (MariaDB), PostgreSQL, Microsoft SQL, Oracle and DB2.

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
	useIdCsvArgKey    = "dbcopy.ToIdCsv"          // if true then create csv files with enum id's default: enum code
	deleteArgKey      = "dbcopy.Delete"           // delete model or workset or model run from database
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
	_ = flag.Bool(deleteArgKey, false, "delete from database: model or workset (set of model input parameters) or model run")
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
	_ = flag.Bool(useIdCsvArgKey, false, "if true then create csv files with enum id's default: enum code")
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

	// minimal validation of run options:
	// to-database can be used only with "db" or "db2db"
	// id csv is only for output
	copyToArg := strings.ToLower(runOpts.String(copyToArgKey))
	isDel := runOpts.Bool(deleteArgKey)

	if isDel && runOpts.IsExist(copyToArgKey) {
		return errors.New("dbcopy invalid arguments: " + deleteArgKey + " cannot be used with " + copyToArgKey)
	}
	if copyToArg != "db" && copyToArg != "db2db" &&
		(runOpts.IsExist(toDbConnectionStr) || runOpts.IsExist(toDbDriverName)) {
		return errors.New("dbcopy invalid arguments: output database can be specified only if " + copyToArgKey + "=db or =db2db")
	}
	if copyToArg != "text" && runOpts.IsExist(useIdCsvArgKey) {
		return errors.New("dbcopy invalid arguments: " + useIdCsvArgKey + " can be used only if " + copyToArgKey + "=text")
	}

	// do delete model run, workset or entire model
	// if not delete then copy: workset, model run data, modeilng task
	// by default: copy entire model

	switch {

	// do delete
	case isDel:

		switch {
		case runOpts.IsExist(config.RunName) || runOpts.IsExist(config.RunId): // delete model run
			err = dbDeleteRun(modelName, modelDigest, runOpts)
		case runOpts.IsExist(config.SetName) || runOpts.IsExist(config.SetId): // delete workset
			err = dbDeleteWorkset(modelName, modelDigest, runOpts)
		case runOpts.IsExist(config.TaskName) || runOpts.IsExist(config.TaskId): // delete modeling task
			err = dbDeleteTask(modelName, modelDigest, runOpts)
		default:
			err = dbDeleteModel(modelName, modelDigest, runOpts) // delete entrire model
		}

	// copy model run
	case !isDel && (runOpts.IsExist(config.RunName) || runOpts.IsExist(config.RunId)):

		switch copyToArg {
		case "text":
			err = dbToTextRun(modelName, modelDigest, runOpts)
		case "db":
			err = textToDbRun(modelName, modelDigest, runOpts)
		case "db2db":
			err = dbToDbRun(modelName, modelDigest, runOpts)
		default:
			return errors.New("dbcopy invalid argument for copy-to: " + copyToArg)
		}

	// copy workset
	case !isDel && (runOpts.IsExist(config.SetName) || runOpts.IsExist(config.SetId)):

		switch copyToArg {
		case "text":
			err = dbToTextWorkset(modelName, modelDigest, runOpts)
		case "db":
			err = textToDbWorkset(modelName, modelDigest, runOpts)
		case "db2db":
			err = dbToDbWorkset(modelName, modelDigest, runOpts)
		default:
			return errors.New("dbcopy invalid argument for copy-to: " + copyToArg)
		}

	// copy modeling task
	case !isDel && (runOpts.IsExist(config.TaskName) || runOpts.IsExist(config.TaskId)):

		switch copyToArg {
		case "text":
			err = dbToTextTask(modelName, modelDigest, runOpts)
		case "db":
			err = textToDbTask(modelName, modelDigest, runOpts)
		case "db2db":
			err = dbToDbTask(modelName, modelDigest, runOpts)
		default:
			return errors.New("dbcopy invalid argument for copy-to: " + copyToArg)
		}

	default: // copy entire model

		switch copyToArg {
		case "text":
			err = dbToText(modelName, modelDigest, runOpts)
		case "db":
			err = textToDb(modelName, runOpts)
		case "db2db":
			err = dbToDb(modelName, modelDigest, runOpts)
		default:
			return errors.New("dbcopy invalid argument for copy-to: " + copyToArg)
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
