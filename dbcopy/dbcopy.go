// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
dbcopy is command line tool for import-export OpenM++ model metadata, input parameters and run results.

Arguments for dbcopy can be specified on command line or through .ini file:
  dbcopy -ini my.ini
Command line arguments take precedence over ini-file options.

Only model argument does not have default value and must be specified explicitly:
  dbcopy -m modelOne
  dbcopy -dbcopy.ModelName modelOne
  dbcopy -dbcopy.ModelDigest 649f17f26d67c37b78dde94f79772445

Model digest is globally unique and you may want to it if there are multiple versions of the model.

There are 4 possible copy directions: "text", "db", "db2db", "csv" and default is "text".
It is also possible to delete entire model or some model data from database (see dbcopy.Delete below).

Copy to "text": read from database and save into metadata .json and .csv values (parameters and output tables):
  dbcopy -m modelOne

Copy to "db": read from metadata .json and .csv values and insert or update database:
  dbcopy -m modelOne -dbcopy.To db

Copy to "db2db": direct copy between two databases:
  dbcopy -m modelOne -dbcopy.To db2db -dbcopy.ToDatabase "Database=dst.sqlite;OpenMode=ReadWrite"

Copy to "csv": read entire model from database and save .csv files:
  dbcopy -m modelOne -dbcopy.To csv

By default entire model data is copied.
It is also possible to copy only: model run results and input parameters,
set of input parameters (workset), modeling task metadata and run history.

To copy only one set of input parameters:
  dbcopy -m redModel -dbcopy.SetName Default
  dbcopy -m redModel -s Default

To copy only one model run results and input parameters:
  dbcopy -m modelOne -dbcopy.RunId 101
  dbcopy -m modelOne -dbcopy.RunName modelOne_2016_09_28_11_38_49_0945_101

To copy only one modeling task metadata and run history:
  dbcopy -m modelOne -dbcopy.TaskId 1
  dbcopy -m modelOne -dbcopy.TaskName taskOne

It may be convenient to pack (unpack) text files into .zip archive:
  dbcopy -m modelOne -dbcopy.Zip=true
  dbcopy -m modelOne -dbcopy.Zip
  dbcopy -m redModel -dbcopy.SetName Default -dbcopy.Zip

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
If neccesary you can specify exact directory for input parameters by using "-dbcopy.ParamDir" or "-p":
  dbcopy -m modelOne -dbcopy.SetId 2 -dbcopy.ParamDir two
  dbcopy -m modelOne -dbcopy.SetId 2 -p two
  dbcopy -m redModel -s Default -p 101 -dbcopy.To db -dbcopy.ToDatabase "Database=dst.sqlite;OpenMode=ReadWrite"

Dbcopy create output directories (and json files) for model data by combining model name and run name.
By default if run name is unique then output directory name is: modelName.run.MyRun
else it is: modelName.run.1234.MyRun where 1234 is run id.
To explicitly control usage of id's in directory names (and file names) specify IdOutputNames=true or IdOutputNames=false:
  dbcopy -m modelOne -dbcopy.To csv
  dbcopy -m modelOne -dbcopy.To csv -dbcopy.IdOutputNames=true
  dbcopy -m modelOne -dbcopy.To csv -dbcopy.IdOutputNames=false

By default parameters and output results .csv files contain codes in dimension column(s), e.g.: Sex=[Male,Female].
If you want to create csv files with numeric id's Sex=[0,1] instead then use IdCsv=true option:
  dbcopy -m modelOne -dbcopy.IdCsv
  dbcopy -m redModel -dbcopy.IdCsv -s Default
  dbcopy -m modelOne -dbcopy.IdCsv -dbcopy.RunId 101
  dbcopy -m modelOne -dbcopy.IdCsv -dbcopy.TaskName taskOne

Dbcopy do auto detect input files encoding to convert source text into utf-8.
On Windows you may want to expliciltly specify encoding name:
  dbcopy -m modelOne -dbcopy.To db -dbcopy.CodePage windows-1252

If you want to write utf-8 BOM into output csv file then:
  dbcopy -m modelOne -dbcopy.Utf8BomIntoCsv
  dbcopy -m modelOne -dbcopy.Utf8BomIntoCsv -dbcopy.To csv

To delete from database entire model, model run results, set of input parameters or modeling task:
  dbcopy -m modelOne -dbcopy.Delete
  dbcopy -m modelOne -dbcopy.Delete -dbcopy.RunId 101
  dbcopy -m modelOne -dbcopy.Delete -dbcopy.RunName modelOne_2016_09_28_11_38_49_0945_101
  dbcopy -m modelOne -dbcopy.Delete -dbcopy.SetId 2
  dbcopy -m modelOne -dbcopy.Delete -s Default
  dbcopy -m modelOne -dbcopy.Delete -dbcopy.TaskId 1
  dbcopy -m modelOne -dbcopy.Delete -dbcopy.TaskName taskOne

OpenM++ using hash digest to compare models, input parameters and output values.
By default float and double values converted into text with "%.15g" format.
It is possible to specify other format for float values digest calculation:
  dbcopy -m redModel -dbcopy.DoubleFormat "%.7G" -dbcopy.To db -dbcopy.ToDatabase "Database=dst.sqlite;OpenMode=ReadWrite"

By default dbcopy using SQLite database connection:
  dbcopy -m modelOne
is equivalent of:
  dbcopy -m modelOne -dbcopy.DatabaseDriver SQLite -dbcopy.Database "Database=modelOne.sqlite; Timeout=86400; OpenMode=ReadWrite;"

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
	copyToArgKey       = "dbcopy.To"               // copy to: text=db-to-text, db=text-to-db, db2db=db-to-db
	deleteArgKey       = "dbcopy.Delete"           // delete model or workset or model run from database
	modelNameArgKey    = "dbcopy.ModelName"        // model name
	modelNameShortKey  = "m"                       // model name (short form)
	modelDigestArgKey  = "dbcopy.ModelDigest"      // model hash digest
	setNameArgKey      = "dbcopy.SetName"          // workset name
	setNameShortKey    = "s"                       // workset name (short form)
	setIdArgKey        = "dbcopy.SetId"            // workset id, workset is a set of model input parameters
	runNameArgKey      = "dbcopy.RunName"          // model run name
	runIdArgKey        = "dbcopy.RunId"            // model run id
	taskNameArgKey     = "dbcopy.TaskName"         // modeling task name
	taskIdArgKey       = "dbcopy.TaskId"           // modeling task id
	dbConnStrArgKey    = "dbcopy.Database"         // db connection string
	dbDriverArgKey     = "dbcopy.DatabaseDriver"   // db driver name, ie: SQLite, odbc, sqlite3
	toDbConnStrArgKey  = "dbcopy.ToDatabase"       // output db connection string
	toDbDriverArgKey   = "dbcopy.ToDatabaseDriver" // output db driver name, ie: SQLite, odbc, sqlite3
	inputDirArgKey     = "dbcopy.InputDir"         // input dir to read model .json and .csv files
	outputDirArgKey    = "dbcopy.OutputDir"        // output dir to write model .json and .csv files
	paramDirArgKey     = "dbcopy.ParamDir"         // path to workset parameters directory
	paramDirShortKey   = "p"                       // path to workset parameters directory (short form)
	zipArgKey          = "dbcopy.Zip"              // create output or use as input model.zip
	doubleFormatArgKey = "dbcopy.DoubleFormat"     // convert to string format for float and double
	useIdCsvArgKey     = "dbcopy.IdCsv"            // if true then create csv files with enum id's default: enum code
	useIdNamesArgKey   = "dbcopy.IdOutputNames"    // if true then always use id's in output directory names, false never use it default: only if name conflict
	encodingArgKey     = "dbcopy.CodePage"         // code page for converting source files, e.g. windows-1252
	useUtf8CsvArgKey   = "dbcopy.Utf8BomIntoCsv"   // if true then write utf-8 BOM into csv file
)

// useIdNames is type to define how to make run and set directory and file names
type useIdNames uint8

const (
	defaultUseIdNames useIdNames = iota // default db facet
	yesUseIdNames                       // always use run and set id in directory and file names
	noUseIdNames                        // never use run and set id in directory and file names
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

	// set dbcopy command line argument keys and ini-file keys
	_ = flag.String(copyToArgKey, "text", "copy to: `text`=db-to-text, db=text-to-db, db2db=db-to-db, csv=db-to-csv")
	_ = flag.Bool(deleteArgKey, false, "delete from database: model or workset (set of model input parameters) or model run")
	_ = flag.String(modelNameArgKey, "", "model name")
	_ = flag.String(modelNameShortKey, "", "model name (short of "+modelNameArgKey+")")
	_ = flag.String(modelDigestArgKey, "", "model hash digest")
	_ = flag.String(setNameArgKey, "", "workset name (set of model input parameters), if specified then copy only this workset")
	_ = flag.String(setNameShortKey, "", "workset name (short of "+setNameArgKey+")")
	_ = flag.Int(setIdArgKey, 0, "workset id (set of model input parameters), if specified then copy only this workset")
	_ = flag.String(runNameArgKey, "", "model run name, if specified then copy only this run data")
	_ = flag.Int(runIdArgKey, 0, "model run id, if specified then copy only this run data")
	_ = flag.String(taskNameArgKey, "", "modeling task name, if specified then copy only this modeling task data")
	_ = flag.Int(taskIdArgKey, 0, "modeling task id, if specified then copy only this run modeling task data")
	_ = flag.String(dbConnStrArgKey, "", "input database connection string")
	_ = flag.String(dbDriverArgKey, db.SQLiteDbDriver, "input database driver name")
	_ = flag.String(toDbConnStrArgKey, "", "output database connection string")
	_ = flag.String(toDbDriverArgKey, db.SQLiteDbDriver, "output database driver name")
	_ = flag.String(inputDirArgKey, "", "input directory to read model .json and .csv files")
	_ = flag.String(outputDirArgKey, "", "output directory for model .json and .csv files")
	_ = flag.String(paramDirArgKey, "", "path to parameters directory (workset directory)")
	_ = flag.String(paramDirShortKey, "", "path to parameters directory (short of "+paramDirArgKey+")")
	_ = flag.Bool(zipArgKey, false, "create output model.zip or use model.zip as input")
	_ = flag.String(doubleFormatArgKey, "%.15g", "convert to string format for float and double")
	_ = flag.Bool(useIdCsvArgKey, false, "if true then create csv files with enum id's default: enum code")
	_ = flag.Bool(useIdNamesArgKey, false, "if true then always use id's in output directory names, false never use, by default: only if csv name conflict")
	_ = flag.String(encodingArgKey, "", "code page to convert source file into utf-8, e.g.: windows-1252")
	_ = flag.Bool(useUtf8CsvArgKey, false, "if true then write utf-8 BOM into csv file")

	// pairs of full and short argument names to map short name to full name
	var optFs = []config.FullShort{
		config.FullShort{Full: modelNameArgKey, Short: modelNameShortKey},
		config.FullShort{Full: setNameArgKey, Short: setNameShortKey},
		config.FullShort{Full: paramDirArgKey, Short: paramDirShortKey},
	}

	// parse command line arguments and ini-file
	runOpts, logOpts, extraArgs, err := config.New(encodingArgKey, optFs)
	if err != nil {
		return errors.New("invalid arguments: " + err.Error())
	}
	if len(extraArgs) > 0 {
		return errors.New("invalid arguments: " + strings.Join(extraArgs, " "))
	}

	omppLog.New(logOpts) // adjust log options according to command line arguments or ini-values

	// model name or model digest is required
	modelName := runOpts.String(modelNameArgKey)
	modelDigest := runOpts.String(modelDigestArgKey)

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
		(runOpts.IsExist(toDbConnStrArgKey) || runOpts.IsExist(toDbDriverArgKey)) {
		return errors.New("dbcopy invalid arguments: output database can be specified only if " + copyToArgKey + "=db or =db2db")
	}
	if copyToArg != "text" && copyToArg != "csv" && runOpts.IsExist(useIdCsvArgKey) {
		return errors.New("dbcopy invalid arguments: " + useIdCsvArgKey + " can be used only if " + copyToArgKey + "=text or =csv")
	}

	// do delete model run, workset or entire model
	// if not delete then copy: workset, model run data, modeilng task
	// by default: copy entire model

	switch {

	// do delete
	case isDel:

		switch {
		case runOpts.IsExist(runNameArgKey) || runOpts.IsExist(runIdArgKey): // delete model run
			err = dbDeleteRun(modelName, modelDigest, runOpts)
		case runOpts.IsExist(setNameArgKey) || runOpts.IsExist(setIdArgKey): // delete workset
			err = dbDeleteWorkset(modelName, modelDigest, runOpts)
		case runOpts.IsExist(taskNameArgKey) || runOpts.IsExist(taskIdArgKey): // delete modeling task
			err = dbDeleteTask(modelName, modelDigest, runOpts)
		default:
			err = dbDeleteModel(modelName, modelDigest, runOpts) // delete entrire model
		}

	// copy model run
	case !isDel && (runOpts.IsExist(runNameArgKey) || runOpts.IsExist(runIdArgKey)):

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
	case !isDel && (runOpts.IsExist(setNameArgKey) || runOpts.IsExist(setIdArgKey)):

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
	case !isDel && (runOpts.IsExist(taskNameArgKey) || runOpts.IsExist(taskIdArgKey)):

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
		case "csv":
			err = dbToCsv(modelName, modelDigest, runOpts)
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
