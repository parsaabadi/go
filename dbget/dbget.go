// Copyright OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
dbget is command line tool to export OpenM++ model metadata, input parameters and run results.

Get list of the models from database:

	dbget -db modelOne.sqlite -do model-list

	dbget -m modelOne -do model-list -dbget.As csv
	dbget -m modelOne -do model-list -dbget.As tsv
	dbget -m modelOne -do model-list -dbget.As json

	dbget -m modelOne -do model-list -csv  -dbget.ToConsole
	dbget -m modelOne -do model-list -tsv  -dbget.ToConsole
	dbget -m modelOne -do model-list -json -dbget.ToConsole
	dbget -m modelOne -do model-list -tsv  -pipe

	dbget -m modelOne -do model-list -dbget.Language EN
	dbget -m modelOne -do model-list -lang fr-CA
	dbget -m modelOne -do model-list -lang isl

	dbget -m modelOne -do model-list -dbget.Notes -lang en-CA
	dbget -m modelOne -do model-list -dbget.Notes -lang fr-CA
	dbget -m modelOne -do model-list -dbget.Notes -lang isl
	dbget -m modelOne -do model-list -dbget.NoLanguage

	dbget -dbget.Sqlite modelOne.sqlite -dbget.Do model-list

	dbget
	  -dbget.Do model-list
	  -dbget.Database "Database=modelName.sqlite; Timeout=86400; OpenMode=ReadWrite;"
	  -dbget.DatabaseDriver SQLite

Get all model runs parameters and output table values:

	dbget -m modelOne -do all-runs
	dbget -m modelOne -do all-runs -lang fr-CA
	dbget -m modelOne -do all-runs -tsv
	dbget -m modelOne -do all-runs -dir my/output/dir
	dbget -m modelOne -do all-runs -pipe
	dbget -m modelOne -do all-runs -dbget.NoZeroCsv
	dbget -m modelOne -do all-runs -dbget.NoNullCsv
	dbget -m modelOne -do all-runs -dbget.NoZeroCsv -dbget.NoNullCsv

	dbget -dbget.Sqlite modelOne.sqlite -dbget.Do all-runs

Get model run parameters and output table values:

	dbget -m modelOne -do run -dbget.FirstRun
	dbget -m modelOne -do run -dbget.LastRun
	dbget -m modelOne -do run -r Default-4
	dbget -m modelOne -do run -r Default-4 -lang fr-CA
	dbget -m modelOne -do run -r Default-4 -tsv
	dbget -m modelOne -do run -r Default-4 -pipe
	dbget -m modelOne -do run -r Default-4 -dbget.NoZeroCsv
	dbget -m modelOne -do run -r Default-4 -dbget.NoNullCsv
	dbget -m modelOne -do run -r Default-4 -dbget.NoZeroCsv -dbget.NoNullCsv

	dbget -dbget.Sqlite modelOne.sqlite -dbget.Do run -dbget.Run Default

Get parameter run values:

	dbget -m modelOne -r Default -parameter ageSex
	dbget -m modelOne -r Default -parameter ageSex -lang fr-CA
	dbget -m modelOne -r Default -parameter ageSex -dbget.NoLanguage
	dbget -m modelOne -r Default -parameter ageSex -tsv
	dbget -m modelOne -r Default -parameter ageSex -pipe

	dbget -m modelOne -dbget.FirstRun -parameter ageSex
	dbget -m modelOne -dbget.LastRun  -parameter ageSex

	dbget -dbget.Sqlite modelOne.sqlite -dbget.Do parameter -dbget.Run Default -dbget.Parameter ageSex

Get output table values:

	dbget -m modelOne -r Default -table ageSexIncome
	dbget -m modelOne -r Default -table ageSexIncome -lang fr-CA
	dbget -m modelOne -r Default -table ageSexIncome -dbget.NoLanguage
	dbget -m modelOne -r Default -table ageSexIncome -tsv
	dbget -m modelOne -r Default -table ageSexIncome -pipe
	dbget -m modelOne -r Default -table ageSexIncome -dbget.NoZeroCsv
	dbget -m modelOne -r Default -table ageSexIncome -dbget.NoNullCsv

	dbget -m modelOne -dbget.FirstRun -table ageSexIncome
	dbget -m modelOne -dbget.LastRun  -table ageSexIncome

	dbget -dbget.Sqlite modelOne.sqlite -dbget.Do table -dbget.Run Default -dbget.Table ageSexIncome

Aggregate and compare microdata run values:

	dbget -m modelOne -do microdata-aggregate
	  -dbget.RunId 219
	  -dbget.Entity Other
	  -dbget.GroupBy AgeGroup
	  -dbget.Calc OM_AVG(Income)

	dbget -m modelOne -do microdata-aggregate
	  -dbget.FirstRun
	  -dbget.WithLastRun
	  -dbget.Entity Other
	  -dbget.GroupBy AgeGroup
	  -dbget.Calc OM_AVG(Income),OM_AVG(Income[base]-Income[variant])

	dbget -m modelOne -do microdata-compare
	  -dbget.RunId 219
	  -dbget.WithRunIds 221
	  -dbget.Entity Person
	  -dbget.GroupBy AgeGroup
	  -dbget.Calc OM_AVG(Income[base]-Income[variant])

Get model metadata from compatibility (Modgen) views:

	dbget -m modelOne -do old-model
	dbget -m modelOne -do old-model -csv
	dbget -m modelOne -do old-model -tsv
	dbget -m modelOne -do old-model -json
	dbget -m modelOne -do old-model -pipe

	dbget -dbget.Sqlite modelOne.sqlite -dbget.Do old-model -dbget.As csv -dbget.ToConsole -dbget.Language FR

Get model run parameters and out tables values from compatibility (Modgen) views:

	dbget -m modelOne -do old-run
	dbget -m modelOne -do old-run -csv
	dbget -m modelOne -do old-run -tsv
	dbget -m modelOne -do old-run -lang fr-CA
	dbget -m modelOne -do old-run -dbget.NoLanguage
	dbget -m modelOne -do old-run -pipe
	dbget -m modelOne -do old-run -dir my/dir
	dbget -m modelOne -do old-run -dbget.NoZeroCsv
	dbget -m modelOne -do old-run -dbget.NoNullCsv

	dbget -dbget.Sqlite modelOne.sqlite -dbget.Do old-run -dbget.As csv -dbget.ToConsole -dbget.Language FR

Get parameter run values from compatibility (Modgen) views:

	dbget -m modelOne -do old-parameter -dbget.Parameter ageSex
	dbget -m modelOne -do old-parameter -dbget.Parameter ageSex -csv
	dbget -m modelOne -do old-parameter -dbget.Parameter ageSex -tsv
	dbget -m modelOne -do old-parameter -dbget.Parameter ageSex -lang fr-CA
	dbget -m modelOne -do old-parameter -dbget.Parameter ageSex -pipe

	dbget -dbget.Sqlite modelOne.sqlite -dbget.Do old-parameter -dbget.Parameter ageSex -dbget.As csv -dbget.ToConsole -dbget.Language FR

Get output table values from compatibility (Modgen) views:

	dbget -m modelOne -do old-table -dbget.Table salarySex
	dbget -m modelOne -do old-table -dbget.Table salarySex -csv
	dbget -m modelOne -do old-table -dbget.Table salarySex -tsv
	dbget -m modelOne -do old-table -dbget.Table salarySex -lang fr-CA
	dbget -m modelOne -do old-table -dbget.Table salarySex -pipe
	dbget -m modelOne -do old-table -dbget.Table salarySex -dbget.NoZeroCsv
	dbget -m modelOne -do old-table -dbget.Table salarySex -dbget.NoNullCsv

	dbget -dbget.Sqlite modelOne.sqlite -dbget.Do old-table -dbget.Table ageSexIncome -dbget.As csv -dbget.ToConsole -dbget.Language FR
*/
package main

import (
	"errors"
	"flag"
	"os"
	"strconv"
	"strings"

	"github.com/jeandeaual/go-locale"
	_ "github.com/mattn/go-sqlite3"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// dbget config keys to get values from ini-file or command line arguments.
const (
	cmdArgKey           = "dbget.Do"             // action, what to do, for example: model-list
	cmdShortKey         = "do"                   // action, what to do (short form)
	asArgKey            = "dbget.As"             // output as csv, tsv or json, default: .csv
	csvArgKey           = "csv"                  // short form of: dbget.As csv
	tsvArgKey           = "tsv"                  // short form of: dbget.As tsv
	jsonArgKey          = "json"                 // short form of: dbget.As json
	outputFileArgKey    = "dbget.File"           // output file name, default: action-name.csv, e.g.: model-list.csv
	outputFileShortKey  = "f"                    // output file name (short form)
	outputDirArgKey     = "dbget.Dir"            // output directory to write .csv or .json files
	outputDirShortKey   = "dir"                  // output directory (short form)
	keepOutputDirArgKey = "dbget.KeepOutputDir"  // keep output directory if it is already exist
	consoleArgKey       = "dbget.ToConsole"      // if true then use stdout and do not create file(s)
	consoleShortKey     = "pipe"                 // short form of: -dbget.ToConsole -OpenM.LogToConsole=false
	langArgKey          = "dbget.Language"       // prefered output language: fr-CA
	langShortKey        = "lang"                 // prefered output language (short form)
	noLangArgKey        = "dbget.NoLanguage"     // if true then do language-neutral output: enum codes and "C" formats
	encodingArgKey      = "dbget.CodePage"       // code page for converting source files, e.g. windows-1252
	useUtf8ArgKey       = "dbget.Utf8Bom"        // if true then write utf-8 BOM into output
	noteArgKey          = "dbget.Notes"          // if true then output notes into .md files
	doubleFormatArgKey  = "dbget.DoubleFormat"   // convert to string format for float and double
	noZeroArgKey        = "dbget.NoZeroCsv"      // if true then do not write zero values into output tables or microdata csv
	noNullArgKey        = "dbget.NoNullCsv"      // if true then do not write NULL values into output tables or microdata csv
	sqliteArgKey        = "dbget.Sqlite"         // input db SQLite path
	sqliteShortKey      = "db"                   // input db SQLite path (short form)
	dbConnStrArgKey     = "dbget.Database"       // db connection string
	dbDriverArgKey      = "dbget.DatabaseDriver" // db driver name, ie: SQLite, odbc, sqlite3
	modelNameArgKey     = "dbget.ModelName"      // model name
	modelNameShortKey   = "m"                    // model name (short form)
	modelDigestArgKey   = "dbget.ModelDigest"    // model hash digest
	runArgKey           = "dbget.Run"            // model run digest, stamp or name
	runShortKey         = "r"                    // model run digest, stamp or name (short form)
	runIdArgKey         = "dbget.RunId"          // model run id
	runFirstArgKey      = "dbget.FirstRun"       // use first model run
	runLastArgKey       = "dbget.LastRun"        // use last model run
	withRunsArgKey      = "dbget.WithRuns"       // with model run digests, stamps or names (variant runs)
	withRunIdsArgKey    = "dbget.WithRunIds"     // with list model run id's (variant runs)
	withRunFirstArgKey  = "dbget.WithFirstRun"   // with first model run (with first run as variant)
	withRunLastArgKey   = "dbget.WithLastRun"    // with last model run (with last run as variant)
	paramArgKey         = "dbget.Parameter"      // parameter name
	paramShortKey       = "parameter"            // short form of: -dbget.do parameter -dbget.Parameter Name
	tableArgKey         = "dbget.Table"          // output table name
	tableShortKey       = "table"                // short form of: -dbget.do table -dbget.Table Name
	entityArgKey        = "dbget.Entity"         // microdata entity name
	groupByArgKey       = "dbget.GroupBy"        // microdata group by attributes
	calcArgKey          = "dbget.Calc"           // calculation(s) expressions to compare or aggregate
)

// output format: csv by default, or tsv or json
type outputAs int

const (
	asCsv outputAs = iota
	asTsv
	asJson
)

// run options
var theCfg = struct {
	action          string   // action name (what to do)
	kind            outputAs // output as csv, tsv or json
	fileName        string   // output file name, default depends on action
	dir             string   // output directory
	isKeepOutputDir bool     // if true then keep existing output directory
	isConsole       bool     // if true then write into stdout
	modelName       string   // model name
	modelDigest     string   // model digest
	doubleFmt       string   // format to convert float or double value to string
	userLang        string   // prefered output language: fr-CA
	lang            string   // model language matched to user language
	isNoLang        bool     // if true then do language-neutral output: enum codes and "C" formats
	encodingName    string   // "code page" to convert source file into utf-8, for example: windows-1252
	isWriteUtf8Bom  bool     // if true then write utf-8 BOM into csv file
	isNote          bool     // if true then output notes into .md files
}{
	kind:           asCsv,   // by default output as as .csv
	encodingName:   "",      // by default detect utf-8 encoding or use OS-specific default: windows-1252 on Windowds and utf-8 outside
	isWriteUtf8Bom: false,   // do not write BOM by default
	doubleFmt:      "%.15g", // default format to convert float or double values to string
}

const logPeriod = 5 // seconds, log periodically if output takes a long time

// main entry point: wrapper to handle errors
func main() {
	defer exitOnPanic() // fatal error handler: log and exit

	err := mainBody(os.Args)
	if err != nil {
		omppLog.Log(err.Error())
		os.Exit(1)
	}
	omppLog.Log("Done.") // compeleted OK
}

// actual main body
func mainBody(args []string) error {

	isPipe := false
	doParamName := ""
	doTableName := ""
	_ = flag.String(cmdArgKey, "", "action, what to do, for example: model-list")
	_ = flag.String(cmdShortKey, "", "action, what to do (short of "+cmdArgKey+")")
	_ = flag.String(asArgKey, "", "output as .csv, .tsv or .json, default: .csv")
	_ = flag.Bool(csvArgKey, true, "output as .csv (short of "+asArgKey+" csv)")
	_ = flag.Bool(tsvArgKey, false, "output as .tsv (short of "+asArgKey+" tsv)")
	_ = flag.Bool(jsonArgKey, false, "output as .json (short of "+asArgKey+" json)")
	_ = flag.String(outputFileArgKey, theCfg.fileName, "output file name, default depends on action")
	_ = flag.String(outputFileShortKey, theCfg.fileName, "output file name (short of "+outputFileArgKey+")")
	_ = flag.String(outputDirArgKey, theCfg.dir, "output directory for model .json or .csv files")
	_ = flag.String(outputDirShortKey, theCfg.dir, "output directory (short of "+outputDirArgKey+")")
	_ = flag.Bool(keepOutputDirArgKey, theCfg.isKeepOutputDir, "keep (do not delete) existing output directory")
	_ = flag.Bool(consoleArgKey, theCfg.isConsole, "if true then write into standard output instead of file(s)")
	flag.BoolVar(&isPipe, consoleShortKey, theCfg.isConsole, "short form of: -"+consoleArgKey+" -"+config.LogToConsoleArgKey+"=false")
	_ = flag.String(langArgKey, theCfg.userLang, "prefered output language")
	_ = flag.String(langShortKey, theCfg.userLang, "prefered output language (short of "+langArgKey+")")
	_ = flag.Bool(noLangArgKey, theCfg.isNoLang, "if true then do language-neutral output: enum codes and 'C' formats")
	_ = flag.String(encodingArgKey, theCfg.encodingName, "code page to convert source file into utf-8, e.g.: windows-1252")
	_ = flag.Bool(useUtf8ArgKey, theCfg.isWriteUtf8Bom, "if true then write utf-8 BOM into output")
	_ = flag.Bool(noteArgKey, theCfg.isNote, "if true then write notes into .md files")
	_ = flag.String(doubleFormatArgKey, theCfg.doubleFmt, "convert to string format for float and double")
	_ = flag.Bool(noZeroArgKey, false, "if true then do not write zero values into output tables .csv files")
	_ = flag.Bool(noNullArgKey, false, "if true then do not write NULL values into output tables .csv files")
	_ = flag.String(sqliteArgKey, "", "input database SQLite file path")
	_ = flag.String(sqliteShortKey, "", "model name (short of "+sqliteArgKey+")")
	_ = flag.String(dbConnStrArgKey, "", "input database connection string")
	_ = flag.String(dbDriverArgKey, db.SQLiteDbDriver, "input database driver name: SQLite, odbc, sqlite3")
	_ = flag.String(modelNameArgKey, "", "model name")
	_ = flag.String(modelNameShortKey, "", "model name (short of "+modelNameArgKey+")")
	_ = flag.String(modelDigestArgKey, "", "model hash digest")
	_ = flag.String(runArgKey, "", "model run digest, run stamp or run name")
	_ = flag.String(runShortKey, "", "model run digest, run stamp or run name (short of "+runArgKey+")")
	_ = flag.Int(runIdArgKey, 0, "model run id")
	_ = flag.Bool(runFirstArgKey, false, "if true then use first model run")
	_ = flag.Bool(runLastArgKey, false, "if true then use last model run")
	_ = flag.String(withRunsArgKey, "", "with model run digests, stamps or names (variant runs)")
	_ = flag.String(withRunIdsArgKey, "", "with list model run id's (variant runs)")
	_ = flag.Bool(withRunFirstArgKey, false, "if true then use first model run (use as variant run)")
	_ = flag.Bool(withRunLastArgKey, false, "if true then use last model run (use as variant run)")
	_ = flag.String(paramArgKey, "", "parameter name")
	flag.StringVar(&doParamName, paramShortKey, "", "short form of: -"+cmdArgKey+" parameter -"+paramArgKey+" Name")
	_ = flag.String(tableArgKey, "", "output table name")
	flag.StringVar(&doTableName, tableShortKey, "", "short form of: -"+cmdArgKey+" table -"+tableArgKey+" Name")
	_ = flag.String(entityArgKey, "", "microdata entity name")
	_ = flag.String(groupByArgKey, "", "list of microdata group by attributes")
	_ = flag.String(calcArgKey, "", "list of calculation(s) expressions to compare or aggregate")

	// pairs of full and short argument names to map short name to full name
	var optFs = []config.FullShort{
		{Full: cmdArgKey, Short: cmdShortKey},
		{Full: sqliteArgKey, Short: sqliteShortKey},
		{Full: modelNameArgKey, Short: modelNameShortKey},
		{Full: runArgKey, Short: runShortKey},
		{Full: outputFileArgKey, Short: outputFileShortKey},
		{Full: outputDirArgKey, Short: outputDirShortKey},
		{Full: consoleArgKey, Short: consoleShortKey},
		{Full: langArgKey, Short: langShortKey},
		{Full: paramArgKey, Short: paramShortKey},
		{Full: tableArgKey, Short: tableShortKey},
	}

	// parse command line arguments and ini-file
	runOpts, logOpts, extraArgs, err := config.New(encodingArgKey, optFs)
	if err != nil {
		return errors.New("invalid arguments: " + err.Error())
	}
	if len(extraArgs) > 0 {
		return errors.New("invalid arguments: " + strings.Join(extraArgs, " "))
	}
	if isPipe {
		logOpts.IsConsole = false // suppress log console output if -pipe required
	}
	omppLog.New(logOpts) // adjust log options according to command line arguments or ini-values

	// get common run options
	theCfg.action = runOpts.String(cmdArgKey)
	theCfg.fileName = helper.CleanFileName(runOpts.String(outputFileArgKey))
	theCfg.dir = helper.CleanFilePath(runOpts.String(outputDirArgKey))
	theCfg.isKeepOutputDir = runOpts.Bool(keepOutputDirArgKey)
	theCfg.isConsole = runOpts.Bool(consoleArgKey)
	theCfg.userLang = runOpts.String(langArgKey)
	theCfg.isNoLang = runOpts.Bool(noLangArgKey)
	theCfg.encodingName = runOpts.String(encodingArgKey)
	theCfg.isWriteUtf8Bom = runOpts.Bool(useUtf8ArgKey)
	theCfg.isNote = runOpts.Bool(noteArgKey)
	theCfg.doubleFmt = runOpts.String(doubleFormatArgKey)

	// get output format: cv, tsv or json
	if f := runOpts.String(asArgKey); f != "" {

		if runOpts.IsExist(csvArgKey) || runOpts.IsExist(tsvArgKey) || runOpts.IsExist(jsonArgKey) {
			return errors.New("invalid arguments: " + csvArgKey + " or " + tsvArgKey + " or " + jsonArgKey)
		}
		switch strings.ToLower(f) {
		case "csv":
			theCfg.kind = asCsv
		case "tsv":
			theCfg.kind = asTsv
		case "json":
			theCfg.kind = asJson
		default:
			return errors.New("invalid arguments: " + asArgKey + " " + f)
		}
	} else {
		if runOpts.IsExist(csvArgKey) && (runOpts.IsExist(tsvArgKey) || runOpts.IsExist(jsonArgKey)) ||
			runOpts.IsExist(tsvArgKey) && (runOpts.IsExist(csvArgKey) || runOpts.IsExist(jsonArgKey)) ||
			runOpts.IsExist(jsonArgKey) && (runOpts.IsExist(csvArgKey) || runOpts.IsExist(tsvArgKey)) {
			return errors.New("invalid arguments: " + csvArgKey + " or " + tsvArgKey + " or " + jsonArgKey)
		}
		switch {
		case runOpts.IsExist(csvArgKey) && runOpts.Bool(csvArgKey):
			theCfg.kind = asCsv
		case runOpts.IsExist(tsvArgKey) && runOpts.Bool(tsvArgKey):
			theCfg.kind = asTsv
		case runOpts.IsExist(jsonArgKey) && runOpts.Bool(jsonArgKey):
			theCfg.kind = asJson
		// if there is no dbget.As argument and there is no dbget.csv, dbget.tsv, dbget.json
		// then use output file name extension to detect kind of output
		case !runOpts.IsExist(csvArgKey) && !runOpts.IsExist(tsvArgKey) && !runOpts.IsExist(jsonArgKey):
			// if file name is empty or extension is unknown then result is csv by default
			theCfg.kind = kindByExt(theCfg.fileName)
		default:
			return errors.New("invalid arguments: " + csvArgKey + " or " + tsvArgKey + " or " + jsonArgKey)
		}
	}

	// output to json supported only for model metadata
	if theCfg.kind == asJson {
		if theCfg.action != "model-list" && theCfg.action != "old-model" {
			return errors.New("JSON output not allowed for: " + theCfg.action)
		}
	}

	// get default user language
	if !theCfg.isNoLang && theCfg.userLang == "" {
		if ln, e := locale.GetLocale(); e == nil {
			theCfg.userLang = ln
		} else {
			omppLog.Log("Warning: unable to get user default language")
		}
	}

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefaultReadOnly(runOpts.String(modelNameArgKey), runOpts.String(sqliteArgKey), runOpts.String(dbConnStrArgKey), runOpts.String(dbDriverArgKey))

	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	if err := db.CheckOpenmppSchemaVersion(srcDb); err != nil {
		srcDb.Close()
		return err
	}

	// if it is not a model-list then
	//   find by model name or digest
	//   match model language to user language
	modelId := 0
	if theCfg.action != "model-list" {

		theCfg.modelName = runOpts.String(modelNameArgKey)
		theCfg.modelDigest = runOpts.String(modelDigestArgKey)

		if theCfg.modelName == "" && theCfg.modelDigest == "" {
			return errors.New("invalid (empty) model name and model digest")
		}
		omppLog.Log("Model ", theCfg.modelName, " ", theCfg.modelDigest)

		// check if model exists in database
		ok := false
		if ok, modelId, err = db.GetModelId(srcDb, theCfg.modelName, theCfg.modelDigest); err != nil {
			return err
		}
		if !ok {
			return errors.New("model " + theCfg.modelName + " " + theCfg.modelDigest + " not found")
		}
		mdRow, err := db.GetModelRow(srcDb, modelId)
		if err != nil {
			return err
		}
		if mdRow == nil {
			return errors.New("model not found by Id: " + strconv.Itoa(modelId))
		}

		// match user language to model language, use default model language if there are no match
		if !theCfg.isNoLang {
			if theCfg.userLang != "" {
				theCfg.lang, err = matchUserLang(srcDb, *mdRow)
				if err != nil {
					return err
				}
				if theCfg.lang == "" {
					omppLog.Log("Warning: unable to match user language: ", theCfg.userLang)
				}
			}
			if theCfg.lang != "" {
				omppLog.Log("Using model language: ", theCfg.lang)
			} else {
				theCfg.lang = mdRow.DefaultLangCode
				omppLog.Log("Using default model language: ", theCfg.lang)
			}
		}
	}

	// remove output directory if required, create output directory if not already exists
	if err := makeOutputDir(theCfg.dir, theCfg.isKeepOutputDir); err != nil {
		return err
	}

	if doParamName != "" {
		if runOpts.IsExist(cmdArgKey) && theCfg.action != "parameter" {
			return errors.New("invalid action argument: " + theCfg.action)
		}
		theCfg.action = "parameter"
	}
	if doTableName != "" {
		if runOpts.IsExist(cmdArgKey) && theCfg.action != "table" {
			return errors.New("invalid action argument: " + theCfg.action)
		}
		theCfg.action = "table"
	}

	// dispatch the command
	switch theCfg.action {
	case "model-list":
		return modelList(srcDb)
	case "run":
		return runValue(srcDb, modelId, runOpts)
	case "all-runs":
		return runAllValue(srcDb, modelId, runOpts)
	case "all-sets":
		return setAllValue(srcDb, modelId, runOpts)
	case "set":
		return setValue(srcDb, modelId, runOpts)
	case "parameter":
		return parameterValue(srcDb, modelId, runOpts)
	case "table":
		return tableValue(srcDb, modelId, runOpts)
	case "microdata-aggregate":
		return microdataAggregate(srcDb, modelId, false, runOpts)
	case "microdata-compare":
		return microdataAggregate(srcDb, modelId, true, runOpts)
	case "old-model":
		return modelOldMeta(srcDb, modelId)
	case "old-run":
		return runOldValue(srcDb, modelId, runOpts)
	case "old-parameter":
		return parameterOldValue(srcDb, modelId, runOpts)
	case "old-table":
		return tableOldValue(srcDb, modelId, runOpts)
	}
	return errors.New("invalid action argument: " + theCfg.action)
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
