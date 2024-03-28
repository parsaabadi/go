// Copyright OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
dbget is command line tool to export OpenM++ model metadata, input parameters and run results.

Get list of the models from database:

	dbget -db modelOne.sqlite -do model-list

Aggregate microdata run values:

	dbget -db test\modelOne.sqlite -do microdata-aggregate
	  -m modelOne
	  -dbget.WithRunIds 219,221
	  -dbget.Entity Other
	  -dbget.GroupBy AgeGroup
	  -dbget.Calc OM_AVG(Income)

Compare microdata run values:

	dbget -db modelOne.sqlite -do microdata-compare
	  -m modelOne
	  -dbget.RunId 219
	  -dbget.WithRunIds 221
	  -dbget.Entity Person
	  -dbget.GroupBy AgeGroup
	  -dbget.Calc OM_AVG(Income[base]-Income[variant])

Aggregate and compare microdata run values:

	dbget -db test\modelOne.sqlite -do microdata-aggregate
	  -m modelOne
	  -dbget.RunId 219
	  -dbget.WithRunIds 221
	  -dbget.Entity Other
	  -dbget.GroupBy AgeGroup
	  -dbget.Calc OM_AVG(Income),OM_AVG(Income[base]-Income[variant])
*/
package main

import (
	"errors"
	"flag"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// dbget config keys to get values from ini-file or command line arguments.
const (
	cmdArgKey           = "dbget.Do"             // action, what to do, for example: model-list
	cmdShortKey         = "do"                   // action, what to do (short form)
	outputFileArgKey    = "dbget.File"           // output file name, default: action-name.csv, e.g.: model-list.csv
	outputFileShortKey  = "f"                    // output file name (short form)
	noFileArgKey        = "dbget.NoFile"         // if true then do not write into output files
	outputDirArgKey     = "dbget.Dir"            // output directory to write .csv or .json files
	outputDirShortKey   = "dir"                  // output directory (short form)
	keepOutputDirArgKey = "dbget.KeepOutputDir"  // keep output directory if it is already exist
	toConsoleArgKey     = "dbget.ToConsole"      // if true then write output to console
	toJsonArgKey        = "dbget.ToJson"         // if true then output to .json files, default: .csv
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
	entityArgKey        = "dbget.Entity"         // microdata entity name
	groupByArgKey       = "dbget.GroupBy"        // microdata group by attributes
	calcArgKey          = "dbget.Calc"           // calculation(s) expressions to compare or aggregate
	doubleFormatArgKey  = "dbget.DoubleFormat"   // convert to string format for float and double
	langArgKey          = "dbget.Language"       // prefered output language: fr-CA
	langShortKey        = "lang"                 // prefered output language (short form)
	encodingArgKey      = "dbget.CodePage"       // code page for converting source files, e.g. windows-1252
	useUtf8ArgKey       = "dbget.Utf8Bom"        // if true then write utf-8 BOM into output
)

// run options
var theCfg = struct {
	action          string // action name (what to do)
	fileName        string // output file name, default: action-name.csv
	dir             string // output directory
	isKeepOutputDir bool   // if true then keep existing output directory
	isNoFile        bool   // if true then do not write into output files
	isConsole       bool   // if true then output to console
	isJson          bool   // if true then output is .json files, default: .csv
	modelName       string // model name
	modelDigest     string // model digest
	doubleFmt       string // format to convert float or double value to string
	lang            string // prefered output language: fr-CA
	encodingName    string // "code page" to convert source file into utf-8, for example: windows-1252
	isWriteUtf8Bom  bool   // if true then write utf-8 BOM into csv file
}{
	doubleFmt:      "%.15g", // default format to convert float or double values to string
	encodingName:   "",      // by default detect utf-8 encoding or use OS-specific default: windows-1252 on Windowds and utf-8 outside
	isWriteUtf8Bom: false,   // do not write BOM by default
}

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

	_ = flag.String(cmdArgKey, "", "action, what to do, for example: model-list")
	_ = flag.String(cmdShortKey, "", "action, what to do (short of "+cmdArgKey+")")
	_ = flag.String(outputFileArgKey, theCfg.fileName, "output file name, default depends on action")
	_ = flag.String(outputFileShortKey, theCfg.fileName, "output file name (short of "+outputFileArgKey+")")
	_ = flag.Bool(noFileArgKey, theCfg.isNoFile, "if true then do not write into output files")
	_ = flag.String(outputDirArgKey, theCfg.dir, "output directory for model .json or .csv files")
	_ = flag.String(outputDirShortKey, theCfg.dir, "output directory (short of "+outputDirArgKey+")")
	_ = flag.Bool(keepOutputDirArgKey, theCfg.isKeepOutputDir, "keep (do not delete) existing output directory")
	_ = flag.Bool(toConsoleArgKey, theCfg.isConsole, "if true then write output to console")
	_ = flag.Bool(toJsonArgKey, theCfg.isJson, "if true then output to .json files, default: .csv")
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
	_ = flag.String(entityArgKey, "", "microdata entity name")
	_ = flag.String(groupByArgKey, "", "list of microdata group by attributes")
	_ = flag.String(calcArgKey, "", "list of calculation(s) expressions to compare or aggregate")
	_ = flag.String(doubleFormatArgKey, theCfg.doubleFmt, "convert to string format for float and double")
	_ = flag.String(langArgKey, "theCfg.lang", "prefered output language")
	_ = flag.String(langShortKey, "theCfg.lang", "prefered output language (short of "+langArgKey+")")
	_ = flag.String(encodingArgKey, theCfg.encodingName, "code page to convert source file into utf-8, e.g.: windows-1252")
	_ = flag.Bool(useUtf8ArgKey, theCfg.isWriteUtf8Bom, "if true then write utf-8 BOM into output")

	// pairs of full and short argument names to map short name to full name
	var optFs = []config.FullShort{
		{Full: cmdArgKey, Short: cmdShortKey},
		{Full: sqliteArgKey, Short: sqliteShortKey},
		{Full: modelNameArgKey, Short: modelNameShortKey},
		{Full: runArgKey, Short: runShortKey},
		{Full: outputFileArgKey, Short: outputFileShortKey},
		{Full: outputDirArgKey, Short: outputDirShortKey},
		{Full: langArgKey, Short: langShortKey},
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

	// get common run options
	theCfg.action = runOpts.String(cmdArgKey)
	theCfg.fileName = runOpts.String(outputFileArgKey)
	theCfg.isNoFile = runOpts.Bool(noFileArgKey)
	theCfg.dir = runOpts.String(outputDirArgKey)
	theCfg.isKeepOutputDir = runOpts.Bool(keepOutputDirArgKey)
	theCfg.isConsole = runOpts.Bool(toConsoleArgKey)
	theCfg.isJson = runOpts.Bool(toJsonArgKey)
	theCfg.lang = runOpts.String(langArgKey)
	theCfg.doubleFmt = runOpts.String(doubleFormatArgKey)
	theCfg.encodingName = runOpts.String(encodingArgKey)
	theCfg.isWriteUtf8Bom = runOpts.Bool(useUtf8ArgKey)

	if theCfg.isNoFile && !theCfg.isConsole {
		omppLog.Log("Warning: empty result, output to file and to console is disabled")
		return nil
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

	// model name or digest required if it is not a model-list
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
	}

	// remove output directory if required, create output directory if not already exists
	if err := makeOutputDir(); err != nil {
		return err
	}

	// dispatch the command
	switch theCfg.action {
	case "model-list":
		return modelList(srcDb, runOpts)
	case "microdata-aggregate":
		return microdataAggregate(srcDb, modelId, false, runOpts)
	case "microdata-compare":
		return microdataAggregate(srcDb, modelId, true, runOpts)
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
