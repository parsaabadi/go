// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
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

	// do copy operation
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

// copy model from database into text json and csv files
func dbToText(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(config.DbConnectionStr), runOpts.String(config.DbDriverName))
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	nv, err := db.OpenmppSchemaVersion(srcDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid database, likely not an openM++ database")
	}

	// get model metadata
	modelDef, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = modelDef.Model.Name // set model name: it can be empty and only model digest specified

	// create new output directory, use modelName subdirectory
	outDir := filepath.Join(runOpts.String(outputDirArgKey), modelName)
	err = os.MkdirAll(outDir, 0750)
	if err != nil {
		return err
	}

	// write model definition to json file
	if err = toModelJsonFile(srcDb, modelDef, outDir); err != nil {
		return err
	}

	// write all model run data into csv files: parameters, output expressions and accumulators
	if err = toCsvRunFile(srcDb, modelDef, outDir); err != nil {
		return err
	}

	// write all readonly workset data into csv files: input parameters
	if err = toCsvWorksetFile(srcDb, modelDef, outDir); err != nil {
		return err
	}

	// write all modeling tasks and task run history to json file
	if err = toTaskJsonFile(srcDb, modelDef, outDir); err != nil {
		return err
	}

	// pack model metadata, run results and worksets into zip
	if runOpts.Bool(zipArgKey) {
		zipPath, err := helper.PackZip(outDir, "")
		if err != nil {
			return err
		}
		omppLog.Log("Packed ", zipPath)
	}

	return nil
}

// copy model from text json and csv files into database
func textToDb(modelName string, runOpts *config.RunOptions) error {

	// get connection string and driver name
	// use OpenM options if DBCopy ouput database not defined
	cs := runOpts.String(toDbConnectionStr)
	if cs == "" && runOpts.IsExist(config.DbConnectionStr) {
		cs = runOpts.String(config.DbConnectionStr)
	}

	dn := runOpts.String(toDbDriverName)
	if dn == "" && runOpts.IsExist(config.DbDriverName) {
		dn = runOpts.String(config.DbDriverName)
	}

	cs, dn = db.IfEmptyMakeDefault(modelName, cs, dn)

	// open destination database and check is it valid
	dstDb, dbFacet, err := db.Open(cs, dn, true)
	if err != nil {
		return err
	}
	defer dstDb.Close()

	nv, err := db.OpenmppSchemaVersion(dstDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid database, likely not an openM++ database")
	}

	// use modelName as subdirectory inside of input and output directories or as name of model.zip file
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}
	inpDir := runOpts.String(inputDirArgKey)
	outDir := runOpts.String(outputDirArgKey)

	if !runOpts.Bool(zipArgKey) {
		inpDir = filepath.Join(inpDir, modelName) // json and csv files located in modelName subdir
	} else {
		omppLog.Log("Unpack ", modelName, ".zip")

		err = helper.UnpackZip(filepath.Join(inpDir, modelName+".zip"), outDir)
		if err != nil {
			return err
		}
		inpDir = filepath.Join(outDir, modelName)
	}

	// insert model metadata from json file into database
	modelDef, err := fromModelJsonToDb(dstDb, dbFacet, inpDir, modelName)
	if err != nil {
		return err
	}

	// insert languages and model text metadata from json file into database
	langDef, err := fromLangTextJsonToDb(dstDb, modelDef, inpDir)
	if err != nil {
		return err
	}

	// insert model runs data from csv into database:
	// parameters, output expressions and accumulators
	runIdMap, err := fromCsvRunToDb(dstDb, modelDef, langDef, inpDir)
	if err != nil {
		return err
	}

	// insert model workset data from csv into database: input parameters
	setIdMap, err := fromCsvWorksetToDb(dstDb, modelDef, langDef, inpDir, runIdMap)
	if err != nil {
		return err
	}

	// insert modeling tasks and tasks run history from json file into database
	if err = fromTaskJsonToDb(dstDb, modelDef, langDef, inpDir, runIdMap, setIdMap); err != nil {
		return err
	}
	return nil
}

// copy model from source database to destination database
func dbToDb(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// validate source and destination
	inpConnStr := runOpts.String(config.DbConnectionStr)
	inpDriver := runOpts.String(config.DbDriverName)
	outConnStr := runOpts.String(toDbConnectionStr)
	outDriver := runOpts.String(toDbDriverName)

	if inpConnStr == outConnStr && inpDriver == outDriver {
		return errors.New("source same as destination: cannot overwrite model in database")
	}

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, inpConnStr, inpDriver)
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	nv, err := db.OpenmppSchemaVersion(srcDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid source database, likely not an openM++ database")
	}

	// open destination database and check is it valid
	cs, dn = db.IfEmptyMakeDefault(modelName, outConnStr, outDriver)
	dstDb, dbFacet, err := db.Open(cs, dn, true)
	if err != nil {
		return err
	}
	defer dstDb.Close()

	nv, err = db.OpenmppSchemaVersion(dstDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid destination database, likely not an openM++ database")
	}

	// get source model metadata and languages, make a deep copy to use for destination database writing
	err = copyDbToDb(srcDb, dstDb, dbFacet, modelName, modelDigest)
	if err != nil {
		return err
	}
	return nil
}
