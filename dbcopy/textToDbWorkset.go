// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// copy workset from text json and csv files into database
func textToDbWorkset(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// validate parameters
	if modelName == "" {
		return errors.New("invalid (empty) model name")
	}

	// get workset name and id
	setName := runOpts.String(setNameArgKey)
	setId := runOpts.Int(setIdArgKey, 0)

	if setId < 0 || setId == 0 && setName == "" {
		return errors.New("dbcopy invalid argument(s) for set id: " + runOpts.String(setIdArgKey) + " and/or set name: " + runOpts.String(setNameArgKey))
	}

	// root for workset data: input directory or name of input.zip
	// it is parameter directory (if specified) or input directory/modelName.set.id
	// for csv files this "root" combined with sub-directory: root/set.id.setName or root/set.setName
	inpDir := ""
	if runOpts.IsExist(paramDirArgKey) {
		inpDir = filepath.Clean(runOpts.String(paramDirArgKey))
	} else {
		if setId > 0 {
			inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".set."+strconv.Itoa(setId))
		} else {
			inpDir = filepath.Join(runOpts.String(inputDirArgKey), modelName+".set."+setName)
		}
	}

	// unzip if required and use unzipped directory as "root" input diretory
	if runOpts.Bool(zipArgKey) {
		base := filepath.Base(inpDir)
		omppLog.Log("Unpack ", base, ".zip")

		outDir := runOpts.String(outputDirArgKey)
		if outDir == "" {
			outDir = filepath.Dir(inpDir)
		}
		if err := helper.UnpackZip(inpDir+".zip", outDir); err != nil {
			return err
		}
		inpDir = filepath.Join(outDir, base)
	}

	// get workset metadata json path and csv directory by set id or set name or both
	var metaPath string
	var csvDir string

	if runOpts.IsExist(setNameArgKey) && runOpts.IsExist(setIdArgKey) { // both: set id and name

		metaPath = filepath.Join(inpDir,
			modelName+".set."+strconv.Itoa(setId)+"."+helper.ToAlphaNumeric(setName)+".json")

		if _, err := os.Stat(metaPath); err != nil { // clear path to indicate metadata json file does not exist
			metaPath = ""
		}

		csvDir = filepath.Join(inpDir,
			"set."+strconv.Itoa(setId)+"."+helper.ToAlphaNumeric(setName))

		if _, err := os.Stat(csvDir); err != nil { // clear path to indicate csv directory does not exist
			csvDir = ""
		}

	} else { // set id or set name only

		// make path search patterns for metadata json and csv directory
		var cp string
		if runOpts.IsExist(setNameArgKey) && !runOpts.IsExist(setIdArgKey) { // set name only
			cp = "set.*" + helper.ToAlphaNumeric(setName)
		}
		if !runOpts.IsExist(setNameArgKey) && runOpts.IsExist(setIdArgKey) { // set id only
			cp = "set." + strconv.Itoa(setId) + ".*"
		}
		mp := modelName + "." + cp + ".json"

		// find path to metadata json by pattern
		fl, err := filepath.Glob(inpDir + "/" + mp)
		if err != nil {
			return err
		}
		if len(fl) >= 1 {
			metaPath = fl[0]
			if len(fl) > 1 {
				omppLog.Log("found multiple workset metadata json files, using: " + filepath.Base(metaPath))
			}
		}

		// csv directory:
		// if metadata json file exist then check if csv directory for that json file
		if metaPath != "" {

			d, f := filepath.Split(metaPath)
			c := strings.TrimSuffix(strings.TrimPrefix(f, modelName+"."), ".json")

			if len(c) <= 4 { // expected csv directory: set.4.w or set.w
				csvDir = ""
			} else {
				csvDir = filepath.Join(d, c)
				if _, err := os.Stat(csvDir); err != nil {
					csvDir = ""
				}
			}

		} else { // metadata json file not exist: search for csv directory by pattern

			fl, err := filepath.Glob(inpDir + "/" + cp)
			if err != nil {
				return err
			}
			if len(fl) >= 1 {
				csvDir = fl[0]
				if len(fl) > 1 {
					omppLog.Log("found multiple workset csv directories, using: " + filepath.Base(csvDir))
				}
			}
		}
	}

	// check results: metadata json file or csv directory must exist
	if metaPath == "" && csvDir == "" {
		return errors.New("no workset metadata json file and no csv directory, workset: " + strconv.Itoa(setId) + " " + setName)
	}

	// get connection string and driver name
	cs := runOpts.String(toDbConnStrArgKey)

	dn := runOpts.String(toDbDriverArgKey)
	if dn == "" && runOpts.IsExist(dbDriverArgKey) {
		dn = runOpts.String(dbDriverArgKey)
	}

	cs, dn = db.IfEmptyMakeDefault(modelName, cs, dn)

	// open destination database and check is it valid
	dstDb, _, err := db.Open(cs, dn, true)
	if err != nil {
		return err
	}
	defer dstDb.Close()

	nv, err := db.OpenmppSchemaVersion(dstDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid database, likely not an openM++ database")
	}

	// get model metadata
	modelDef, err := db.GetModel(dstDb, modelName, modelDigest)
	if err != nil {
		return err
	}

	// get full list of languages
	langDef, err := db.GetLanguages(dstDb)
	if err != nil {
		return err
	}

	// read from metadata json and csv files and update target database
	encName := runOpts.String(encodingArgKey)

	dstId, err := fromWorksetTextToDb(dstDb, modelDef, langDef, setName, metaPath, csvDir, encName)
	if err != nil {
		return err
	}
	if dstId <= 0 {
		return errors.New("workset not found or empty: " + strconv.Itoa(setId) + " " + setName)
	}

	return nil
}

// fromWorksetTextListToDb read all worksets parameters from csv and json files,
// convert it to db cells and insert into database
// update set id's and base run id's with actual id in database
func fromWorksetTextListToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, inpDir string, encodingName string,
) error {

	// get list of workset json files
	fl, err := filepath.Glob(inpDir + "/" + modelDef.Model.Name + ".set.*.json")
	if err != nil {
		return err
	}
	if len(fl) <= 0 {
		return nil // no worksets
	}

	// for each file:
	// read workset metadata, update workset in target database
	// read csv files from workset csv subdir and update parameter values
	for k := range fl {

		// check if workset subdir exist
		d, f := filepath.Split(fl[k])
		csvDir := strings.TrimSuffix(strings.TrimPrefix(f, modelDef.Model.Name+"."), ".json")

		if len(csvDir) <= 4 { // expected csv directory: set.4.q or set.q
			csvDir = ""
		} else {
			csvDir = filepath.Join(d, csvDir)
			if _, err := os.Stat(csvDir); err != nil {
				csvDir = ""
			}
		}

		// update or insert workset metadata and parameters from csv if csv directory exist
		_, err := fromWorksetTextToDb(dbConn, modelDef, langDef, "", fl[k], csvDir, encodingName)
		if err != nil {
			return err
		}
	}

	return nil
}

// fromWorksetTextToDb read workset metadata from json file,
// read all parameters from csv files, convert it to db cells and insert into database
// update set id's and base run id's with actual id in destination database
// it return source workset id (set id from metadata json file) and destination set id
func fromWorksetTextToDb(
	dbConn *sql.DB, modelDef *db.ModelMeta, langDef *db.LangMeta, srcName string, metaPath string, csvDir string, encodingName string,
) (int, error) {

	// if no metadata file and no csv directory then exit: nothing to do
	if metaPath == "" && csvDir == "" {
		return 0, nil // no workset
	}

	// get workset metadata:
	// model name and set name must be specified as parameter or inside of metadata json
	var pub db.WorksetPub

	if metaPath == "" && csvDir != "" { // no metadata json file, only csv directory
		pub.Name = srcName
		pub.ModelName = modelDef.Model.Name
	}

	if metaPath != "" { // read metadata json file

		isExist, err := helper.FromJsonFile(metaPath, &pub)
		if err != nil {
			return 0, err
		}

		if !isExist { // metadata from json is empty

			if csvDir == "" { // if metadata json empty and no csv directory then exit: no data
				return 0, nil
			}
			// metadata empty but there is csv directory: use expected model name and set name
			pub.Name = srcName
			pub.ModelName = modelDef.Model.Name
		}
	}
	if pub.Name == "" {
		return 0, errors.New("workset name is empty and metadata json file not found or empty")
	}

	// if only csv directory specified:
	//   make list of parameters based on csv file names
	//   assume only one parameter sub-value in csv file
	if metaPath == "" && csvDir != "" {

		fl, err := filepath.Glob(csvDir + "/*.csv")
		if err != nil {
			return 0, err
		}
		pub.Param = make([]db.ParamRunSetPub, len(fl))

		for j := range fl {
			fn := filepath.Base(fl[j])
			fn = fn[:len(fn)-4] // remove .csv extension
			pub.Param[j].Name = fn
			pub.Param[j].SubCount = 1 // only one sub-value
		}
	}

	// save workset metadata as "read-write" and after importing all parameters set it as "readonly"
	// save workset metadata parameters list, make it empty and use add parameters to update metadata and values from csv
	isReadonly := pub.IsReadonly
	pub.IsReadonly = false
	paramLst := append([]db.ParamRunSetPub{}, pub.Param...)
	pub.Param = []db.ParamRunSetPub{}

	// destination: convert from "public" format into destination db rows
	// display warning if base run not found in destination database
	ws, err := pub.FromPublic(dbConn, modelDef)
	if err != nil {
		return 0, err
	}
	if ws.Set.BaseRunId <= 0 && pub.BaseRunDigest != "" {
		omppLog.Log("Warning: workset ", ws.Set.Name, ", base run not found by digest ", pub.BaseRunDigest)
	}

	// if destination workset exists then delete it to remove all parameter values
	wsRow, err := db.GetWorksetByName(dbConn, modelDef.Model.ModelId, pub.Name)
	if err != nil {
		return 0, err
	}
	if wsRow != nil {
		err = db.DeleteWorkset(dbConn, wsRow.SetId) // delete existing workset
		if err != nil {
			return 0, errors.New("failed to delete workset " + strconv.Itoa(wsRow.SetId) + " " + wsRow.Name + " " + err.Error())
		}
	}

	// create empty workset metadata or update existing workset metadata
	err = ws.UpdateWorkset(dbConn, modelDef, true, langDef)
	if err != nil {
		return 0, err
	}
	dstId := ws.Set.SetId // actual set id from destination database

	// read all workset parameters and copy into destination database
	omppLog.Log("Workset ", ws.Set.Name, " into id: ", dstId)

	// read all workset parameters from csv files
	for j := range paramLst {

		// read parameter values from csv file
		var cell db.CellParam
		cLst, err := fromCsvFile(csvDir, modelDef, paramLst[j].Name, paramLst[j].SubCount, &cell, encodingName)
		if err != nil {
			return 0, err
		}
		if cLst == nil || cLst.Len() <= 0 {
			return 0, errors.New("workset: " + ws.Set.Name + " parameter empty: " + paramLst[j].Name)
		}

		// insert or update parameter values in workset
		_, err = ws.UpdateWorksetParameter(dbConn, modelDef, true, &paramLst[j], cLst, langDef)
		if err != nil {
			return 0, err
		}
	}

	// update workset readonly status with actual value
	err = db.UpdateWorksetReadonly(dbConn, dstId, isReadonly)
	if err != nil {
		return 0, err
	}

	return dstId, nil
}
