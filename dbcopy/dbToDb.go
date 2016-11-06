// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
)

// copy model from source database to destination database
func dbToDb(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// validate source and destination
	inpConnStr := runOpts.String(dbConnStrArgKey)
	inpDriver := runOpts.String(dbDriverArgKey)
	outConnStr := runOpts.String(toDbConnStrArgKey)
	outDriver := runOpts.String(toDbDriverArgKey)

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
	err = copyDbToDb(srcDb, dstDb, dbFacet, modelName, modelDigest, runOpts.String(doubleFormatArgKey))
	if err != nil {
		return err
	}
	return nil
}

// copyDbToDb select from source database and insert or update existing
// model metadata in all languages, model runs, workset(s), modeling tasks and task run history.
//
// Model id's and hId's updated with destination database id's.
// For example, in source db model id can be 11 and in destination it will be 200,
// same for all other id's: type Hid, parameter Hid, table Hid, run id, set id, task id, etc.
func copyDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, dbFacet db.Facet, modelName string, modelDigest string, doubleFmt string) error {

	// source: get model metadata
	srcModel, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = srcModel.Model.Name // set model name: it can be empty and only model digest specified

	// source: get list of languages
	srcLang, err := db.GetLanguages(srcDb)
	if err != nil {
		return err
	}

	// source: get model text (description and notes) in all languages
	modelTxt, err := db.GetModelText(srcDb, srcModel.Model.ModelId, "")
	if err != nil {
		return err
	}

	// source: get model parameter and output table groups and group text (description and notes) in all languages
	modelGroup, err := db.GetModelGroup(srcDb, srcModel.Model.ModelId, "")
	if err != nil {
		return err
	}

	// source: get model profile: default model profile is profile where name = model name
	modelProfile, err := db.GetProfile(srcDb, modelName)
	if err != nil {
		return err
	}

	// deep copy of model metadata and languages is required
	// because during db writing metadata structs updated with destination database id's,
	// for example, in source db model id can be 11 and in destination it will be 200,
	// same for all other id's: type Hid, parameter Hid, table Hid, run id, set id, task id, etc.
	dstModel, err := srcModel.Clone()
	if err != nil {
		return err
	}
	dstLang, err := srcLang.Clone()
	if err != nil {
		return err
	}

	// destination: insert model metadata into destination database if not exists
	if err = db.UpdateModel(dstDb, dbFacet, dstModel); err != nil {
		return err
	}

	// destination: insert or update language list
	if err = db.UpdateLanguage(dstDb, dstLang); err != nil {
		return err
	}

	// destination: get full list of languages in destination database
	dstLang, err = db.GetLanguages(dstDb)
	if err != nil {
		return err
	}

	// destination: insert, update or delete model default profile
	if err = db.UpdateProfile(dstDb, modelProfile); err != nil {
		return err
	}

	// destination: insert or update model text data (description and notes)
	if err = db.UpdateModelText(dstDb, dstModel, dstLang, modelTxt); err != nil {
		return err
	}

	// destination: insert or update model groups and groups text (description, notes)
	if err = db.UpdateModelGroup(dstDb, dstModel, dstLang, modelGroup); err != nil {
		return err
	}

	// source to destination: copy model runs: parameters, output expressions and accumulators
	err = copyRunListDbToDb(srcDb, dstDb, srcModel, dstModel, dstLang, doubleFmt)
	if err != nil {
		return err
	}

	// source to destination: copy all readonly worksets parameters
	err = copyWorksetListDbToDb(srcDb, dstDb, srcModel, dstModel, dstLang)
	if err != nil {
		return err
	}

	// source to destination: copy all modeling tasks
	err = copyTaskListDbToDb(srcDb, dstDb, srcModel, dstModel, dstLang)
	if err != nil {
		return err
	}

	return nil
}
