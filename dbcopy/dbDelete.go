// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// delete model from database
func dbDeleteModel(modelName string, modelDigest string, runOpts *config.RunOptions) error {

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
	isFound, modelId, err := db.GetModelId(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New("model " + modelName + " " + modelDigest + " not found")
	}

	// delete model metadata and drop model tables
	err = db.DeleteModel(srcDb, modelId)
	if err != nil {
		return errors.New("model delete failed " + modelName + " " + modelDigest + " " + err.Error())
	}
	return nil
}

// delete model run data from database
func dbDeleteRun(modelName string, modelDigest string, runOpts *config.RunOptions) error {

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

	// find the model by name and/or digest
	isFound, modelId, err := db.GetModelId(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New("model " + modelName + " " + modelDigest + " not found")
	}

	//
	_ = modelId
	omppLog.Log("not implented")
	return nil
}

// delete workset data from database
func dbDeleteWorkset(modelName string, modelDigest string, runOpts *config.RunOptions) error {

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
	isFound, modelId, err := db.GetModelId(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New("model " + modelName + " " + modelDigest + " not found")
	}

	//
	_ = modelId
	omppLog.Log("not implented")
	return nil
}

// delete modeling task and task run history from database
func dbDeleteTask(modelName string, modelDigest string, runOpts *config.RunOptions) error {

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
	isFound, modelId, err := db.GetModelId(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	if !isFound {
		return errors.New("model " + modelName + " " + modelDigest + " not found")
	}

	//
	_ = modelId
	omppLog.Log("not implented")
	return nil
}
