// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"strconv"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// copy workset from source database to destination database
func dbToDbWorkset(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// get workset name and id
	setName := runOpts.String(setNameArgKey)
	setId := runOpts.Int(setIdArgKey, 0)

	// conflicting options: use set id if positive else use set name
	if runOpts.IsExist(setNameArgKey) && runOpts.IsExist(setIdArgKey) {
		if setId > 0 {
			omppLog.Log("dbcopy options conflict. Using set id: ", setId, " ignore set name: ", setName)
			setName = ""
		} else {
			omppLog.Log("dbcopy options conflict. Using set name: ", setName, " ignore set id: ", setId)
			setId = 0
		}
	}

	if setId < 0 || setId == 0 && setName == "" {
		return errors.New("dbcopy invalid argument(s) for set id: " + runOpts.String(setIdArgKey) + " and/or set name: " + runOpts.String(setNameArgKey))
	}

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
	dstDb, _, err := db.Open(cs, dn, true)
	if err != nil {
		return err
	}
	defer dstDb.Close()

	nv, err = db.OpenmppSchemaVersion(dstDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid destination database, likely not an openM++ database")
	}

	// source: get model metadata
	srcModel, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = srcModel.Model.Name // set model name: it can be empty and only model digest specified

	// get workset metadata by id or name
	var wsRow *db.WorksetRow
	if setId > 0 {
		if wsRow, err = db.GetWorkset(srcDb, setId); err != nil {
			return err
		}
		if wsRow == nil {
			return errors.New("workset not found, set id: " + strconv.Itoa(setId))
		}
	} else {
		if wsRow, err = db.GetWorksetByName(srcDb, srcModel.Model.ModelId, setName); err != nil {
			return err
		}
		if wsRow == nil {
			return errors.New("workset not found: " + setName)
		}
	}

	srcWs, err := db.GetWorksetFull(srcDb, wsRow, "") // get full workset metadata
	if err != nil {
		return err
	}

	// check: workset must be readonly
	if !srcWs.Set.IsReadonly {
		return errors.New("workset must be readonly: " + strconv.Itoa(wsRow.SetId) + " " + wsRow.Name)
	}

	// destination: get model metadata
	dstModel, err := db.GetModel(dstDb, modelName, modelDigest)
	if err != nil {
		return err
	}

	// destination: get list of languages
	dstLang, err := db.GetLanguages(dstDb)
	if err != nil {
		return err
	}

	// convert workset db rows into "public" format
	// and copy source workset metadata and parameters into destination database
	pub, err := srcWs.ToPublic(srcDb, srcModel)
	if err != nil {
		return err
	}
	_, err = copyWorksetDbToDb(srcDb, dstDb, srcModel, dstModel, srcWs.Set.SetId, pub, dstLang)
	if err != nil {
		return err
	}
	return nil
}

// copyWorksetListDbToDb do copy all readonly worksets parameters from source to destination database
func copyWorksetListDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, dstLang *db.LangMeta) error {

	// source: get all readonly worksets in all languages
	srcWl, err := db.GetWorksetFullList(srcDb, srcModel.Model.ModelId, true, "")
	if err != nil {
		return err
	}
	if len(srcWl) <= 0 {
		return nil
	}

	// copy worksets from source to destination database by using "public" format
	for k := range srcWl {

		// convert workset db rows into "public"" format
		pub, err := srcWl[k].ToPublic(srcDb, srcModel)
		if err != nil {
			return err
		}

		// save into destination database
		_, err = copyWorksetDbToDb(srcDb, dstDb, srcModel, dstModel, srcWl[k].Set.SetId, pub, dstLang)
		if err != nil {
			return err
		}
	}
	return nil
}

// copyWorksetDbToDb do copy workset metadata and parameters from source to destination database
// it return destination set id (set id in destination database)
func copyWorksetDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, srcId int, pub *db.WorksetPub, dstLang *db.LangMeta) (int, error) {

	// validate parameters
	if pub == nil {
		return 0, errors.New("invalid (empty) source workset metadata, source workset not found or not exists")
	}

	// save workset metadata as "read-write" and after importing all parameters set it as "readonly"
	// save workset metadata parameters list, make it empty and use add parameters to update metadata and values from csv
	isReadonly := pub.IsReadonly
	pub.IsReadonly = false
	paramLst := append([]db.ParamRunSetPub{}, pub.Param...)
	pub.Param = []db.ParamRunSetPub{}

	// destination: convert from "public" format into destination db rows
	// display warning if base run not found in destination database
	dstWs, err := pub.FromPublic(dstDb, dstModel)
	if err != nil {
		return 0, err
	}
	if dstWs.Set.BaseRunId <= 0 && pub.BaseRunDigest != "" {
		omppLog.Log("Warning: workset ", dstWs.Set.Name, ", base run not found by digest ", pub.BaseRunDigest)
	}

	// if destination workset exists then delete it to remove all parameter values
	wsRow, err := db.GetWorksetByName(dstDb, dstModel.Model.ModelId, pub.Name)
	if err != nil {
		return 0, err
	}
	if wsRow != nil {
		err = db.DeleteWorkset(dstDb, wsRow.SetId) // delete existing workset
		if err != nil {
			return 0, errors.New("failed to delete workset " + strconv.Itoa(wsRow.SetId) + " " + wsRow.Name + " " + err.Error())
		}
	}

	// create empty workset metadata or update existing workset metadata
	err = dstWs.UpdateWorkset(dstDb, dstModel, true, dstLang)
	if err != nil {
		return 0, err
	}
	dstId := dstWs.Set.SetId // actual set id from destination database

	// read all workset parameters and copy into destination database
	omppLog.Log("Workset ", dstWs.Set.Name, " from id ", srcId, " to ", dstId)

	paramLt := &db.ReadParamLayout{ReadLayout: db.ReadLayout{FromId: srcId}, IsFromSet: true}

	// write parameter into destination database
	for j := range paramLst {

		// source: read workset parameter values
		paramLt.Name = paramLst[j].Name

		cLst, err := db.ReadParameter(srcDb, srcModel, paramLt)
		if err != nil {
			return 0, err
		}
		if cLst.Len() <= 0 { // parameter data must exist for all parameters
			return 0, errors.New("missing workset parameter values " + paramLt.Name + " set id: " + strconv.Itoa(paramLt.FromId))
		}

		// destination: insert or update parameter values in workset
		_, err = dstWs.UpdateWorksetParameter(dstDb, dstModel, true, &paramLst[j], cLst, dstLang)
		if err != nil {
			return 0, err
		}
	}

	// update workset readonly status with actual value
	err = db.UpdateWorksetReadonly(dstDb, dstId, isReadonly)
	if err != nil {
		return 0, err
	}

	return dstId, nil
}
