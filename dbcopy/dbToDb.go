// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"strconv"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	omppLog "go.openmpp.org/ompp/log"
)

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
	err = copyDbToDb(srcDb, dstDb, dbFacet, modelName, modelDigest, runOpts.String(doubleFmtArgKey))
	if err != nil {
		return err
	}
	return nil
}

// copy workset from source database to destination database
func dbToDbWorkset(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// get workset name and id
	setName := runOpts.String(config.SetName)
	setId := runOpts.Int(config.SetId, 0)

	// conflicting options: use set id if positive else use set name
	if runOpts.IsExist(config.SetName) && runOpts.IsExist(config.SetId) {
		if setId > 0 {
			omppLog.Log("dbcopy options conflict. Using set id: ", setId, " ignore set name: ", setName)
			setName = ""
		} else {
			omppLog.Log("dbcopy options conflict. Using set name: ", setName, " ignore set id: ", setId)
			setId = 0
		}
	}

	if setId < 0 || setId == 0 && setName == "" {
		return errors.New("dbcopy invalid argument(s) for set id: " + runOpts.String(config.SetId) + " and/or set name: " + runOpts.String(config.SetName))
	}

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

	srcWs, err := db.GetWorksetFull(srcDb, srcModel, wsRow, "") // get full workset metadata
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

	// copy source workset metadata and parameters into destination database
	_, err = copyWorksetDbToDb(srcDb, dstDb, srcModel, dstModel, srcWs, dstLang)
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

	// source: get model parameter and output table groups (description and notes) in all languages
	modelGroup, err := db.GetModelGroup(srcDb, srcModel.Model.ModelId, "")
	if err != nil {
		return err
	}

	// source: get model profile: default model profile is profile where name = model name
	modelProfile, err := db.GetProfile(srcDb, modelName)
	if err != nil {
		return err
	}

	// source: get all modeling tasks and successfully completed tasks run history in all languages
	tl, err := db.GetTaskFullList(srcDb, srcModel.Model.ModelId, true, "")
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
	runIdMap, err := copyRunListDbToDb(srcDb, dstDb, srcModel, dstModel, dstLang, doubleFmt)
	if err != nil {
		return err
	}

	// source to destination: copy all readonly worksets parameters
	setIdMap, err := copyWorksetListDbToDb(srcDb, dstDb, srcModel, dstModel, dstLang)
	if err != nil {
		return err
	}

	// destination: insert or update modeling tasks and tasks run history
	for k := range tl {

		omppLog.Log("Modeling task ", tl[k].Task.TaskId)

		if err = db.UpdateTask(dstDb, dstModel, dstLang, &tl[k], runIdMap, setIdMap); err != nil {
			return err
		}
	}

	return nil
}

// copyRunListDbToDb do copy all model runs parameters and output tables from source to destination database
// Double format is used for float model types digest calculation, if non-empty format supplied
func copyRunListDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, dstLang *db.LangList, doubleFmt string) (map[int]int, error) {

	// source: get all successfully completed model runs in all languages
	srcRl, err := db.GetRunFullList(srcDb, srcModel, true, "")
	if err != nil {
		return nil, err
	}
	if len(srcRl) <= 0 {
		return make(map[int]int), nil // exit if no model runs
	}

	// copy all run metadata, run parameters, output accumulators and expressions from source to destination
	// update run id's with actual destination database values
	runIdMap := make(map[int]int, len(srcRl))

	for k := range srcRl {

		srcId := srcRl[k].Run.RunId

		dstId, err := copyRunDbToDb(srcDb, dstDb, srcModel, dstModel, &srcRl[k], dstLang, doubleFmt)
		if err != nil {
			return nil, err
		}
		runIdMap[srcId] = dstId
	}
	return runIdMap, nil
}

// copyRunDbToDb do copy model run metadata, run parameters and output tables from source to destination database
// it return destination run id (run id in destination database)
func copyRunDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, srcRun *db.RunMeta, dstLang *db.LangList, doubleFmt string) (int, error) {

	// validate parameters
	if srcRun == nil {
		return 0, errors.New("invalid (empty) source model run metadata, source run not found or not exists")
	}

	// destination: make a deep of run
	// deep copy is required because during db writing source run id's updated with destination database id's
	var dstRun db.RunMeta
	helper.DeepCopy(srcRun, &dstRun)

	// destination: save model run metadata
	// update incoming run id's with actual new run id created in database
	srcId := srcRun.Run.RunId

	isExist, err := db.UpdateRun(dstDb, dstModel, dstLang, &dstRun)
	if err != nil {
		return 0, err
	}
	dstId := dstRun.Run.RunId
	if isExist { // exit if model run already exist
		omppLog.Log("Model run ", srcId, " already exists as ", dstId)
		return dstId, nil
	}

	// copy all run parameters, output accumulators and expressions from source to destination
	omppLog.Log("Model run from ", srcId, " to ", dstId)

	srcLt := db.ReadLayout{FromId: srcId}
	dstLt := db.WriteLayout{ToId: dstId, IsToRun: true}

	// copy all parameters values for that modrel run
	for j := range srcModel.Param {

		// source: read parameter values
		srcLt.Name = srcModel.Param[j].Name

		cLst, err := db.ReadParameter(srcDb, srcModel, &srcLt)
		if err != nil {
			return 0, err
		}
		if cLst.Len() <= 0 { // parameter data must exist for all parameters
			return 0, errors.New("missing run parameter values " + srcLt.Name + " run id: " + strconv.Itoa(srcLt.FromId))
		}

		// destination: insert parameter values in model run
		dstLt.Name = dstModel.Param[j].Name

		if err = db.WriteParameter(dstDb, dstModel, &dstLt, cLst, doubleFmt); err != nil {
			return 0, err
		}
	}

	// copy all output tables values for that modrel run
	for j := range srcModel.Table {

		// source: read output table accumulator
		srcLt.Name = srcModel.Table[j].Name
		srcLt.IsAccum = true

		acLst, err := db.ReadOutputTable(srcDb, srcModel, &srcLt)
		if err != nil {
			return 0, err
		}

		// source: read output table expression values
		srcLt.IsAccum = false

		ecLst, err := db.ReadOutputTable(srcDb, srcModel, &srcLt)
		if err != nil {
			return 0, err
		}

		// insert output table values (accumulators and expressions) in model run
		dstLt.Name = dstModel.Table[j].Name
		if err = db.WriteOutputTable(dstDb, dstModel, &dstLt, acLst, ecLst, doubleFmt); err != nil {
			return 0, err
		}
	}

	return dstId, nil
}

// copyWorksetListDbToDb do copy all readonly worksets parameters from source to destination database
func copyWorksetListDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, dstLang *db.LangList) (map[int]int, error) {

	// source: get all readonly worksets in all languages
	srcWl, err := db.GetWorksetFullList(srcDb, srcModel, true, "")
	if err != nil {
		return nil, err
	}
	if len(srcWl) <= 0 {
		return make(map[int]int), nil // no worksets
	}

	// copy worksets from source to destination database
	// update set id's and base run id's with actual destination database values
	setIdMap := make(map[int]int, len(srcWl))

	for k := range srcWl {

		srcId := srcWl[k].Set.SetId

		dstId, err := copyWorksetDbToDb(srcDb, dstDb, srcModel, dstModel, &srcWl[k], dstLang)
		if err != nil {
			return nil, err
		}
		setIdMap[srcId] = dstId
	}
	return setIdMap, nil
}

// copyWorksetDbToDb do copy workset metadata and parameters from source to destination database
// it return destination set id (set id in destination database)
func copyWorksetDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, srcWs *db.WorksetMeta, dstLang *db.LangList) (int, error) {

	// validate parameters
	if srcWs == nil {
		return 0, errors.New("invalid (empty) source workset metadata, source workset not found or not exists")
	}

	// destination: make a deep of source workset
	// deep copy is required because during db writing source set id's and base run id's updated with destination database id's
	var dstWs db.WorksetMeta
	helper.DeepCopy(srcWs, &dstWs)

	// destination: save model workset metadata
	// update incoming set id with actual new set id created in database
	// update incoming base run id with actual run id in database
	srcId := srcWs.Set.SetId

	// update incoming base run id with actual run id in database
	// if run digest empty then run id must be zero (treated as NULL) else find base run id by digest
	if dstWs.Set.BaseRunDigest == "" {

		dstWs.Set.BaseRunId = 0 // no run digest: no base run, set base run id as NULL

	} else { // find base run id by digest

		runRow, err := db.GetRunByDigest(srcDb, dstWs.Set.BaseRunDigest)
		if err != nil {
			return 0, err
		}
		if runRow != nil {
			dstWs.Set.BaseRunId = runRow.RunId
		} else {
			// run not found in target database: set base run id as NULL
			dstWs.Set.BaseRunId = 0
			omppLog.Log("warning: workset ", srcId, " ", dstWs.Set.Name, ", base run not found by digest ", dstWs.Set.BaseRunDigest)
		}
	}

	err := db.UpdateWorkset(dstDb, dstModel, dstLang, &dstWs)
	if err != nil {
		return 0, err
	}
	dstId := dstWs.Set.SetId

	// read all workset parameters and copy into destination database
	omppLog.Log("Workset from ", srcId, " to ", dstId)

	srcLt := &db.ReadLayout{FromId: srcId, IsFromSet: true}
	dstLt := db.WriteLayout{ToId: dstId}

	// write parameter into destination database
	for j := range srcWs.Param {

		// source: read workset parameter values
		srcLt.Name = srcWs.Param[j].Name

		cLst, err := db.ReadParameter(srcDb, srcModel, srcLt)
		if err != nil {
			return 0, err
		}
		if cLst.Len() <= 0 { // parameter data must exist for all parameters
			return 0, errors.New("missing workset parameter values " + srcLt.Name + " set id: " + strconv.Itoa(srcLt.FromId))
		}

		// destination: insert or update parameter values in workset
		dstLt.Name = dstWs.Param[j].Name

		err = db.WriteParameter(dstDb, dstModel, &dstLt, cLst, "")
		if err != nil {
			return 0, err
		}
	}

	return dstId, nil
}
