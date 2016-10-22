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
	err = copyDbToDb(srcDb, dstDb, dbFacet, modelName, modelDigest, runOpts.String(config.DoubleFormat))
	if err != nil {
		return err
	}
	return nil
}

// copy model run from source database to destination database
func dbToDbRun(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// get model run name and id
	runName := runOpts.String(config.RunName)
	runId := runOpts.Int(config.RunId, 0)

	// conflicting options: use run id if positive else use run name
	if runOpts.IsExist(config.RunName) && runOpts.IsExist(config.RunId) {
		if runId > 0 {
			omppLog.Log("dbcopy options conflict. Using run id: ", runId, " ignore model run name: ", runName)
			runName = ""
		} else {
			omppLog.Log("dbcopy options conflict. Using model run name: ", runName, " ignore run id: ", runId)
			runId = 0
		}
	}

	if runId < 0 || runId == 0 && runName == "" {
		return errors.New("dbcopy invalid argument(s) for model run id: " + runOpts.String(config.RunId) + " and/or name: " + runOpts.String(config.RunName))
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

	// get model run metadata by id or name
	var runRow *db.RunRow
	if runId > 0 {
		if runRow, err = db.GetRun(srcDb, runId); err != nil {
			return err
		}
		if runRow == nil {
			return errors.New("model run not found, id: " + strconv.Itoa(runId))
		}
	} else {
		if runRow, err = db.GetRunByName(srcDb, srcModel.Model.ModelId, runName); err != nil {
			return err
		}
		if runRow == nil {
			return errors.New("model run not found: " + runName)
		}
	}

	// run must be completed: status success, error or exit
	if runRow.Status != db.DoneRunStatus && runRow.Status != db.ExitRunStatus && runRow.Status != db.ErrorRunStatus {
		return errors.New("model run not completed: " + strconv.Itoa(runRow.RunId) + " " + runRow.Name)
	}

	// get full model run metadata
	meta, err := db.GetRunFull(srcDb, runRow, "")
	if err != nil {
		return err
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

	// convert model run db rows into "public"" format
	// and copy source model run metadata, parameter values, output results into destination database
	pub, err := meta.ToPublic(srcDb, srcModel)
	if err != nil {
		return err
	}
	dblFmt := runOpts.String(config.DoubleFormat)

	_, err = copyRunDbToDb(srcDb, dstDb, srcModel, dstModel, meta.Run.RunId, pub, dstLang, dblFmt)
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

	// convert workset db rows into "public"" format
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

// copy modeling task metadata and run history from source database to destination database
func dbToDbTask(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// get task name and id
	taskName := runOpts.String(config.TaskName)
	taskId := runOpts.Int(config.TaskId, 0)

	// conflicting options: use task id if positive else use task name
	if runOpts.IsExist(config.TaskName) && runOpts.IsExist(config.TaskId) {
		if taskId > 0 {
			omppLog.Log("dbcopy options conflict. Using task id: ", taskId, " ignore task name: ", taskName)
			taskName = ""
		} else {
			omppLog.Log("dbcopy options conflict. Using task name: ", taskName, " ignore task id: ", taskId)
			taskId = 0
		}
	}

	if taskId < 0 || taskId == 0 && taskName == "" {
		return errors.New("dbcopy invalid argument(s) for task id: " + runOpts.String(config.TaskId) + " and/or task name: " + runOpts.String(config.TaskName))
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

	// get task metadata by id or name
	var taskRow *db.TaskRow
	if taskId > 0 {
		if taskRow, err = db.GetTask(srcDb, taskId); err != nil {
			return err
		}
		if taskRow == nil {
			return errors.New("modeling task not found, task id: " + strconv.Itoa(taskId))
		}
	} else {
		if taskRow, err = db.GetTaskByName(srcDb, srcModel.Model.ModelId, taskName); err != nil {
			return err
		}
		if taskRow == nil {
			return errors.New("modeling task not found: " + taskName)
		}
	}

	meta, err := db.GetTaskFull(srcDb, taskRow, "") // get task full metadata, including task run history
	if err != nil {
		return err
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

	// convert task db rows into "public"" format
	// and copy source task metadata into destination database
	pub, err := meta.ToPublic(srcDb, srcModel)
	if err != nil {
		return err
	}
	_, err = copyTaskDbToDb(srcDb, dstDb, srcModel, dstModel, meta.Task.TaskId, pub, dstLang)
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

// copyRunListDbToDb do copy all model runs parameters and output tables from source to destination database
// Double format is used for float model types digest calculation, if non-empty format supplied
func copyRunListDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, dstLang *db.LangMeta, doubleFmt string) error {

	// source: get all successfully completed model runs in all languages
	srcRl, err := db.GetRunFullList(srcDb, srcModel.Model.ModelId, true, "")
	if err != nil {
		return err
	}
	if len(srcRl) <= 0 {
		return nil
	}

	// copy all run metadata, run parameters, output accumulators and expressions from source to destination
	// model run "public" format is used
	for k := range srcRl {

		// convert model db rows into "public"" format
		pub, err := srcRl[k].ToPublic(srcDb, srcModel)
		if err != nil {
			return err
		}

		// save into destination database
		_, err = copyRunDbToDb(srcDb, dstDb, srcModel, dstModel, srcRl[k].Run.RunId, pub, dstLang, doubleFmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// copyRunDbToDb do copy model run metadata, run parameters and output tables from source to destination database
// it return destination run id (run id in destination database)
func copyRunDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, srcId int, pub *db.RunPub, dstLang *db.LangMeta, doubleFmt string) (int, error) {

	// validate parameters
	if pub == nil {
		return 0, errors.New("invalid (empty) source model run metadata, source run not found or not exists")
	}

	// destination: convert from "public" format into destination db rows
	dstRun, err := pub.FromPublic(dstDb, dstModel, dstLang)
	if err != nil {
		return 0, err
	}

	// destination: save model run metadata
	isExist, err := dstRun.UpdateRun(dstDb, dstModel)
	if err != nil {
		return 0, err
	}
	dstId := dstRun.Run.RunId
	if isExist { // exit if model run already exist
		omppLog.Log("Model run ", srcId, " ", pub.Name, " already exists as ", dstId)
		return dstId, nil
	}

	// copy all run parameters, output accumulators and expressions from source to destination
	omppLog.Log("Model run from ", srcId, " ", pub.Name, " to ", dstId)

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

	// destination: convert from "public" format into destination db rows
	// display warning if base run not found in destination database
	dstWs, err := pub.FromPublic(dstDb, dstModel, dstLang)
	if err != nil {
		return 0, err
	}
	if dstWs.Set.BaseRunId <= 0 && pub.BaseRunDigest != "" {
		omppLog.Log("Warning: workset ", dstWs.Set.Name, ", base run not found by digest ", pub.BaseRunDigest)
	}

	// save workset metadata as "read-write" and after importing all parameters set it as "readonly"
	isReadonly := pub.IsReadonly
	dstWs.Set.IsReadonly = false

	err = dstWs.UpdateWorkset(dstDb, dstModel)
	if err != nil {
		return 0, err
	}
	dstId := dstWs.Set.SetId // actual set id from destination database

	// read all workset parameters and copy into destination database
	omppLog.Log("Workset ", dstWs.Set.Name, " from id ", srcId, " to ", dstId)

	srcLt := &db.ReadLayout{FromId: srcId, IsFromSet: true}
	dstLt := db.WriteLayout{ToId: dstId}

	// write parameter into destination database
	for j := range pub.Param {

		// source: read workset parameter values
		srcLt.Name = pub.Param[j].Name

		cLst, err := db.ReadParameter(srcDb, srcModel, srcLt)
		if err != nil {
			return 0, err
		}
		if cLst.Len() <= 0 { // parameter data must exist for all parameters
			return 0, errors.New("missing workset parameter values " + srcLt.Name + " set id: " + strconv.Itoa(srcLt.FromId))
		}

		// destination: insert or update parameter values in workset
		dstLt.Name = pub.Param[j].Name

		err = db.WriteParameter(dstDb, dstModel, &dstLt, cLst, "")
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

// copyTaskListDbToDb do copy all modeling tasks from source to destination database
func copyTaskListDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, dstLang *db.LangMeta) error {

	// source: get all modeling tasks metadata in all languages
	srcTl, err := db.GetTaskFullList(srcDb, srcModel.Model.ModelId, true, "")
	if err != nil {
		return err
	}
	if len(srcTl) <= 0 {
		return nil
	}

	// copy task metadata from source to destination database by using "public" format
	for k := range srcTl {

		// convert task metadata db rows into "public"" format
		pub, err := srcTl[k].ToPublic(srcDb, srcModel)
		if err != nil {
			return err
		}

		// save into destination database
		_, err = copyTaskDbToDb(srcDb, dstDb, srcModel, dstModel, srcTl[k].Task.TaskId, pub, dstLang)
		if err != nil {
			return err
		}
	}
	return nil
}

// copyTaskDbToDb do copy modeling task metadata and task run history from source to destination database
// it return destination task id (task id in destination database)
func copyTaskDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, srcId int, pub *db.TaskPub, dstLang *db.LangMeta) (int, error) {

	// validate parameters
	if pub == nil {
		return 0, errors.New("invalid (empty) source modeling task metadata, source task not found or not exists")
	}

	// destination: convert from "public" format into destination db rows
	dstTask, isSetNotFound, isTaskRunNotFound, err := pub.FromPublic(dstDb, dstModel, dstLang)
	if err != nil {
		return 0, err
	}
	if isSetNotFound {
		omppLog.Log("Warning: task ", dstTask.Task.Name, " worksets not found, copy of task incomplete")
	}
	if isTaskRunNotFound {
		omppLog.Log("Warning: task ", dstTask.Task.Name, " worksets or model runs not found, copy of task run history incomplete")
	}

	// destination: save modeling task metadata
	err = dstTask.UpdateTask(dstDb, dstModel)
	if err != nil {
		return 0, err
	}
	dstId := dstTask.Task.TaskId
	omppLog.Log("Modeling task from ", srcId, " ", pub.Name, " to ", dstId)

	return dstId, nil
}
