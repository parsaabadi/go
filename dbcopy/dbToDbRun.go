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

	// convert model run db rows into "public" format
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
