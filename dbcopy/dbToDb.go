// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"strconv"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	omppLog "go.openmpp.org/ompp/log"
)

// copyDbToDb select from source database and insert or update existing
// model metadata in all languages, model runs, workset(s), modeling tasks and task run history.
//
// Model id's and hId's updated with destination database id's.
// For example, in source db model id can be 11 and in destination it will be 200,
// same for all other id's: type Hid, parameter Hid, table Hid, run id, set id, task id, etc.
func copyDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, dbFacet db.Facet, modelName string, modelDigest string) error {

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
	tl, err := db.GetTaskList(srcDb, srcModel.Model.ModelId, true, "")
	if err != nil {
		return err
	}

	// deep copy of model metadata and languages is required
	// because during db writing metadata structs updated with destination database id's,
	// for example, in source db model id can be 11 and in destination it will be 200,
	// same for all other id's: type Hid, parameter Hid, table Hid, run id, set id, task id, etc.

	// destination: make a deep copy of model metadata
	var dstModel db.ModelMeta
	helper.DeepCopy(srcModel, &dstModel)
	if err = dstModel.Setup(); err != nil {
		return err
	}

	// destination: insert model metadata into destination database if not exists
	if err = db.UpdateModel(dstDb, dbFacet, &dstModel); err != nil {
		return err
	}

	// destination: make a deep of languages
	dstLang := &db.LangList{}
	helper.DeepCopy(srcLang, dstLang)
	dstLang.Setup()

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
	if err = db.UpdateModelText(dstDb, &dstModel, dstLang, modelTxt); err != nil {
		return err
	}

	// destination: insert or update model groups and groups text (description, notes)
	if err = db.UpdateModelGroup(dstDb, &dstModel, dstLang, modelGroup); err != nil {
		return err
	}

	// source to destination: copy model runs: parameters, output expressions and accumulators
	runIdMap, err := copyRunDbToDb(srcDb, dstDb, srcModel, &dstModel, dstLang)
	if err != nil {
		return err
	}

	// source to destination: copy all readonly worksets parameters
	setIdMap, err := copyWorksetDbToDb(srcDb, dstDb, srcModel, &dstModel, dstLang, runIdMap)
	if err != nil {
		return err
	}

	// destination: insert or update modeling tasks and tasks run history
	if len(tl.Lst) > 0 {
		omppLog.Log("Modeling tasks: ", len(tl.Lst))

		if err = db.UpdateTaskList(dstDb, &dstModel, dstLang, tl, runIdMap, setIdMap); err != nil {
			return err
		}
	}

	return nil
}

// copyRunDbToDb do copy model runs parameters and output tables from source to destination database
func copyRunDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, dstLang *db.LangList) (map[int]int, error) {

	// source: get all successfully completed model runs in all languages
	srcRl, err := db.GetRunList(srcDb, srcModel, true, "")
	if err != nil {
		return nil, err
	}
	if len(srcRl.Lst) <= 0 {
		return make(map[int]int), nil // exit if no model runs
	}

	// destination: make a deep of run list
	var dstRl db.RunList
	helper.DeepCopy(srcRl, &dstRl)

	// destination: save model run list
	// update incoming run id's with actual new run id created in database
	runIdMap, err := db.UpdateRunList(dstDb, dstModel, dstLang, &dstRl)
	if err != nil {
		return nil, err
	}

	// copy all run parameters, output accumulators and expressions from source to destination
	for k := range srcRl.Lst {

		// source and destination run id's
		srcId := srcRl.Lst[k].Run.RunId
		dstId := runIdMap[srcId]
		omppLog.Log("Model run from ", srcId, " to ", dstId)

		srcLt := &db.ReadLayout{FromId: srcId}
		dstLt := db.WriteLayout{ToId: dstId, IsToRun: true}

		// copy all parameters values for that modrel run
		for j := range srcModel.Param {

			// source: read parameter values
			srcLt.Name = srcModel.Param[j].Name

			cLst, err := db.ReadParameter(srcDb, srcModel, srcLt)
			if err != nil {
				return nil, err
			}
			if cLst.Len() <= 0 { // parameter data must exist for all parameters
				return nil, errors.New("missing run parameter values " + srcLt.Name + " run id: " + strconv.Itoa(srcLt.FromId))
			}

			// destination: insert parameter values in model run
			dstLt.Name = dstModel.Param[j].Name

			if err = db.WriteParameter(dstDb, dstModel, &dstLt, cLst); err != nil {
				return nil, err
			}

		}

		// copy all output tables values for that modrel run
		for j := range srcModel.Table {

			// source: read output table accumulator
			srcLt.Name = srcModel.Table[j].Name
			srcLt.IsAccum = true

			cLst, err := db.ReadOutputTable(srcDb, srcModel, srcLt)
			if err != nil {
				return nil, err
			}

			// destination: insert accumulator(s) values in model run
			dstLt.Name = dstModel.Table[j].Name
			dstLt.IsAccum = true

			if err = db.WriteOutputTable(dstDb, dstModel, &dstLt, cLst); err != nil {
				return nil, err
			}

			// source: read output table expression values
			srcLt.IsAccum = false

			cLst, err = db.ReadOutputTable(srcDb, srcModel, srcLt)
			if err != nil {
				return nil, err
			}

			// destination: insert expression(s) values in model run
			dstLt.IsAccum = false

			if err = db.WriteOutputTable(dstDb, dstModel, &dstLt, cLst); err != nil {
				return nil, err
			}
		}
	}

	return runIdMap, nil
}

// copyWorksetDbToDb do copy all readonly worksets parameters from source to destination database
func copyWorksetDbToDb(
	srcDb *sql.DB, dstDb *sql.DB, srcModel *db.ModelMeta, dstModel *db.ModelMeta, dstLang *db.LangList, runIdMap map[int]int) (map[int]int, error) {

	// source: get all readonly worksets in all languages
	srcWl, err := db.GetWorksetList(srcDb, srcModel, true, "")
	if err != nil {
		return nil, err
	}
	if len(srcWl.Lst) <= 0 {
		return make(map[int]int), nil // no worksets
	}

	// destination: make a deep of run list
	var dstWl db.WorksetList
	helper.DeepCopy(srcWl, &dstWl)

	// destination: save model set list
	// update incoming set id's with actual new set id created in database
	// update incoming base run id's with actual run id in database
	setIdMap, err := db.UpdateWorksetList(dstDb, dstModel, dstLang, runIdMap, &dstWl)
	if err != nil {
		return nil, err
	}

	// read all workset parameters and dump it into csv files
	for k := range srcWl.Lst {

		// source and destination workset id's
		srcId := srcWl.Lst[k].Set.SetId
		dstId := setIdMap[srcId]
		omppLog.Log("Workset from ", srcId, " to ", dstId)

		srcLt := &db.ReadLayout{FromId: srcId, IsFromSet: true}
		dstLt := db.WriteLayout{ToId: dstId}

		// write parameter into csv file
		for j := range srcWl.Lst[k].Param {

			// source: read workset parameter values
			srcLt.Name = srcWl.Lst[k].Param[j].Name

			cLst, err := db.ReadParameter(srcDb, srcModel, srcLt)
			if err != nil {
				return nil, err
			}
			if cLst.Len() <= 0 { // parameter data must exist for all parameters
				return nil, errors.New("missing workset parameter values " + srcLt.Name + " set id: " + strconv.Itoa(srcLt.FromId))
			}

			// destination: insert or update parameter values in workset
			dstLt.Name = dstWl.Lst[k].Param[j].Name

			err = db.WriteParameter(dstDb, dstModel, &dstLt, cLst)
			if err != nil {
				return nil, err
			}
		}
	}

	return setIdMap, nil
}
