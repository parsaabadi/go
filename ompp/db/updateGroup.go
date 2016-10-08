// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// UpdateModelGroup insert new or update existing model groups and groups text (description, notes) in group_lst, group_pc, group_txt.
//
// If modelGroup contains all non-empty GroupLst slice then all 3 tables (group_lst, group_pc, group_txt) erased and new values inserted.
// If only text slice GroupTxt non-empty then only group_txt rows updated or inserted (not deleted).
// Model id and language id updated with id's from modelDef (assuming it contains actual db id's)
func UpdateModelGroup(dbConn *sql.DB, modelDef *ModelMeta, langDef *LangList, modelGroup *GroupMeta) error {

	// source is empty: nothing to do, exit
	if modelGroup == nil {
		return nil
	}

	isMain := len(modelGroup.GroupLst) > 0
	nPc := len(modelGroup.GroupPc)
	isText := len(modelGroup.GroupTxt) > 0

	if !isMain && nPc <= 0 && !isText { // source is empty: nothing to do, exit
		return nil
	}
	if !isMain && nPc > 0 {
		return errors.New("invalid group metadata: empty group list and non-empty parent-child relationships")
	}

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if isText && langDef == nil {
		return errors.New("invalid (empty) language list")
	}
	if modelGroup.ModelName != modelDef.Model.Name || modelGroup.ModelDigest != modelDef.Model.Digest {
		return errors.New("invalid model name " + modelGroup.ModelName + " or digest " + modelGroup.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}

	// if group_lst rows supplied then update all tables else text only
	if isMain {
		if err = doUpdateGroupAll(trx, modelDef.Model.ModelId, langDef, modelGroup); err != nil {
			trx.Rollback()
			return err
		}
	} else {
		if isText {
			if err = doUpdateGroupText(trx, modelDef.Model.ModelId, langDef, modelGroup.GroupTxt); err != nil {
				trx.Rollback()
				return err
			}
		}
	}

	trx.Commit()
	return nil
}

// doUpdateGroupAll delete existing and insert new model groups: group_lst, group_pc, group_txt tables.
// It does update as part of transaction
// Model id and language id updated with actual id's from database
func doUpdateGroupAll(trx *sql.Tx, modelId int, langDef *LangList, modelGroup *GroupMeta) error {

	// delete existing groups
	smId := strconv.Itoa(modelId)

	err := TrxUpdate(trx, "DELETE FROM group_txt WHERE model_id = "+smId)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx, "DELETE FROM group_pc WHERE model_id = "+smId)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx, "DELETE FROM group_lst WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// insert new groups into group_lst
	for idx := range modelGroup.GroupLst {

		modelGroup.GroupLst[idx].ModelId = modelId // update model id with actual value

		err = TrxUpdate(trx,
			"INSERT INTO group_lst (model_id, group_id, is_parameter, group_name, is_hidden)"+
				" VALUES ("+
				smId+", "+
				strconv.Itoa(modelGroup.GroupLst[idx].GroupId)+", "+
				toBoolStr(modelGroup.GroupLst[idx].IsParam)+", "+
				toQuoted(modelGroup.GroupLst[idx].Name)+", "+
				toBoolStr(modelGroup.GroupLst[idx].IsHidden)+")")
		if err != nil {
			return err
		}
	}

	// insert new groups parent-child into group_pc
	// treat as NULL negative child group id or leaf id (parameter or output table id)
	for idx := range modelGroup.GroupPc {

		modelGroup.GroupPc[idx].ModelId = modelId // update model id with actual value

		q := "INSERT INTO group_pc (model_id, group_id, child_pos, child_group_id, leaf_id)" +
			" VALUES (" +
			smId + ", " +
			strconv.Itoa(modelGroup.GroupPc[idx].GroupId) + ", " +
			strconv.Itoa(modelGroup.GroupPc[idx].ChildPos) + ", "

		if modelGroup.GroupPc[idx].ChildGroupId >= 0 {
			q += strconv.Itoa(modelGroup.GroupPc[idx].ChildGroupId) + ", "
		} else {
			q += "NULL, "
		}
		if modelGroup.GroupPc[idx].ChildLeafId >= 0 {
			q += strconv.Itoa(modelGroup.GroupPc[idx].ChildLeafId) + ")"
		} else {
			q += "NULL)"
		}

		err = TrxUpdate(trx, q)
		if err != nil {
			return err
		}
	}

	// insert new groups text (description and notes) into group_txt
	for idx := range modelGroup.GroupTxt {

		// update model id and language id
		modelGroup.GroupTxt[idx].ModelId = modelId
		modelGroup.GroupTxt[idx].LangId = langDef.IdByCode(modelGroup.GroupTxt[idx].LangCode)

		err = TrxUpdate(trx,
			"INSERT INTO group_txt (model_id, group_id, lang_id, descr, note)"+
				" VALUES ("+
				smId+", "+
				strconv.Itoa(modelGroup.GroupTxt[idx].GroupId)+", "+
				strconv.Itoa(modelGroup.GroupTxt[idx].LangId)+", "+
				toQuoted(modelGroup.GroupTxt[idx].Descr)+", "+
				toQuotedOrNull(modelGroup.GroupTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	return nil
}

// doUpdateGroupText insert new or update existing groups text (description, notes) in group_txt table.
// It does update as part of transaction
// Model id and language id updated with actual id's from database
func doUpdateGroupText(trx *sql.Tx, modelId int, langDef *LangList, groupTxt []GroupTxtRow) error {

	// update groups text (description and notes) into group_txt
	smId := strconv.Itoa(modelId)

	for idx := range groupTxt {

		// update model id and language id
		groupTxt[idx].ModelId = modelId
		groupTxt[idx].LangId = langDef.IdByCode(groupTxt[idx].LangCode)

		// delete and insert into group_txt
		err := TrxUpdate(trx,
			"DELETE FROM group_txt"+
				" WHERE model_id = "+smId+
				" AND group_id = "+strconv.Itoa(groupTxt[idx].GroupId)+
				" AND lang_id = "+strconv.Itoa(groupTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO group_txt (model_id, group_id, lang_id, descr, note)"+
				" VALUES ("+
				smId+", "+
				strconv.Itoa(groupTxt[idx].GroupId)+", "+
				strconv.Itoa(groupTxt[idx].LangId)+", "+
				toQuoted(groupTxt[idx].Descr)+", "+
				toQuotedOrNull(groupTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	return nil
}
