// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// UpdateModelText insert new or update existing model text (description and notes) in database.
//
// Model id, type Hid, parameter Hid, table Hid, language id updated with actual database id's
func UpdateModelText(dbConn *sql.DB, modelDef *ModelMeta, langDef *LangMeta, modelTxt *ModelTxtMeta) error {

	// validate parameters
	if modelTxt == nil {
		return nil // source is empty: nothing to do, exit
	}
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if langDef == nil {
		return errors.New("invalid (empty) language list")
	}
	if modelTxt.ModelName != modelDef.Model.Name || modelTxt.ModelDigest != modelDef.Model.Digest {
		return errors.New("invalid model name " + modelTxt.ModelName + " or digest " + modelTxt.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err = doUpdateModelText(trx, modelDef, langDef, modelTxt); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// doUpdateModelText insert new or update existing model text (description and notes) in database.
// It does update as part of transaction
// Model id, type Hid, parameter Hid, table Hid, language id updated with actual database id's
func doUpdateModelText(trx *sql.Tx, modelDef *ModelMeta, langDef *LangMeta, modelTxt *ModelTxtMeta) error {

	// update model_dic_txt and ids
	smId := strconv.Itoa(modelDef.Model.ModelId)
	for idx := range modelTxt.ModelTxt {

		// update model id and language id
		modelTxt.ModelTxt[idx].ModelId = modelDef.Model.ModelId
		modelTxt.ModelTxt[idx].LangId = langDef.IdByCode(modelTxt.ModelTxt[idx].LangCode)

		// delete and insert into model_dic_txt
		err := TrxUpdate(trx,
			"DELETE FROM model_dic_txt WHERE model_id = "+smId+" AND lang_id = "+strconv.Itoa(modelTxt.ModelTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO model_dic_txt (model_id, lang_id, descr, note) VALUES ("+
				smId+", "+
				strconv.Itoa(modelTxt.ModelTxt[idx].LangId)+", "+
				toQuoted(modelTxt.ModelTxt[idx].Descr)+", "+
				toQuotedOrNull(modelTxt.ModelTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	// update type_dic_txt and ids
	for idx := range modelTxt.TypeTxt {

		// update model id and language id
		modelTxt.TypeTxt[idx].ModelId = modelDef.Model.ModelId
		modelTxt.TypeTxt[idx].LangId = langDef.IdByCode(modelTxt.TypeTxt[idx].LangCode)

		// find type Hid
		k, ok := modelDef.TypeByKey(modelTxt.TypeTxt[idx].TypeId)
		if !ok {
			return errors.New("invalid type id " + strconv.Itoa(modelTxt.TypeTxt[idx].TypeId))
		}
		hId := modelDef.Type[k].TypeHid

		// delete and insert into type_dic_txt
		err := TrxUpdate(trx,
			"DELETE FROM type_dic_txt"+
				" WHERE type_hid = "+strconv.Itoa(hId)+
				" AND lang_id = "+strconv.Itoa(modelTxt.TypeTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO type_dic_txt (type_hid, lang_id, descr, note) VALUES ("+
				strconv.Itoa(hId)+", "+
				strconv.Itoa(modelTxt.TypeTxt[idx].LangId)+", "+
				toQuoted(modelTxt.TypeTxt[idx].Descr)+", "+
				toQuotedOrNull(modelTxt.TypeTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	// update type_enum_txt and ids
	for idx := range modelTxt.TypeEnumTxt {

		// update model id and language id
		modelTxt.TypeEnumTxt[idx].ModelId = modelDef.Model.ModelId
		modelTxt.TypeEnumTxt[idx].LangId = langDef.IdByCode(modelTxt.TypeEnumTxt[idx].LangCode)

		// find type Hid
		k, ok := modelDef.TypeByKey(modelTxt.TypeEnumTxt[idx].TypeId)
		if !ok {
			return errors.New("invalid type id " + strconv.Itoa(modelTxt.TypeEnumTxt[idx].TypeId))
		}
		hId := modelDef.Type[k].TypeHid

		// delete and insert into type_enum_txt
		err := TrxUpdate(trx,
			"DELETE FROM type_enum_txt"+
				" WHERE type_hid = "+strconv.Itoa(hId)+
				" AND enum_id = "+strconv.Itoa(modelTxt.TypeEnumTxt[idx].EnumId)+
				" AND lang_id = "+strconv.Itoa(modelTxt.TypeEnumTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO type_enum_txt (type_hid, enum_id, lang_id, descr, note) VALUES ("+
				strconv.Itoa(hId)+", "+
				strconv.Itoa(modelTxt.TypeEnumTxt[idx].EnumId)+", "+
				strconv.Itoa(modelTxt.TypeEnumTxt[idx].LangId)+", "+
				toQuoted(modelTxt.TypeEnumTxt[idx].Descr)+", "+
				toQuotedOrNull(modelTxt.TypeEnumTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	// update parameter_dic_txt and ids
	for idx := range modelTxt.ParamTxt {

		// update model id and language id
		modelTxt.ParamTxt[idx].ModelId = modelDef.Model.ModelId
		modelTxt.ParamTxt[idx].LangId = langDef.IdByCode(modelTxt.ParamTxt[idx].LangCode)

		// find parameter Hid
		hId := modelDef.ParamHidById(modelTxt.ParamTxt[idx].ParamId)
		if hId <= 0 {
			return errors.New("invalid parameter id " + strconv.Itoa(modelTxt.ParamTxt[idx].ParamId))
		}

		// delete and insert into parameter_dic_txt
		err := TrxUpdate(trx,
			"DELETE FROM parameter_dic_txt"+
				" WHERE parameter_hid = "+strconv.Itoa(hId)+
				" AND lang_id = "+strconv.Itoa(modelTxt.ParamTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO parameter_dic_txt (parameter_hid, lang_id, descr, note) VALUES ("+
				strconv.Itoa(hId)+", "+
				strconv.Itoa(modelTxt.ParamTxt[idx].LangId)+", "+
				toQuoted(modelTxt.ParamTxt[idx].Descr)+", "+
				toQuotedOrNull(modelTxt.ParamTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	// update parameter_dims_txt and ids
	for idx := range modelTxt.ParamDimsTxt {

		// update model id and language id
		modelTxt.ParamDimsTxt[idx].ModelId = modelDef.Model.ModelId
		modelTxt.ParamDimsTxt[idx].LangId = langDef.IdByCode(modelTxt.ParamDimsTxt[idx].LangCode)

		// find parameter Hid
		hId := modelDef.ParamHidById(modelTxt.ParamDimsTxt[idx].ParamId)
		if hId <= 0 {
			return errors.New("invalid parameter id " + strconv.Itoa(modelTxt.ParamDimsTxt[idx].ParamId))
		}

		// delete and insert into parameter_dims_txt
		err := TrxUpdate(trx,
			"DELETE FROM parameter_dims_txt"+
				" WHERE parameter_hid = "+strconv.Itoa(hId)+
				" AND dim_id = "+strconv.Itoa(modelTxt.ParamDimsTxt[idx].DimId)+
				" AND lang_id = "+strconv.Itoa(modelTxt.ParamDimsTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO parameter_dims_txt (parameter_hid, dim_id, lang_id, descr, note) VALUES ("+
				strconv.Itoa(hId)+", "+
				strconv.Itoa(modelTxt.ParamDimsTxt[idx].DimId)+", "+
				strconv.Itoa(modelTxt.ParamDimsTxt[idx].LangId)+", "+
				toQuoted(modelTxt.ParamDimsTxt[idx].Descr)+", "+
				toQuotedOrNull(modelTxt.ParamDimsTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	// update table_dic_txt and ids
	for idx := range modelTxt.TableTxt {

		// update model id and language id
		modelTxt.TableTxt[idx].ModelId = modelDef.Model.ModelId
		modelTxt.TableTxt[idx].LangId = langDef.IdByCode(modelTxt.TableTxt[idx].LangCode)

		// find output table Hid
		hId := modelDef.OutTableHidById(modelTxt.TableTxt[idx].TableId)
		if hId <= 0 {
			return errors.New("invalid output table id " + strconv.Itoa(modelTxt.TableTxt[idx].TableId))
		}

		// delete and insert into table_dic_txt
		err := TrxUpdate(trx,
			"DELETE FROM table_dic_txt"+
				" WHERE table_hid = "+strconv.Itoa(hId)+
				" AND lang_id = "+strconv.Itoa(modelTxt.TableTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO table_dic_txt (table_hid, lang_id, descr, note, expr_descr, expr_note) VALUES ("+
				strconv.Itoa(hId)+", "+
				strconv.Itoa(modelTxt.TableTxt[idx].LangId)+", "+
				toQuoted(modelTxt.TableTxt[idx].Descr)+", "+
				toQuotedOrNull(modelTxt.TableTxt[idx].Note)+", "+
				toQuoted(modelTxt.TableTxt[idx].ExprDescr)+", "+
				toQuotedOrNull(modelTxt.TableTxt[idx].ExprNote)+")")
		if err != nil {
			return err
		}
	}

	// update table_dims_txt and ids
	for idx := range modelTxt.TableDimsTxt {

		// update model id and language id
		modelTxt.TableDimsTxt[idx].ModelId = modelDef.Model.ModelId
		modelTxt.TableDimsTxt[idx].LangId = langDef.IdByCode(modelTxt.TableDimsTxt[idx].LangCode)

		// find output table Hid
		hId := modelDef.OutTableHidById(modelTxt.TableDimsTxt[idx].TableId)
		if hId <= 0 {
			return errors.New("invalid output table id " + strconv.Itoa(modelTxt.TableDimsTxt[idx].TableId))
		}

		// delete and insert into table_dims_txt
		err := TrxUpdate(trx,
			"DELETE FROM table_dims_txt"+
				" WHERE table_hid = "+strconv.Itoa(hId)+
				" AND dim_id = "+strconv.Itoa(modelTxt.TableDimsTxt[idx].DimId)+
				" AND lang_id = "+strconv.Itoa(modelTxt.TableDimsTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO table_dims_txt (table_hid, dim_id, lang_id, descr, note) VALUES ("+
				strconv.Itoa(hId)+", "+
				strconv.Itoa(modelTxt.TableDimsTxt[idx].DimId)+", "+
				strconv.Itoa(modelTxt.TableDimsTxt[idx].LangId)+", "+
				toQuoted(modelTxt.TableDimsTxt[idx].Descr)+", "+
				toQuotedOrNull(modelTxt.TableDimsTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	// update table_acc_txt and ids
	for idx := range modelTxt.TableAccTxt {

		// update model id and language id
		modelTxt.TableAccTxt[idx].ModelId = modelDef.Model.ModelId
		modelTxt.TableAccTxt[idx].LangId = langDef.IdByCode(modelTxt.TableAccTxt[idx].LangCode)

		// find output table Hid
		hId := modelDef.OutTableHidById(modelTxt.TableAccTxt[idx].TableId)
		if hId <= 0 {
			return errors.New("invalid output table id " + strconv.Itoa(modelTxt.TableAccTxt[idx].TableId))
		}

		// delete and insert into table_acc_txt
		err := TrxUpdate(trx,
			"DELETE FROM table_acc_txt"+
				" WHERE table_hid = "+strconv.Itoa(hId)+
				" AND acc_id = "+strconv.Itoa(modelTxt.TableAccTxt[idx].AccId)+
				" AND lang_id = "+strconv.Itoa(modelTxt.TableAccTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO table_acc_txt (table_hid, acc_id, lang_id, descr, note) VALUES ("+
				strconv.Itoa(hId)+", "+
				strconv.Itoa(modelTxt.TableAccTxt[idx].AccId)+", "+
				strconv.Itoa(modelTxt.TableAccTxt[idx].LangId)+", "+
				toQuoted(modelTxt.TableAccTxt[idx].Descr)+", "+
				toQuotedOrNull(modelTxt.TableAccTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	// update table_expr_txt and ids
	for idx := range modelTxt.TableExprTxt {

		// update model id and language id
		modelTxt.TableExprTxt[idx].ModelId = modelDef.Model.ModelId
		modelTxt.TableExprTxt[idx].LangId = langDef.IdByCode(modelTxt.TableExprTxt[idx].LangCode)

		// find output table Hid
		hId := modelDef.OutTableHidById(modelTxt.TableExprTxt[idx].TableId)
		if hId <= 0 {
			return errors.New("invalid output table id " + strconv.Itoa(modelTxt.TableExprTxt[idx].TableId))
		}

		// delete and insert into table_expr_txt
		err := TrxUpdate(trx,
			"DELETE FROM table_expr_txt"+
				" WHERE table_hid = "+strconv.Itoa(hId)+
				" AND expr_id = "+strconv.Itoa(modelTxt.TableExprTxt[idx].ExprId)+
				" AND lang_id = "+strconv.Itoa(modelTxt.TableExprTxt[idx].LangId))
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"INSERT INTO table_expr_txt (table_hid, expr_id, lang_id, descr, note) VALUES ("+
				strconv.Itoa(hId)+", "+
				strconv.Itoa(modelTxt.TableExprTxt[idx].ExprId)+", "+
				strconv.Itoa(modelTxt.TableExprTxt[idx].LangId)+", "+
				toQuoted(modelTxt.TableExprTxt[idx].Descr)+", "+
				toQuotedOrNull(modelTxt.TableExprTxt[idx].Note)+")")
		if err != nil {
			return err
		}
	}

	return nil
}
