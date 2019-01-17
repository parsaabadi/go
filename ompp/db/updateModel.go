// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"

	"go.openmpp.org/ompp/helper"
)

// UpdateModel insert new model metadata in database.
//
// If model with same digest already exist then call simply existing model metadata (no changes made in database).
// Parameters and output tables Hid's and db table names updated with actual database values
// If new model inserted then modelDef updated with actual id's (model id, parameter Hid...)
// If parameter (output table) not exist then create db tables for parameter values (output table values)
// If db table names is "" empty then make db table names for parameter values (output table values)
func UpdateModel(dbConn *sql.DB, dbFacet Facet, modelDef *ModelMeta) error {

	// validate parameters
	if modelDef == nil {
		return errors.New("invalid (empty) model metadata")
	}
	if modelDef.Model.Name == "" || modelDef.Model.Digest == "" {
		return errors.New("invalid (empty) model name or model digest")
	}

	// check if model already exist
	isExist, mId, err := GetModelId(dbConn, "", modelDef.Model.Digest)
	if err != nil {
		return err
	}
	if isExist {
		md, err := GetModelById(dbConn, mId) // read existing model definition
		if err != nil {
			return err
		}
		*modelDef = *md
		return err
	}
	// else
	// model not exist: do insert and update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err = doInsertModel(trx, dbFacet, modelDef); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// doInsertModel insert new existing model metadata in database.
// It does update as part of transaction
// Parameters and output tables Hid's and db table names updated with actual database values
// If new model inserted then modelDef updated with actual id's (model id, parameter Hid...)
// If parameter (output table) not exist then create db tables for parameter values (output table values)
// If db table names is "" empty or too long then make db table names for parameter values (output table values)
func doInsertModel(trx *sql.Tx, dbFacet Facet, modelDef *ModelMeta) error {

	// find default model language id by code
	var dlId int
	err := TrxSelectFirst(trx,
		"SELECT lang_id FROM lang_lst WHERE lang_code = "+toQuoted(modelDef.Model.DefaultLangCode),
		func(row *sql.Row) error {
			return row.Scan(&dlId)
		})
	switch {
	case err == sql.ErrNoRows:
		return errors.New("invalid default model language: " + modelDef.Model.DefaultLangCode)
	case err != nil:
		return err
	}

	// get new model id
	err = TrxUpdate(trx,
		"UPDATE id_lst SET id_value = id_value + 1 WHERE id_key = 'model_id'")
	if err != nil {
		return err
	}
	err = TrxSelectFirst(trx,
		"SELECT id_value FROM id_lst WHERE id_key = 'model_id'",
		func(row *sql.Row) error {
			return row.Scan(&modelDef.Model.ModelId)
		})
	switch {
	case err == sql.ErrNoRows:
		return errors.New("invalid destination database, likely not an openM++ database")
	case err != nil:
		return err
	}
	smId := strconv.Itoa(modelDef.Model.ModelId)

	// insert model_dic row with new model id
	// INSERT INTO model_dic
	//   (model_id, model_name, model_digest, model_type, model_ver, create_dt, default_lang_id)
	// VALUES
	//   (1234, 'modelOne', '1234abcd', 0, '1.0.0.0', '2012-08-17 16:04:59.148')
	err = TrxUpdate(trx,
		"INSERT INTO model_dic"+
			" (model_id, model_name, model_digest, model_type, model_ver, create_dt, default_lang_id)"+
			" VALUES ("+
			smId+", "+
			toQuoted(modelDef.Model.Name)+", "+
			toQuoted(modelDef.Model.Digest)+", "+
			strconv.Itoa(modelDef.Model.Type)+", "+
			toQuoted(modelDef.Model.Version)+", "+
			toQuoted(modelDef.Model.CreateDateTime)+", "+
			strconv.Itoa(dlId)+")")
	if err != nil {
		return err
	}

	// for each type:
	// if type not exist then insert into type_dic, type_enum_lst
	// update type Hid with actual db value
	// insert into model_type_dic to append this type to the model
	for idx := range modelDef.Type {

		modelDef.Type[idx].ModelId = modelDef.Model.ModelId // update model id with db value

		// get new type Hid
		// UPDATE id_lst SET id_value =
		//   CASE
		//     WHEN 0 = (SELECT COUNT(*) FROM type_dic WHERE type_digest = 'abcdef')
		//       THEN id_value + 1
		//     ELSE id_value
		//   END
		// WHERE id_key = 'type_hid'
		err = TrxUpdate(trx,
			"UPDATE id_lst SET id_value ="+
				" CASE"+
				" WHEN 0 = (SELECT COUNT(*) FROM type_dic WHERE type_digest = "+toQuoted(modelDef.Type[idx].Digest)+")"+
				" THEN id_value + 1"+
				" ELSE id_value"+
				" END"+
				" WHERE id_key = 'type_hid'")
		if err != nil {
			return err
		}

		// check if this type already exist
		modelDef.Type[idx].TypeHid = 0
		err = TrxSelectFirst(trx,
			"SELECT type_hid FROM type_dic WHERE type_digest = "+toQuoted(modelDef.Type[idx].Digest),
			func(row *sql.Row) error {
				return row.Scan(&modelDef.Type[idx].TypeHid)
			})
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		// if type not exists then insert into type_dic and type_enum_lst
		if modelDef.Type[idx].TypeHid <= 0 {

			// get new type Hid
			err = TrxSelectFirst(trx,
				"SELECT id_value FROM id_lst WHERE id_key = 'type_hid'",
				func(row *sql.Row) error {
					return row.Scan(&modelDef.Type[idx].TypeHid)
				})
			switch {
			case err == sql.ErrNoRows:
				return errors.New("invalid destination database, likely not an openM++ database")
			case err != nil:
				return err
			}

			// INSERT INTO type_dic
			//   (type_hid, type_name, type_digest, dic_id, total_enum_id)
			// VALUES
			//   (26, 'age', '20128171604590121', 2, 4)
			err = TrxUpdate(trx,
				"INSERT INTO type_dic (type_hid, type_name, type_digest, dic_id, total_enum_id)"+
					" VALUES ("+
					strconv.Itoa(modelDef.Type[idx].TypeHid)+", "+
					toQuoted(modelDef.Type[idx].Name)+", "+
					toQuoted(modelDef.Type[idx].Digest)+", "+
					strconv.Itoa(modelDef.Type[idx].DicId)+", "+
					strconv.Itoa(modelDef.Type[idx].TotalEnumId)+")")
			if err != nil {
				return err
			}

			// INSERT INTO type_enum_lst (type_hid, enum_id, enum_name) VALUES (26, 0, '10')
			for j := range modelDef.Type[idx].Enum {

				modelDef.Type[idx].Enum[j].ModelId = modelDef.Model.ModelId // update model id with db value

				err = TrxUpdate(trx,
					"INSERT INTO type_enum_lst (type_hid, enum_id, enum_name) VALUES ("+
						strconv.Itoa(modelDef.Type[idx].TypeHid)+", "+
						strconv.Itoa(modelDef.Type[idx].Enum[j].EnumId)+", "+
						toQuoted(modelDef.Type[idx].Enum[j].Name)+")")
				if err != nil {
					return err
				}
			}
		}

		// append type into model type list, if not in the list
		//
		// INSERT INTO model_type_dic (model_id, model_type_id, type_hid)
		// SELECT 1234, 31, D.type_hid
		// FROM type_dic D
		// WHERE D.type_digest = '20128171604590121'
		// AND NOT EXISTS
		// (
		//   SELECT * FROM model_type_dic E WHERE E.model_id = 1234 AND E.model_type_id = 31
		// )
		err = TrxUpdate(trx,
			"INSERT INTO model_type_dic (model_id, model_type_id, type_hid)"+
				" SELECT "+
				smId+", "+
				strconv.Itoa(modelDef.Type[idx].TypeId)+", "+
				" D.type_hid"+
				" FROM type_dic D"+
				" WHERE D.type_digest = "+toQuoted(modelDef.Type[idx].Digest)+
				" AND NOT EXISTS"+
				" (SELECT * FROM model_type_dic E"+
				" WHERE E.model_id = "+smId+
				" AND E.model_type_id = "+strconv.Itoa(modelDef.Type[idx].TypeId)+
				" )")
		if err != nil {
			return err
		}
	}

	// for each parameter:
	// if parameter not exist then insert into parameter_dic, parameter_dims
	// update parameter Hid with actual db value
	// insert into model_parameter_dic to append this parameter to the model
	// if parameter not exist then create db tables for parameter values
	// if db table names is "" empty then make db table names for parameter values
	for idx := range modelDef.Param {

		modelDef.Param[idx].ModelId = modelDef.Model.ModelId // update model id with db value

		// get new parameter Hid
		// UPDATE id_lst SET id_value =
		//   CASE
		//     WHEN 0 = (SELECT COUNT(*) FROM parameter_dic WHERE parameter_digest = '978abf5')
		//       THEN id_value + 1
		//     ELSE id_value
		//   END
		// WHERE id_key = 'parameter_hid'
		err = TrxUpdate(trx,
			"UPDATE id_lst SET id_value ="+
				" CASE"+
				" WHEN 0 = (SELECT COUNT(*) FROM parameter_dic WHERE parameter_digest = "+toQuoted(modelDef.Param[idx].Digest)+")"+
				" THEN id_value + 1"+
				" ELSE id_value"+
				" END"+
				" WHERE id_key = 'parameter_hid'")
		if err != nil {
			return err
		}

		// check if this parameter already exist
		modelDef.Param[idx].ParamHid = 0
		err = TrxSelectFirst(trx,
			"SELECT parameter_hid FROM parameter_dic WHERE parameter_digest = "+toQuoted(modelDef.Param[idx].Digest),
			func(row *sql.Row) error {
				return row.Scan(&modelDef.Param[idx].ParamHid)
			})
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		// if parameter not exists then insert into parameter_dic and parameter_dims
		// if db table names is "" empty then make db table names for parameter values
		// if parameter not exist then create db tables for parameter values
		if modelDef.Param[idx].ParamHid <= 0 {

			// get new parameter Hid
			err = TrxSelectFirst(trx,
				"SELECT id_value FROM id_lst WHERE id_key = 'parameter_hid'",
				func(row *sql.Row) error {
					return row.Scan(&modelDef.Param[idx].ParamHid)
				})
			switch {
			case err == sql.ErrNoRows:
				return errors.New("invalid destination database, likely not an openM++ database")
			case err != nil:
				return err
			}

			// update parameter values db table names, if empty or too long for current database
			if modelDef.Param[idx].DbRunTable == "" ||
				len(modelDef.Param[idx].DbRunTable) > dbFacet.maxTableNameSize() ||
				modelDef.Param[idx].DbSetTable == "" ||
				len(modelDef.Param[idx].DbSetTable) > dbFacet.maxTableNameSize() {

				p, s := makeDbTablePrefixSuffix(dbFacet, modelDef.Param[idx].Name, modelDef.Param[idx].Digest)
				modelDef.Param[idx].DbRunTable = p + "_p" + s
				modelDef.Param[idx].DbSetTable = p + "_w" + s
			}

			// INSERT INTO parameter_dic
			//   (parameter_hid, parameter_name, parameter_digest, db_run_table,
			//   db_set_table, parameter_rank, type_hid, is_extendable, num_cumulated)
			// VALUES
			//   (4, 'ageSex', '978abf5', 'ageSex', '2012817', 2, 14, 1, 0)
			err = TrxUpdate(trx,
				"INSERT INTO parameter_dic"+
					" (parameter_hid, parameter_name, parameter_digest, db_run_table,"+
					" db_set_table, parameter_rank, type_hid, is_extendable,"+
					" num_cumulated)"+
					" VALUES ("+
					strconv.Itoa(modelDef.Param[idx].ParamHid)+", "+
					toQuoted(modelDef.Param[idx].Name)+", "+
					toQuoted(modelDef.Param[idx].Digest)+", "+
					toQuoted(modelDef.Param[idx].DbRunTable)+", "+
					toQuoted(modelDef.Param[idx].DbSetTable)+", "+
					strconv.Itoa(modelDef.Param[idx].Rank)+", "+
					strconv.Itoa(modelDef.Param[idx].typeOf.TypeHid)+", "+
					toBoolSqlConst(modelDef.Param[idx].IsExtendable)+", "+
					strconv.Itoa(modelDef.Param[idx].NumCumulated)+")")
			if err != nil {
				return err
			}

			// INSERT INTO parameter_dims (parameter_hid, dim_id, dim_name, type_hid) VALUES (4, 0, 'dim0', 26)
			for j := range modelDef.Param[idx].Dim {

				modelDef.Param[idx].Dim[j].ModelId = modelDef.Model.ModelId // update model id with db value

				err = TrxUpdate(trx,
					"INSERT INTO parameter_dims (parameter_hid, dim_id, dim_name, type_hid) VALUES ("+
						strconv.Itoa(modelDef.Param[idx].ParamHid)+", "+
						strconv.Itoa(modelDef.Param[idx].Dim[j].DimId)+", "+
						toQuoted(modelDef.Param[idx].Dim[j].Name)+", "+
						strconv.Itoa(modelDef.Param[idx].Dim[j].typeOf.TypeHid)+")")
				if err != nil {
					return err
				}
			}

			// create parameter tables: parameter run values and parameter workset values
			rSql, wSql, err := paramCreateTable(dbFacet, &modelDef.Param[idx])
			if err != nil {
				return err
			}
			err = TrxUpdate(trx, rSql)
			if err != nil {
				return err
			}
			err = TrxUpdate(trx, wSql)
			if err != nil {
				return err
			}
		}

		// append type into model parameter list, if not in the list
		//
		// INSERT INTO model_parameter_dic (model_id, model_parameter_id, parameter_hid, is_hidden)
		// SELECT 1234, 0, D.parameter_hid, 1
		// FROM parameter_dic D
		// WHERE D.parameter_digest = '978abf5'
		// AND NOT EXISTS
		// (
		//   SELECT * FROM model_parameter_dic E WHERE E.model_id = 1234 AND E.model_parameter_id = 0
		// )
		err = TrxUpdate(trx,
			"INSERT INTO model_parameter_dic (model_id, model_parameter_id, parameter_hid, is_hidden)"+
				" SELECT "+
				smId+", "+
				strconv.Itoa(modelDef.Param[idx].ParamId)+", "+
				" D.parameter_hid, "+
				toBoolSqlConst(modelDef.Param[idx].IsHidden)+
				" FROM parameter_dic D"+
				" WHERE D.parameter_digest = "+toQuoted(modelDef.Param[idx].Digest)+
				" AND NOT EXISTS"+
				" (SELECT * FROM model_parameter_dic E"+
				" WHERE E.model_id = "+smId+
				" AND E.model_parameter_id = "+strconv.Itoa(modelDef.Param[idx].ParamId)+
				" )")
		if err != nil {
			return err
		}
	}

	// for each output table:
	// if output table not exist then insert into table_dic, table_dims, table_acc, table_expr
	// update table Hid with actual db value
	// insert into model_table_dic to append this output table to the model
	// if output table not exist then create db tables for output table values
	// if db table names is "" empty then make db table names for output table values
	for idx := range modelDef.Table {

		modelDef.Table[idx].ModelId = modelDef.Model.ModelId // update model id with db value

		// get new output table Hid
		// UPDATE id_lst SET id_value =
		//   CASE
		//     WHEN 0 = (SELECT COUNT(*) FROM table_dic WHERE table_digest = '0887a6494df')
		//      THEN id_value + 1
		//     ELSE id_value
		//   END
		// WHERE id_key = 'table_hid'
		err = TrxUpdate(trx,
			"UPDATE id_lst SET id_value ="+
				" CASE"+
				" WHEN 0 = (SELECT COUNT(*) FROM table_dic WHERE table_digest = "+toQuoted(modelDef.Table[idx].Digest)+")"+
				" THEN id_value + 1"+
				" ELSE id_value"+
				" END"+
				" WHERE id_key = 'table_hid'")
		if err != nil {
			return err
		}

		// check if this output table already exist
		modelDef.Table[idx].TableHid = 0
		err = TrxSelectFirst(trx,
			"SELECT table_hid FROM table_dic WHERE table_digest = "+toQuoted(modelDef.Table[idx].Digest),
			func(row *sql.Row) error {
				return row.Scan(&modelDef.Table[idx].TableHid)
			})
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		// if output table not exists then insert into table_dic, table_dims, table_acc, table_expr
		// if output table not exist then create db tables for output table values
		// if db table names is "" empty then make db table names for output table values
		if modelDef.Table[idx].TableHid <= 0 {

			// get new output table Hid
			err = TrxSelectFirst(trx,
				"SELECT id_value FROM id_lst WHERE id_key = 'table_hid'",
				func(row *sql.Row) error {
					return row.Scan(&modelDef.Table[idx].TableHid)
				})
			switch {
			case err == sql.ErrNoRows:
				return errors.New("invalid destination database, likely not an openM++ database")
			case err != nil:
				return err
			}

			// update output table values db table names, if empty or too long for current database
			if modelDef.Table[idx].DbExprTable == "" ||
				len(modelDef.Table[idx].DbExprTable) > dbFacet.maxTableNameSize() ||
				modelDef.Table[idx].DbAccTable == "" ||
				len(modelDef.Table[idx].DbAccTable) > dbFacet.maxTableNameSize() {

				p, s := makeDbTablePrefixSuffix(dbFacet, modelDef.Table[idx].Name, modelDef.Table[idx].Digest)
				modelDef.Table[idx].DbExprTable = p + "_v" + s
				modelDef.Table[idx].DbAccTable = p + "_a" + s
				modelDef.Table[idx].DbAccAllView = p + "_d" + s
			}

			// INSERT INTO table_dic
			//   (table_hid, table_name, table_digest, table_rank,
			//   is_sparse, db_expr_table, db_acc_table, db_acc_all_view)
			// VALUES
			//   (2, 'salarySex', '0887a6494df', 'salarySex', '2012820', 2, 1)
			err = TrxUpdate(trx,
				"INSERT INTO table_dic"+
					" (table_hid, table_name, table_digest, table_rank,"+
					" is_sparse, db_expr_table, db_acc_table, db_acc_all_view)"+
					" VALUES ("+
					strconv.Itoa(modelDef.Table[idx].TableHid)+", "+
					toQuoted(modelDef.Table[idx].Name)+", "+
					toQuoted(modelDef.Table[idx].Digest)+", "+
					strconv.Itoa(modelDef.Table[idx].Rank)+", "+
					toBoolSqlConst(modelDef.Table[idx].IsSparse)+", "+
					toQuoted(modelDef.Table[idx].DbExprTable)+", "+
					toQuoted(modelDef.Table[idx].DbAccTable)+", "+
					toQuoted(modelDef.Table[idx].DbAccAllView)+
					")")
			if err != nil {
				return err
			}

			// INSERT INTO table_dims
			//   (table_hid, dim_id, dim_name, type_hid, is_total, dim_size)
			// VALUES
			//   (2, 0, 'dim0', 28, 0, 3)
			for j := range modelDef.Table[idx].Dim {

				modelDef.Table[idx].Dim[j].ModelId = modelDef.Model.ModelId // update model id with db value

				err = TrxUpdate(trx,
					"INSERT INTO table_dims (table_hid, dim_id, dim_name, type_hid, is_total, dim_size)"+
						" VALUES ("+
						strconv.Itoa(modelDef.Table[idx].TableHid)+", "+
						strconv.Itoa(modelDef.Table[idx].Dim[j].DimId)+", "+
						toQuoted(modelDef.Table[idx].Dim[j].Name)+", "+
						strconv.Itoa(modelDef.Table[idx].Dim[j].typeOf.TypeHid)+", "+
						toBoolSqlConst(modelDef.Table[idx].Dim[j].IsTotal)+", "+
						strconv.Itoa(modelDef.Table[idx].Dim[j].DimSize)+")")
				if err != nil {
					return err
				}
			}

			// INSERT INTO table_acc (table_hid, acc_id, acc_name, is_derived, acc_expr)
			// VALUES (2, 4, 'acc4', 1, 'acc0 + acc1')
			for j := range modelDef.Table[idx].Acc {

				modelDef.Table[idx].Acc[j].ModelId = modelDef.Model.ModelId // update model id with db value

				err = TrxUpdate(trx,
					"INSERT INTO table_acc (table_hid, acc_id, acc_name, is_derived, acc_src, acc_sql)"+
						" VALUES ("+
						strconv.Itoa(modelDef.Table[idx].TableHid)+", "+
						strconv.Itoa(modelDef.Table[idx].Acc[j].AccId)+", "+
						toQuoted(modelDef.Table[idx].Acc[j].Name)+", "+
						toBoolSqlConst(modelDef.Table[idx].Acc[j].IsDerived)+", "+
						toQuoted(modelDef.Table[idx].Acc[j].SrcAcc)+", "+
						toQuoted(modelDef.Table[idx].Acc[j].AccSql)+")")
				if err != nil {
					return err
				}
			}

			// INSERT INTO table_expr
			//   (table_hid, expr_id, expr_name, expr_decimals, expr_src, expr_sql)
			// VALUES
			//   (2, 0, 'expr0', 4, 'OM_AVG(acc0)', '....sql....')
			for j := range modelDef.Table[idx].Expr {

				modelDef.Table[idx].Expr[j].ModelId = modelDef.Model.ModelId // update model id with db value

				err = TrxUpdate(trx,
					"INSERT INTO table_expr (table_hid, expr_id, expr_name, expr_decimals, expr_src, expr_sql)"+
						" VALUES ("+
						strconv.Itoa(modelDef.Table[idx].TableHid)+", "+
						strconv.Itoa(modelDef.Table[idx].Expr[j].ExprId)+", "+
						toQuoted(modelDef.Table[idx].Expr[j].Name)+", "+
						strconv.Itoa(modelDef.Table[idx].Expr[j].Decimals)+", "+
						toQuoted(modelDef.Table[idx].Expr[j].SrcExpr)+", "+
						toQuoted(modelDef.Table[idx].Expr[j].ExprSql)+")")
				if err != nil {
					return err
				}
			}

			// create db tables: output table expression(s) values and accumulator(s) values
			eSql, aSql, err := outTableCreateTable(dbFacet, &modelDef.Table[idx])
			if err != nil {
				return err
			}
			err = TrxUpdate(trx, eSql)
			if err != nil {
				return err
			}
			err = TrxUpdate(trx, aSql)
			if err != nil {
				return err
			}

			// create db views: output table all accumulators view
			avSql, err := outTableCreateAccAllView(dbFacet, &modelDef.Table[idx])
			if err != nil {
				return err
			}
			err = TrxUpdate(trx, avSql)
			if err != nil {
				return err
			}
		}

		// append type into model output table list, if not in the list
		//
		// INSERT INTO model_table_dic (model_id, model_table_id, table_hid, is_user, expr_dim_pos)
		// SELECT 1234, 0, D.table_hid, 0, 1
		// FROM table_dic D
		// WHERE D.table_digest = '0887a6494df'
		// AND NOT EXISTS
		// (
		//   SELECT * FROM model_table_dic E WHERE E.model_id = 1234 AND E.model_table_id = 0
		// )
		err = TrxUpdate(trx,
			"INSERT INTO model_table_dic (model_id, model_table_id, table_hid, is_user, expr_dim_pos)"+
				" SELECT "+
				smId+", "+
				strconv.Itoa(modelDef.Table[idx].TableId)+", "+
				" D.table_hid, "+
				toBoolSqlConst(modelDef.Table[idx].IsUser)+", "+
				strconv.Itoa(modelDef.Table[idx].ExprPos)+
				" FROM table_dic D"+
				" WHERE D.table_digest = "+toQuoted(modelDef.Table[idx].Digest)+
				" AND NOT EXISTS"+
				" (SELECT * FROM model_table_dic E"+
				" WHERE E.model_id = "+smId+
				" AND E.model_table_id = "+strconv.Itoa(modelDef.Table[idx].TableId)+
				" )")
		if err != nil {
			return err
		}
	}

	return nil
}

// paramCreateTable return create table for parameter run values and workset values:
//
// CREATE TABLE ageSex_p20120817
// (
//  run_id      INT   NOT NULL,
//  sub_id      INT   NOT NULL,
//  dim0        INT   NOT NULL,
//  dim1        INT   NOT NULL,
//  param_value FLOAT NOT NULL, -- can be NULL
//  PRIMARY KEY (run_id, sub_id, dim0, dim1)
// )
//
// CREATE TABLE ageSex_w20120817
// (
//  set_id      INT   NOT NULL,
//  sub_id      INT   NOT NULL,
//  dim0        INT   NOT NULL,
//  dim1        INT   NOT NULL,
//  param_value FLOAT NOT NULL, -- can be NULL
//  PRIMARY KEY (set_id, sub_id, dim0, dim1)
// )
func paramCreateTable(dbFacet Facet, param *ParamMeta) (string, string, error) {

	colPart := ""
	for k := range param.Dim {
		colPart += param.Dim[k].Name + " INT NOT NULL, "
	}

	tname, err := param.typeOf.sqlColumnType(dbFacet)
	if err != nil {
		return "", "", nil
	}
	if param.IsExtendable {
		colPart += "param_value " + tname + " NULL, " // "extendable" parameter means value can be NULL
	} else {
		colPart += "param_value " + tname + " NOT NULL, "
	}

	keyPart := ""
	for k := range param.Dim {
		keyPart += ", " + param.Dim[k].Name
	}

	rSql := dbFacet.createTableIfNotExist(param.DbRunTable, "("+
		"run_id INT NOT NULL, "+
		"sub_id INT NOT NULL, "+
		colPart+
		"PRIMARY KEY (run_id, sub_id"+keyPart+")"+
		")")
	wSql := dbFacet.createTableIfNotExist(param.DbSetTable, "("+
		"set_id INT NOT NULL, "+
		"sub_id INT NOT NULL, "+
		colPart+
		"PRIMARY KEY (set_id, sub_id"+keyPart+")"+
		")")
	return rSql, wSql, nil
}

// outTableCreateTable return create table for output table expressions and accumulators:
//
// CREATE TABLE salarySex_v20120820
// (
//  run_id     INT   NOT NULL,
//  expr_id    INT   NOT NULL,
//  dim0       INT   NOT NULL,
//  dim1       INT   NOT NULL,
//  expr_value FLOAT NULL,
//  PRIMARY KEY (run_id, expr_id, dim0, dim1)
// )
//
// CREATE TABLE salarySex_a20120820
// (
//  run_id    INT   NOT NULL,
//  acc_id    INT   NOT NULL,
//  sub_id    INT   NOT NULL,
//  dim0      INT   NOT NULL,
//  dim1      INT   NOT NULL,
//  acc_value FLOAT NULL,
//  PRIMARY KEY (run_id, acc_id, sub_id, dim0, dim1)
// )
func outTableCreateTable(dbFacet Facet, meta *TableMeta) (string, string, error) {

	colPart := ""
	for k := range meta.Dim {
		colPart += meta.Dim[k].Name + " INT NOT NULL, "
	}

	keyPart := ""
	for k := range meta.Dim {
		keyPart += ", " + meta.Dim[k].Name
	}

	eSql := dbFacet.createTableIfNotExist(meta.DbExprTable, "("+
		"run_id  INT NOT NULL, "+
		"expr_id INT NOT NULL, "+
		colPart+
		"expr_value "+dbFacet.floatType()+" NULL, "+
		"PRIMARY KEY (run_id, expr_id"+keyPart+")"+
		")")
	aSql := dbFacet.createTableIfNotExist(meta.DbAccTable, "("+
		"run_id INT NOT NULL, "+
		"acc_id INT NOT NULL, "+
		"sub_id INT NOT NULL, "+
		colPart+
		"acc_value "+dbFacet.floatType()+" NULL, "+
		"PRIMARY KEY (run_id, acc_id, sub_id"+keyPart+")"+
		")")
	return eSql, aSql, nil
}

// outTableCreateAccAllView return create view for all accumulators view:
//
// CREATE VIEW salarySex_d_2012820
// AS
// SELECT
//   A.run_id, A.sub_id, A.dim0, A.dim1,
//   A.acc_value AS acc0,
//   (
//     SELECT A1.acc_value FROM salarySex_a_2012820 A1
//     WHERE A1.run_id = A.run_id AND A1.sub_id = A.sub_id AND A1.dim0 = A.dim0 AND A1.dim1 = A.dim1
//     AND A1.acc_id = 1
//   ) AS acc1,
//   (
//     (
//       A.acc_value
//     )
//     +
//     (
//       SELECT A1.acc_value FROM salarySex_a_2012820 A1
//       WHERE A1.run_id = A.run_id AND A1.sub_id = A.sub_id AND A1.dim0 = A.dim0 AND A1.dim1 = A.dim1
//       AND A1.acc_id = 1
//     )
//   ) AS acc2
// FROM salarySex_a_2012820 A
// WHERE A.acc_id = 0;
//
func outTableCreateAccAllView(dbFacet Facet, meta *TableMeta) (string, error) {

	// start view body with run id, sub id and dimensions
	sql := "SELECT A.run_id, A.sub_id"

	for k := range meta.Dim {
		sql += ", A." + meta.Dim[k].Name
	}

	// append accumulators as sql subqueries
	for k := range meta.Acc {
		sql += ", (" + meta.Acc[k].AccSql + ") AS " + meta.Acc[k].Name
	}

	// main accumulator table
	// select first accumulator from main table
	// all other accumulators joined to the first by run id, sub id and dimensions
	sql += " FROM " + meta.DbAccTable + " A" +
		" WHERE A.acc_id = " + strconv.Itoa(meta.Acc[0].AccId) + ";"

	return dbFacet.createViewIfNotExist(meta.DbAccAllView, sql), nil
}

// return prefix and suffix for parameter value db tables or output table value db tables.
// db table name is: paramNameAsPrefix + _p + md5Suffix, for example: ageSex_p12345678abcdef
// prefix based on parameter name or output table name
// suffix is 32 chars of md5 or 8 chars of crc32
// there is extra 2 chars: _p, _w, _v, _a in table name between prefix and suffix
func makeDbTablePrefixSuffix(dbFacet Facet, name string, digest string) (string, string) {

	// if max size of db table name is too short then use crc32(md5) digest
	// isCrc32Name := dbFacet.maxTableNameSize() < 50
	isCrc32Name := true // always use short crc32 name suffix

	dbSuffixSize := 32
	if isCrc32Name {
		dbSuffixSize = 8
	}

	dbPrefixSize := dbFacet.maxTableNameSize() - (2 + dbSuffixSize)
	if dbPrefixSize < 2 {
		dbPrefixSize = 2
	}

	// make prefix part of db table name by using only [A-Z,a-z,0-9] and _ underscore
	// also shorten source name, ie: ageSexProvince => ageSexPr
	prefix := helper.ToAlphaNumeric(name)
	if len(prefix) > dbPrefixSize {
		prefix = prefix[:dbPrefixSize]
	}

	// make unique suffix of db table name by using digest or crc32(digest)
	suffix := digest
	if isCrc32Name {
		hCrc32 := crc32.NewIEEE()
		hCrc32.Write([]byte(digest))
		suffix = fmt.Sprintf("%x", hCrc32.Sum(nil))
	}

	return prefix, suffix
}
