// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// GetModelList return list of the models: model_dic table rows.
func GetModelList(dbConn *sql.DB) ([]ModelDicRow, error) {

	var modelRs []ModelDicRow

	err := SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_name, M.model_digest, M.model_type, M.model_ver, M.create_dt"+
			" FROM model_dic M"+
			" ORDER BY 1",
		func(rows *sql.Rows) error {
			var r ModelDicRow
			if err := rows.Scan(
				&r.ModelId, &r.Name, &r.Digest, &r.Type, &r.Version, &r.CreateDateTime); err != nil {
				return err
			}
			modelRs = append(modelRs, r)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return modelRs, nil
}

// GetModelId return model id if exists.
//
// Model selected by name and/or digest, i.e.: ("modelOne", "20120817_1604590148")
// if digest is empty then first model with min(model_id) is used
func GetModelId(dbConn *sql.DB, name, digest string) (bool, int, error) {

	// model not found: model name and digest empty
	if name == "" && digest == "" {
		return false, 0, nil
	}

	// select model_id by name and/or digest
	// if digest is empty then first model with min(model_id) is used
	q := "SELECT M.model_id FROM model_dic M"
	if name != "" && digest != "" {
		q += " WHERE M.model_name = " + toQuoted(name) +
			" AND M.model_digest = " + toQuoted(digest)
	}
	if name == "" && digest != "" {
		q += " WHERE M.model_digest = " + toQuoted(digest)
	}
	if name != "" && digest == "" {
		q += " WHERE M.model_name = " + toQuoted(name) +
			" AND M.model_id = (SELECT MIN(MMD.model_id) FROM model_dic MMD WHERE MMD.model_name = " + toQuoted(name) + ")"
	}
	q += " ORDER BY 1"

	mId := 0
	err := SelectFirst(dbConn, q,
		func(row *sql.Row) error {
			return row.Scan(&mId)
		})
	switch {
	case err == sql.ErrNoRows:
		return false, 0, nil
	case err != nil:
		return false, 0, err
	}

	return true, mId, nil
}

// GetModel return model metadata: parameters and output tables definition.
//
// Model selected by name and/or digest, i.e.: ("modelOne", "20120817_1604590148")
// if digest is empty then first model with min(model_id) is used
func GetModel(dbConn *sql.DB, name, digest string) (*ModelMeta, error) {

	if name == "" && digest == "" {
		return nil, errors.New("invalid (empty) model name and model digest")
	}

	// find model id
	isExist, mId, err := GetModelId(dbConn, name, digest)
	if err != nil {
		return nil, err
	}
	if !isExist {
		return nil, errors.New("model " + name + " " + digest + " not found")
	}

	return GetModelById(dbConn, mId)
}

// GetModelById return model metadata: parameters and output tables definition.
//
// Model selected by model id, which expected to be positive.
func GetModelById(dbConn *sql.DB, modelId int) (*ModelMeta, error) {

	// validate parameters
	if modelId <= 0 {
		return nil, errors.New("invalid model id")
	}

	// select model_dic row
	var modelRow = ModelDicRow{ModelId: modelId}

	err := SelectFirst(dbConn,
		"SELECT"+
			" M.model_id, M.model_name, M.model_digest, M.model_type, M.model_ver, M.create_dt"+
			" FROM model_dic M"+
			" WHERE M.model_id = "+strconv.Itoa(modelId)+
			" ORDER BY 1",
		func(row *sql.Row) error {
			return row.Scan(
				&modelRow.ModelId, &modelRow.Name, &modelRow.Digest, &modelRow.Type, &modelRow.Version, &modelRow.CreateDateTime)
		})
	switch {
	case err == sql.ErrNoRows:
		return nil, errors.New("model id " + strconv.Itoa(modelId) + " not found")
	case err != nil:
		return nil, err
	}

	return getModel(dbConn, &modelRow)
}

// getModel return model metadata by modelRow (model_dic row).
func getModel(dbConn *sql.DB, modelRow *ModelDicRow) (*ModelMeta, error) {

	// select db rows from type_dic join to model_type_dic
	meta := &ModelMeta{Model: *modelRow}
	smId := strconv.Itoa(meta.Model.ModelId)

	err := SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_type_id, H.type_hid, H.type_name, H.type_digest, H.dic_id, H.total_enum_id"+
			" FROM type_dic H"+
			" INNER JOIN model_type_dic M ON (M.type_hid = H.type_hid)"+
			" WHERE M.model_id = "+smId+
			" ORDER BY 1, 2",
		func(rows *sql.Rows) error {
			var r TypeDicRow
			if err := rows.Scan(
				&r.ModelId, &r.TypeId, &r.TypeHid, &r.Name, &r.Digest, &r.DicId, &r.TotalEnumId); err != nil {
				return err
			}
			meta.Type = append(meta.Type, TypeMeta{TypeDicRow: r})
			return nil
		})
	if err != nil {
		return nil, err
	}
	if len(meta.Type) <= 0 {
		return nil, errors.New("no model types found")
	}

	// select db rows from type_enum_lst join to model_type_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_type_id, D.enum_id, D.enum_name"+
			" FROM type_enum_lst D"+
			" INNER JOIN model_type_dic M ON (M.type_hid = D.type_hid)"+
			" WHERE M.model_id = "+smId+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r TypeEnumRow
			if err := rows.Scan(
				&r.ModelId, &r.TypeId, &r.EnumId, &r.Name); err != nil {
				return err
			}

			k, ok := meta.TypeByKey(r.TypeId) // find type master row
			if !ok {
				return errors.New("type " + strconv.Itoa(r.TypeId) + " not found for " + r.Name)
			}

			meta.Type[k].Enum = append(meta.Type[k].Enum, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from parameter_dic join to model_parameter_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_parameter_id, D.parameter_hid, D.parameter_name,"+
			" D.parameter_digest, D.db_run_table, D.db_set_table, D.parameter_rank,"+
			" T.model_type_id, M.is_hidden, D.num_cumulated"+
			" FROM parameter_dic D"+
			" INNER JOIN model_parameter_dic M ON (M.parameter_hid = D.parameter_hid)"+
			" INNER JOIN model_type_dic T ON (T.type_hid = D.type_hid AND T.model_id = M.model_id)"+
			" WHERE M.model_id = "+smId+
			" ORDER BY 1, 2",
		func(rows *sql.Rows) error {
			var r ParamDicRow
			nHidden := 0
			if err := rows.Scan(
				&r.ModelId, &r.ParamId, &r.ParamHid, &r.Name,
				&r.Digest, &r.DbRunTable, &r.DbSetTable, &r.Rank,
				&r.TypeId, &nHidden, &r.NumCumulated); err != nil {
				return err
			}
			r.IsHidden = nHidden != 0 // oracle: smallint is float64

			k, ok := meta.TypeByKey(r.TypeId) // find parameter type
			if !ok {
				return errors.New("type " + strconv.Itoa(r.TypeId) + " not found for " + r.Name)
			}

			meta.Param = append(meta.Param, ParamMeta{ParamDicRow: r, typeOf: &meta.Type[k]})
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from parameter_dims join to model_parameter_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_parameter_id, D.dim_id, D.dim_name, T.model_type_id"+
			" FROM parameter_dims D"+
			" INNER JOIN model_parameter_dic M ON (M.parameter_hid = D.parameter_hid)"+
			" INNER JOIN model_type_dic T ON (T.type_hid = D.type_hid AND T.model_id = M.model_id)"+
			" WHERE M.model_id = "+smId+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r ParamDimsRow
			if err := rows.Scan(
				&r.ModelId, &r.ParamId, &r.DimId, &r.Name, &r.TypeId); err != nil {
				return err
			}

			idx, ok := meta.ParamByKey(r.ParamId) // find parameter row for that dimension
			if !ok {
				return errors.New("parameter " + strconv.Itoa(r.ParamId) + " not found for " + r.Name)
			}
			k, ok := meta.TypeByKey(r.TypeId) // find parameter type
			if !ok {
				return errors.New("type " + strconv.Itoa(r.TypeId) + " not found for " + r.Name)
			}
			r.typeOf = &meta.Type[k]
			r.sizeOf = len(r.typeOf.Enum)

			meta.Param[idx].Dim = append(meta.Param[idx].Dim, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from table_dic join to model_table_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_table_id, D.table_hid, D.table_name, D.table_digest,"+
			" D.db_expr_table, D.db_acc_table, D.table_rank, D.is_sparse,"+
			" M.is_user, M.expr_dim_pos"+
			" FROM table_dic D"+
			" INNER JOIN model_table_dic M ON (M.table_hid = D.table_hid)"+
			" WHERE M.model_id = "+smId+
			" ORDER BY 1, 2",
		func(rows *sql.Rows) error {
			var r TableDicRow
			nSparse := 0
			nUser := 0
			if err := rows.Scan(
				&r.ModelId, &r.TableId, &r.TableHid, &r.Name,
				&r.Digest, &r.DbExprTable, &r.DbAccTable, &r.Rank,
				&nSparse, &nUser, &r.ExprPos); err != nil {
				return err
			}
			r.IsSparse = nSparse != 0 // oracle: smallint is float64 (thank you, oracle)
			r.IsUser = nUser != 0     // oracle: smallint is float64 (for my job is security)

			meta.Table = append(meta.Table, TableMeta{TableDicRow: r})
			return nil
		})
	if err != nil {
		return nil, err
	}
	if len(meta.Table) <= 0 {
		return nil, errors.New("no model output tables found")
	}

	// select db rows from table_dims join to model_table_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_table_id, D.dim_id, D.dim_name, T.model_type_id, D.is_total, D.dim_size"+
			" FROM table_dims D"+
			" INNER JOIN model_table_dic M ON (M.table_hid = D.table_hid)"+
			" INNER JOIN model_type_dic T ON (T.type_hid = D.type_hid AND T.model_id = M.model_id)"+
			" WHERE M.model_id = "+smId+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r TableDimsRow
			nTotal := 0
			if err := rows.Scan(
				&r.ModelId, &r.TableId, &r.DimId, &r.Name, &r.TypeId, &nTotal, &r.DimSize); err != nil {
				return err
			}
			r.IsTotal = nTotal != 0 // oracle: smallint is float64

			idx, ok := meta.OutTableByKey(r.TableId) // find table row for that dimension
			if !ok {
				return errors.New("output table " + strconv.Itoa(r.TableId) + " not found for " + r.Name)
			}
			k, ok := meta.TypeByKey(r.TypeId) // find parameter type
			if !ok {
				return errors.New("type " + strconv.Itoa(r.TypeId) + " not found for " + r.Name)
			}
			r.typeOf = &meta.Type[k]

			meta.Table[idx].Dim = append(meta.Table[idx].Dim, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from table_acc join to model_table_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_table_id, D.acc_id, D.acc_name, D.acc_expr"+
			" FROM table_acc D"+
			" INNER JOIN model_table_dic M ON (M.table_hid = D.table_hid)"+
			" WHERE M.model_id = "+smId+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r TableAccRow
			if err := rows.Scan(
				&r.ModelId, &r.TableId, &r.AccId, &r.Name, &r.AccExpr); err != nil {
				return err
			}

			idx, ok := meta.OutTableByKey(r.TableId) // find table row for that accumulator
			if !ok {
				return errors.New("output table " + strconv.Itoa(r.TableId) + " not found for " + r.Name)
			}

			meta.Table[idx].Acc = append(meta.Table[idx].Acc, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from table_expr join to model_table_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_table_id, D.expr_id, D.expr_name, D.expr_decimals, D.expr_src, D.expr_sql"+
			" FROM table_expr D"+
			" INNER JOIN model_table_dic M ON (M.table_hid = D.table_hid)"+
			" WHERE M.model_id = "+smId+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r TableExprRow
			if err := rows.Scan(
				&r.ModelId, &r.TableId, &r.ExprId, &r.Name, &r.Decimals, &r.SrcExpr, &r.ExprSql); err != nil {
				return err
			}

			idx, ok := meta.OutTableByKey(r.TableId) // find table row for that expression
			if !ok {
				return errors.New("output table " + strconv.Itoa(r.TableId) + " not found for " + r.Name)
			}

			meta.Table[idx].Expr = append(meta.Table[idx].Expr, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// update internal members used to link arrays to each other and simplify search:
	// type indexes, dimension indexes, type size, parameter and output table size
	if err := meta.updateInternals(); err != nil {
		return nil, err
	}
	return meta, nil
}
