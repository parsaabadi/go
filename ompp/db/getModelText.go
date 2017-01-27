// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// GetModelTextById return model_dic_txt table rows by model id.
//
// If langCode not empty then only specified language selected else all languages.
func GetModelTextById(dbConn *sql.DB, modelId int, langCode string) ([]ModelTxtRow, error) {

	// select db rows from model_dic_txt
	txtLst := make([]ModelTxtRow, 0)

	q := "SELECT" +
		" M.model_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM model_dic_txt M" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE M.model_id = " + strconv.Itoa(modelId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2"

	err := SelectRows(dbConn, q,
		func(rows *sql.Rows) error {
			var r ModelTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			txtLst = append(txtLst, r)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return txtLst, nil
}

// GetModelText return model text metadata: description and notes.
// If langCode not empty then only specified language selected else all languages.
func GetModelText(dbConn *sql.DB, modelId int, langCode string) (*ModelTxtMeta, error) {

	// select model name and digest by id
	meta := &ModelTxtMeta{}

	err := SelectFirst(dbConn,
		"SELECT model_name, model_digest FROM model_dic WHERE model_id = "+strconv.Itoa(modelId),
		func(row *sql.Row) error {
			return row.Scan(&meta.ModelName, &meta.ModelDigest)
		})
	switch {
	case err == sql.ErrNoRows:
		return nil, errors.New("model not found, invalid model id: " + strconv.Itoa(modelId))
	case err != nil:
		return nil, err
	}

	// make where clause parts:
	// WHERE M.model_id = 1234 AND L.lang_code = 'EN'
	where := " WHERE M.model_id = " + strconv.Itoa(modelId)
	if langCode != "" {
		where += " AND L.lang_code = " + toQuoted(langCode)
	}

	// select db rows from model_dic_txt
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.lang_id, L.lang_code, M.descr, M.note"+
			" FROM model_dic_txt M"+
			" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)"+
			where+
			" ORDER BY 1, 2",
		func(rows *sql.Rows) error {
			var r ModelTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.ModelTxt = append(meta.ModelTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from type_dic_txt join to model_type_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_type_id, T.lang_id, L.lang_code, T.descr, T.note"+
			" FROM type_dic_txt T"+
			" INNER JOIN model_type_dic M ON (M.type_hid = T.type_hid)"+
			" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)"+
			where+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r TypeTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.TypeId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.TypeTxt = append(meta.TypeTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from type_enum_txt join to model_type_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_type_id, T.enum_id, T.lang_id, L.lang_code, T.descr, T.note"+
			" FROM type_enum_txt T"+
			" INNER JOIN model_type_dic M ON (M.type_hid = T.type_hid)"+
			" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)"+
			where+
			" ORDER BY 1, 2, 3, 4",
		func(rows *sql.Rows) error {
			var r TypeEnumTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.TypeId, &r.EnumId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.TypeEnumTxt = append(meta.TypeEnumTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from parameter_dic_txt join to model_parameter_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_parameter_id, T.lang_id, L.lang_code, T.descr, T.note"+
			" FROM parameter_dic_txt T"+
			" INNER JOIN model_parameter_dic M ON (M.parameter_hid = T.parameter_hid)"+
			" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)"+
			where+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r ParamTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.ParamId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.ParamTxt = append(meta.ParamTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from parameter_dims_txt join to model_parameter_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_parameter_id, T.dim_id, T.lang_id, L.lang_code, T.descr, T.note"+
			" FROM parameter_dims_txt T"+
			" INNER JOIN model_parameter_dic M ON (M.parameter_hid = T.parameter_hid)"+
			" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)"+
			where+
			" ORDER BY 1, 2, 3, 4",
		func(rows *sql.Rows) error {
			var r ParamDimsTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.ParamId, &r.DimId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.ParamDimsTxt = append(meta.ParamDimsTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from table_dic_txt join to model_table_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_table_id, T.lang_id, L.lang_code, T.descr, T.note, T.expr_descr, T.expr_note"+
			" FROM table_dic_txt T"+
			" INNER JOIN model_table_dic M ON (M.table_hid = T.table_hid)"+
			" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)"+
			where+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r TableTxtRow
			var lId int
			var note, exnote sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.TableId, &lId, &r.LangCode, &r.Descr, &note, &r.ExprDescr, &exnote); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			if exnote.Valid {
				r.ExprNote = exnote.String
			}
			meta.TableTxt = append(meta.TableTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from table_dims_txt join to model_table_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_table_id, T.dim_id, T.lang_id, L.lang_code, T.descr, T.note"+
			" FROM table_dims_txt T"+
			" INNER JOIN model_table_dic M ON (M.table_hid = T.table_hid)"+
			" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)"+
			where+
			" ORDER BY 1, 2, 3, 4",
		func(rows *sql.Rows) error {
			var r TableDimsTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.TableId, &r.DimId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.TableDimsTxt = append(meta.TableDimsTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from table_acc_txt join to model_table_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_table_id, T.acc_id, T.lang_id, L.lang_code, T.descr, T.note"+
			" FROM table_acc_txt T"+
			" INNER JOIN model_table_dic M ON (M.table_hid = T.table_hid)"+
			" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)"+
			where+
			" ORDER BY 1, 2, 3, 4",
		func(rows *sql.Rows) error {
			var r TableAccTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.TableId, &r.AccId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.TableAccTxt = append(meta.TableAccTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from table_expr_txt join to model_table_dic
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.model_table_id, T.expr_id, T.lang_id, L.lang_code, T.descr, T.note"+
			" FROM table_expr_txt T"+
			" INNER JOIN model_table_dic M ON (M.table_hid = T.table_hid)"+
			" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)"+
			where+
			" ORDER BY 1, 2, 3, 4",
		func(rows *sql.Rows) error {
			var r TableExprTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.TableId, &r.ExprId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.TableExprTxt = append(meta.TableExprTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return meta, nil
}
