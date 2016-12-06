// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

const MinSchemaVersion = 100 // min compatible db schema version

// OpenmppSchemaVersion return db schema: select id_value from id_lst where id_key = 'openmpp'
func OpenmppSchemaVersion(dbConn *sql.DB) (int, error) {

	var nVer int

	err := SelectFirst(dbConn,
		"SELECT id_value FROM id_lst WHERE id_key = 'openmpp'",
		func(row *sql.Row) error {
			return row.Scan(&nVer)
		})
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return -1, err
	}

	return nVer, nil
}

// IdByCode return language id by language code or first language if code not found
func (langDef *LangMeta) IdByCode(langCode string) int {
	return langDef.Lang[langDef.codeIndex[langCode]].LangId
}

// CodeIdId return language code by language id or first language if id not found
func (langDef *LangMeta) CodeById(langId int) string {
	return langDef.Lang[langDef.idIndex[langId]].LangCode
}

// DefaultLanguage return first model language: select min(lang_id) from model_dic_txt.
func DefaultLanguage(dbConn *sql.DB, modelId int) (*LangLstRow, error) {

	// get first language from model text
	var langRow LangLstRow
	isNoTxt := false

	err := SelectFirst(dbConn,
		"SELECT"+
			" L.lang_id, L.lang_code, lang_name FROM lang_lst L"+
			" WHERE L.lang_id ="+
			" (SELECT MIN(M.lang_id) FROM model_dic_txt M WHERE M.model_id = "+strconv.Itoa(modelId)+")",
		func(row *sql.Row) error {
			return row.Scan(&langRow.LangId, &langRow.LangCode, &langRow.Name)
		})
	switch {
	case err == sql.ErrNoRows:
		isNoTxt = true
	case err != nil:
		return nil, err
	}

	// if no model text found then select first from language list
	if isNoTxt {
		err = SelectFirst(dbConn,
			"SELECT"+
				" L.lang_id, L.lang_code, lang_name FROM lang_lst L"+
				" WHERE L.lang_id = (SELECT MIN(M.lang_id) FROM lang_lst M)",
			func(row *sql.Row) error {
				return row.Scan(&langRow.LangId, &langRow.LangCode, &langRow.Name)
			})
		switch {
		case err == sql.ErrNoRows:
			return nil, errors.New("invalid database: no language(s) found")
		case err != nil:
			return nil, err
		}
	}

	return &langRow, nil
}

// GetLanguages return language rows from lang_lst join to lang_word tables and map from lang_code to lang_id.
func GetLanguages(dbConn *sql.DB) (*LangMeta, error) {

	// select lang_lst rows, build index maps
	langDef := LangMeta{idIndex: make(map[int]int), codeIndex: make(map[string]int)}

	err := SelectRows(dbConn, "SELECT lang_id, lang_code, lang_name FROM lang_lst ORDER BY 1",
		func(rows *sql.Rows) error {
			var r LangLstRow
			if err := rows.Scan(&r.LangId, &r.LangCode, &r.Name); err != nil {
				return err
			}
			langDef.Lang = append(langDef.Lang, langWord{LangLstRow: r, Words: make(map[string]string)})
			return nil
		})
	if err != nil {
		return nil, err
	}
	if len(langDef.Lang) <= 0 {
		return nil, errors.New("invalid database: no language(s) found")
	}
	langDef.updateInternals() // update internal maps from id and code to index of language

	// select lang_word rows into (key, value) map for each language
	err = SelectRows(dbConn,
		"SELECT lang_id, word_code, word_value FROM lang_word ORDER BY 1, 2",
		func(rows *sql.Rows) error {

			var langId int
			var code, val string
			err := rows.Scan(&langId, &code, &val)

			if err == nil {
				if i, ok := langDef.idIndex[langId]; ok { // ignore if lang_id not exist, assume updated lang_lst between selects
					langDef.Lang[i].Words[code] = val
				}
			}
			return err
		})
	if err != nil {
		return nil, err
	}

	return &langDef, nil
}

// GetProfileList return profile names: profile_lst table rows.
//
// Profile is a named group of (key, value) options, similar to ini-file.
// Default model options has profile_name = model_name.
func GetProfileList(dbConn *sql.DB) ([]string, error) {

	var rs []string

	err := SelectRows(dbConn,
		"SELECT profile_name FROM profile_lst ORDER BY 1",
		func(rows *sql.Rows) error {
			var r string
			if err := rows.Scan(&r); err != nil {
				return err
			}
			rs = append(rs, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// GetRunOptions return run_option table rows as (key, value) map.
func GetRunOptions(dbConn *sql.DB, runId int) (map[string]string, error) {

	return getOpts(dbConn,
		"SELECT option_key, option_value FROM run_option WHERE run_id = "+strconv.Itoa(runId))
}

// GetProfile return profile_option table rows as (key, value) map.
//
// Profile is a named group of (key, value) options, similar to ini-file.
// Default model options has profile_name = model_name.
func GetProfile(dbConn *sql.DB, name string) (*ProfileMeta, error) {

	meta := ProfileMeta{Name: name}

	kv, err := getOpts(dbConn,
		"SELECT option_key, option_value FROM profile_option WHERE profile_name = "+toQuoted(name))
	if err != nil {
		return nil, err
	}
	meta.Opts = kv

	return &meta, nil
}

// getOpts return option table (profile_option or run_option) rows as (key, value) map.
func getOpts(dbConn *sql.DB, query string) (map[string]string, error) {

	kv := make(map[string]string)

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var key, val string
			if err := rows.Scan(&key, &val); err != nil {
				return err
			}
			kv[key] = val
			return nil
		})
	if err != nil {
		return nil, err
	}

	return kv, nil
}

// getRunOpts return run_option rows as map of maps: map(run_id, map(key, value)).
func getRunOpts(dbConn *sql.DB, query string) (map[int]map[string]string, error) {

	rkv := make(map[int]map[string]string)

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var runId int
			var key, val string
			if err := rows.Scan(&runId, &key, &val); err != nil {
				return err
			}
			if _, ok := rkv[runId]; !ok {
				rkv[runId] = make(map[string]string)
			}
			rkv[runId][key] = val
			return nil
		})
	if err != nil {
		return nil, err
	}

	return rkv, nil
}

// GetModelGroup return db rows of model parent-child groups of parameters and output tables.
//
// If langCode not empty then only specified language selected else all languages.
func GetModelGroup(dbConn *sql.DB, modelId int, langCode string) (*GroupMeta, error) {

	// select model name and digest by id
	meta := GroupMeta{}
	smId := strconv.Itoa(modelId)

	err := SelectFirst(dbConn,
		"SELECT model_name, model_digest FROM model_dic WHERE model_id = "+smId,
		func(row *sql.Row) error {
			return row.Scan(&meta.ModelName, &meta.ModelDigest)
		})
	switch {
	case err == sql.ErrNoRows:
		return nil, errors.New("model not found, invalid model id: " + smId)
	case err != nil:
		return nil, err
	}

	// select db rows from group_lst
	err = SelectRows(dbConn,
		"SELECT"+
			" model_id, group_id, is_parameter, group_name, is_hidden"+
			" FROM group_lst"+
			" WHERE model_id = "+smId+
			" ORDER BY 1, 2",
		func(rows *sql.Rows) error {
			var r GroupLstRow
			nParam := 0
			nHidden := 0
			if err := rows.Scan(
				&r.ModelId, &r.GroupId, &nParam, &r.Name, &nHidden); err != nil {
				return err
			}
			r.IsParam = nParam != 0   // oracle: smallint is float64
			r.IsHidden = nHidden != 0 // oracle: smallint is float64
			meta.GroupLst = append(meta.GroupLst, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from group_pc
	err = SelectRows(dbConn,
		"SELECT"+
			" model_id, group_id, child_pos, child_group_id, leaf_id"+
			" FROM group_pc"+
			" WHERE model_id = "+smId+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r GroupPcRow
			var cgId, leafId sql.NullInt64
			if err := rows.Scan(
				&r.ModelId, &r.GroupId, &r.ChildPos, &cgId, &leafId); err != nil {
				return err
			}
			if cgId.Valid {
				r.ChildGroupId = int(cgId.Int64)
			} else {
				r.ChildGroupId = -1
			}
			if leafId.Valid {
				r.ChildLeafId = int(leafId.Int64)
			} else {
				r.ChildLeafId = -1
			}

			meta.GroupPc = append(meta.GroupPc, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from group_txt
	q := "SELECT" +
		" T.model_id, T.group_id, T.lang_id, L.lang_code, T.descr, T.note" +
		" FROM group_txt T" +
		" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)" +
		" WHERE T.model_id = " + smId
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2, 3"

	err = SelectRows(dbConn, q,
		func(rows *sql.Rows) error {
			var r GroupTxtRow
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.GroupId, &r.LangId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.GroupTxt = append(meta.GroupTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return &meta, nil
}
