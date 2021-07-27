// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// GetLanguages return language rows from lang_lst join to lang_word tables and map from lang_code to lang_id.
func GetLanguages(dbConn *sql.DB) (*LangMeta, error) {

	// select lang_lst rows, build index maps
	langDef := LangMeta{idIndex: make(map[int]int), codeIndex: make(map[string]int)}

	err := SelectRows(dbConn, "SELECT lang_id, lang_code, lang_name FROM lang_lst ORDER BY 1",
		func(rows *sql.Rows) error {
			var r LangLstRow
			if err := rows.Scan(&r.langId, &r.LangCode, &r.Name); err != nil {
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

// GetModelWord return model "words": language-specific stirngs.
// If langCode not empty then only specified language selected else all languages.
func GetModelWord(dbConn *sql.DB, modelId int, langCode string) (*ModelWordMeta, error) {

	// select model name and digest by id
	meta := &ModelWordMeta{}

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
		where += " AND L.lang_code = " + ToQuoted(langCode)
	}

	// select db rows from model_word
	err = SelectRows(dbConn,
		"SELECT"+
			" M.model_id, M.lang_id, L.lang_code, M.word_code, M.word_value"+
			" FROM model_word M"+
			" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)"+
			where+
			" ORDER BY 1, 2, 4",
		func(rows *sql.Rows) error {

			var mId, lId int
			var lCode, wCode, wVal string
			var srcVal sql.NullString
			if err := rows.Scan(&mId, &lId, &lCode, &wCode, &srcVal); err != nil {
				return err
			}
			if srcVal.Valid {
				wVal = srcVal.String
			}

			for k := range meta.ModelWord {
				if meta.ModelWord[k].LangCode == lCode {
					meta.ModelWord[k].Words[wCode] = wVal // append word (code,value) to existing language
					return nil
				}
			}

			// no such language: append new language and append word (code,value) to that language
			idx := len(meta.ModelWord)
			meta.ModelWord = append(
				meta.ModelWord, modelLangWord{LangCode: lCode, Words: make(map[string]string)})
			meta.ModelWord[idx].Words[wCode] = wVal

			return nil
		})
	if err != nil {
		return nil, err
	}

	return meta, nil
}
