// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// UpdateLanguage insert new or update existing language and words in lang_lst and lang_word tables.
// Language ids updated with actual id's from database
func UpdateLanguage(dbConn *sql.DB, langDef *LangList) error {

	// source is empty: nothing to do, exit
	if langDef == nil || len(langDef.LangWord) <= 0 {
		return nil
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}

	if err = doUpdateLanguage(trx, langDef); err != nil {
		trx.Rollback()
		return err
	}

	// rebuild language id index
	langDef.idIndex = make(map[int]int, len(langDef.LangWord))
	for k := range langDef.LangWord {
		langDef.idIndex[langDef.LangWord[k].LangId] = k
	}

	trx.Commit()
	return nil
}

// doUpdateLanguage insert new or update existing language and words in lang_lst and lang_word tables.
// It does update as part of transaction
// Language ids updated with actual id's from database
func doUpdateLanguage(trx *sql.Tx, langDef *LangList) error {

	// for each language:
	// if language code exist then update else insert into lang_lst
	for idx := range langDef.LangWord {

		// get new language id
		// UPDATE id_lst SET id_value =
		//   CASE
		//     WHEN 0 = (SELECT COUNT(*) FROM lang_lst WHERE lang_code = 'EN')
		//       THEN id_value + 1
		//     ELSE id_value
		//   END
		// WHERE id_key = 'lang_id'
		err := TrxUpdate(trx,
			"UPDATE id_lst SET id_value ="+
				" CASE"+
				" WHEN 0 = (SELECT COUNT(*) FROM lang_lst WHERE lang_code = "+toQuoted(langDef.LangWord[idx].LangCode)+")"+
				" THEN id_value + 1"+
				" ELSE id_value"+
				" END"+
				" WHERE id_key = 'lang_id'")
		if err != nil {
			return err
		}

		// check if this language already exist
		langDef.LangWord[idx].LangId = -1
		err = TrxSelectFirst(trx,
			"SELECT lang_id FROM lang_lst WHERE lang_code = "+toQuoted(langDef.LangWord[idx].LangCode),
			func(row *sql.Row) error {
				return row.Scan(&langDef.LangWord[idx].LangId)
			})
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		// if language exist then update else insert into lang_lst
		if langDef.LangWord[idx].LangId >= 0 {

			// UPDATE lang_lst SET lang_name = 'English' WHERE lang_id = 0
			err = TrxUpdate(trx,
				"UPDATE lang_lst"+
					" SET lang_name = "+toQuoted(langDef.LangWord[idx].Name)+
					" WHERE lang_id = "+strconv.Itoa(langDef.LangWord[idx].LangId))
			if err != nil {
				return err
			}

		} else { // insert into lang_lst

			// get new language id
			err = TrxSelectFirst(trx,
				"SELECT id_value FROM id_lst WHERE id_key = 'lang_id'",
				func(row *sql.Row) error {
					return row.Scan(&langDef.LangWord[idx].LangId)
				})
			switch {
			case err == sql.ErrNoRows:
				return errors.New("invalid destination database, likely not an openM++ database")
			case err != nil:
				return err
			}

			// INSERT INTO lang_lst (lang_id, lang_code, lang_name) VALUES (0, 'EN', 'English')
			err = TrxUpdate(trx,
				"INSERT INTO lang_lst (lang_id, lang_code, lang_name)"+
					" VALUES ("+
					strconv.Itoa(langDef.LangWord[idx].LangId)+", "+
					toQuoted(langDef.LangWord[idx].LangCode)+", "+
					toQuoted(langDef.LangWord[idx].Name)+")")
			if err != nil {
				return err
			}
		}

		// update lang_word for that language
		if err = doUpdateWord(trx, langDef.LangWord[idx].LangId, langDef.LangWord[idx].Word); err != nil {
			return err
		}
	}

	return nil
}

// doUpdateWord insert new or update existing language words in lang_word table.
// It does update as part of transaction
// Language id of wordRs[i].LangId are updated with langId, which expected to be actual id from database
func doUpdateWord(trx *sql.Tx, langId int, wordRs []WordRow) error {

	// source is empty: nothing to do, exit
	if len(wordRs) <= 0 {
		return nil
	}

	// for each word:
	// if language id and word code exist then update else insert into lang_word
	for idx := range wordRs {

		wordRs[idx].LangId = langId // set language id

		// check if this language word already exist
		// "SELECT COUNT(*) FROM lang_word WHERE lang_id = 0 AND word_code = 'EN'
		cnt := 0
		err := TrxSelectFirst(trx,
			"SELECT COUNT(*) FROM lang_word"+
				" WHERE lang_id = "+strconv.Itoa(wordRs[idx].LangId)+
				" AND word_code = "+toQuoted(wordRs[idx].WordCode),
			func(row *sql.Row) error {
				return row.Scan(&cnt)
			})
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		// if language word exist then update else insert into lang_word
		if cnt > 0 {

			// UPDATE lang_word SET word_value = 'Max' WHERE lang_id = 0 AND word_code = 'max'
			err = TrxUpdate(trx,
				"UPDATE lang_word"+
					" SET word_value = "+toQuoted(wordRs[idx].Value)+
					" WHERE lang_id = "+strconv.Itoa(wordRs[idx].LangId)+
					" AND word_code = "+toQuoted(wordRs[idx].WordCode))
			if err != nil {
				return err
			}

		} else { // insert into lang_word

			// INSERT INTO lang_word (lang_id, word_code, word_value) VALUES (0, 'Max', 'max')
			err = TrxUpdate(trx,
				"INSERT INTO lang_word (lang_id, word_code, word_value)"+
					" VALUES ("+
					strconv.Itoa(wordRs[idx].LangId)+", "+
					toQuoted(wordRs[idx].WordCode)+", "+
					toQuoted(wordRs[idx].Value)+")")
			if err != nil {
				return err
			}
		}
	}

	return nil
}
