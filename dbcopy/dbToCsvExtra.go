// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"strconv"

	"go.openmpp.org/ompp/db"
)

// toLanguageCsv writes list of languages into csv files.
func toLanguageCsv(dbConn *sql.DB, outDir string) error {

	// get list of languages
	langDef, err := db.GetLanguages(dbConn)
	if err != nil {
		return err
	}

	// write language rows into csv
	row := make([]string, 3)

	idx := 0
	err = toCsvFile(
		outDir,
		"lang_lst.csv",
		[]string{"lang_id", "lang_code", "lang_name"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(langDef.Lang) {
				row[0] = strconv.Itoa(langDef.Lang[idx].LangId)
				row[1] = langDef.Lang[idx].LangCode
				row[2] = langDef.Lang[idx].Name
				idx++
				return false, row, nil
			}
			return true, row, nil // end of language rows
		})
	if err != nil {
		return errors.New("failed to write languages into csv " + err.Error())
	}

	// write language word rows into csv
	row = make([]string, 3)

	idx = 0
	j := 0
	err = toCsvFile(
		outDir,
		"lang_word.csv",
		[]string{"lang_id", "word_code", "word_value"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(langDef.Lang) { // end of language rows
				return true, row, nil
			}

			// if end of current language words then find next language with word list
			if j < 0 || j >= len(langDef.Lang[idx].Word) {
				j = 0
				for {
					idx++
					if idx < 0 || idx >= len(langDef.Lang) { // end of language rows
						return true, row, nil
					}
					if len(langDef.Lang[idx].Word) > 0 {
						break
					}
				}
			}

			// make language word []string row
			row[0] = strconv.Itoa(langDef.Lang[idx].Word[j].LangId)
			row[1] = langDef.Lang[idx].Word[j].WordCode
			row[2] = langDef.Lang[idx].Word[j].Value
			j++
			return false, row, nil
		})
	if err != nil {
		return errors.New("failed to write language words into csv " + err.Error())
	}

	return nil
}

// toModelGroupCsv writes model parameter and output table groups into csv files.
func toModelGroupCsv(dbConn *sql.DB, modelId int, outDir string) error {

	// get model parameter and output table groups and groups text (description and notes) in all languages
	modelGroup, err := db.GetModelGroup(dbConn, modelId, "")
	if err != nil {
		return err
	}

	// write model group rows into csv
	row := make([]string, 5)
	row[0] = strconv.Itoa(modelId)

	idx := 0
	err = toCsvFile(
		outDir,
		"group_lst.csv",
		[]string{"model_id", "group_id", "is_parameter", "group_name", "is_hidden"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(modelGroup.GroupLst) {
				row[1] = strconv.Itoa(modelGroup.GroupLst[idx].GroupId)
				row[2] = strconv.FormatBool(modelGroup.GroupLst[idx].IsParam)
				row[3] = modelGroup.GroupLst[idx].Name
				row[4] = strconv.FormatBool(modelGroup.GroupLst[idx].IsHidden)
				idx++
				return false, row, nil
			}
			return true, row, nil // end of model group rows
		})
	if err != nil {
		return errors.New("failed to write model groups into csv " + err.Error())
	}

	// write group parent-child rows into csv
	row = make([]string, 5)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"group_pc.csv",
		[]string{"model_id", "group_id", "child_pos", "child_group_id", "leaf_id"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelGroup.GroupPc) {
				row[1] = strconv.Itoa(modelGroup.GroupPc[idx].GroupId)
				row[2] = strconv.Itoa(modelGroup.GroupPc[idx].ChildPos)

				if modelGroup.GroupPc[idx].ChildGroupId < 0 { // negative value is NULL
					row[3] = "NULL"
				} else {
					row[3] = strconv.Itoa(modelGroup.GroupPc[idx].ChildGroupId)
				}
				if modelGroup.GroupPc[idx].ChildLeafId < 0 { // negative value is NULL
					row[4] = "NULL"
				} else {
					row[4] = strconv.Itoa(modelGroup.GroupPc[idx].ChildLeafId)
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of group parent-child rows
		})
	if err != nil {
		return errors.New("failed to write group parent-child into csv " + err.Error())
	}

	// write group text rows into csv
	row = make([]string, 6)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"group_txt.csv",
		[]string{"model_id", "group_id", "lang_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelGroup.GroupTxt) {
				row[1] = strconv.Itoa(modelGroup.GroupTxt[idx].GroupId)
				row[2] = strconv.Itoa(modelGroup.GroupTxt[idx].LangId)
				row[3] = modelGroup.GroupTxt[idx].LangCode
				row[4] = modelGroup.GroupTxt[idx].Descr

				if modelGroup.GroupTxt[idx].Note == "" { // empty "" string is NULL
					row[5] = "NULL"
				} else {
					row[5] = modelGroup.GroupTxt[idx].Note
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of group text rows
		})
	if err != nil {
		return errors.New("failed to write group text into csv " + err.Error())
	}

	return nil
}

// toModelProfileCsv writes model profile into csv files.
func toModelProfileCsv(dbConn *sql.DB, modelName string, outDir string) error {

	// get model profile: default model profile is profile where name = model name
	modelProfile, err := db.GetProfile(dbConn, modelName)
	if err != nil {
		return err
	}

	// write profile options into csv
	row := make([]string, 3)
	row[0] = modelProfile.Name

	idx := 0
	err = toCsvFile(
		outDir,
		"profile_option.csv",
		[]string{"profile_name", "option_key", "option_value"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelProfile.Opts) { // if not all options done yet
				k := 0
				for key, val := range modelProfile.Opts { // loop over options (key,value)
					if k++; k < idx {
						continue // skip: that option already processed
					}
					row[1] = key
					row[2] = val
					idx++
					return false, row, nil
				}
			}

			return true, row, nil // end of profile rows
		})
	if err != nil {
		return errors.New("failed to write model profile into csv " + err.Error())
	}

	return nil
}
