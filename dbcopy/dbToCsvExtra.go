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
				lId, _ := langDef.IdByCode(langDef.Lang[idx].LangCode)
				row[0] = strconv.Itoa(lId)
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

	// convert language words map to array of (id,key,value) rows
	var kvArr [][]string
	k := 0
	for j := range langDef.Lang {
		for key, val := range langDef.Lang[j].Words {
			kvArr = append(kvArr, make([]string, 3))
			lId, _ := langDef.IdByCode(langDef.Lang[j].LangCode)
			kvArr[k][0] = strconv.Itoa(lId)
			kvArr[k][1] = key
			kvArr[k][2] = val
			k++
		}
	}

	// write language word rows into csv
	row = make([]string, 3)

	idx = 0
	err = toCsvFile(
		outDir,
		"lang_word.csv",
		[]string{"lang_id", "word_code", "word_value"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(kvArr) {
				row = kvArr[idx]
				idx++
				return false, row, nil
			}
			return true, row, nil // end of language word rows
		})
	if err != nil {
		return errors.New("failed to write language words into csv " + err.Error())
	}

	return nil
}

// toModelWordCsv writes list of model language-specific strings into csv file.
func toModelWordCsv(dbConn *sql.DB, modelId int, outDir string) error {

	// get list of model words
	mwDef, err := db.GetModelWord(dbConn, modelId, "")
	if err != nil {
		return err
	}

	// convert model words map to array of rows
	var mwArr [][]string
	k := 0
	for j := range mwDef.ModelWord {
		for key, val := range mwDef.ModelWord[j].Words {

			mwArr = append(mwArr, make([]string, 4))
			mwArr[k][0] = strconv.Itoa(modelId)
			mwArr[k][1] = mwDef.ModelWord[j].LangCode
			mwArr[k][2] = key

			if val == "" { // empty "" string is NULL
				mwArr[k][3] = "NULL"
			} else {
				mwArr[k][3] = val
			}
			k++
		}
	}

	// write  model words rows into csv
	row := make([]string, 4)
	row[0] = strconv.Itoa(modelId)

	idx := 0
	err = toCsvFile(
		outDir,
		"model_word.csv",
		[]string{"model_id", "lang_code", "word_code", "word_value"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(mwArr) {
				row = mwArr[idx]
				idx++
				return false, row, nil
			}
			return true, row, nil // end of model word rows
		})
	if err != nil {
		return errors.New("failed to write model words into csv " + err.Error())
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
	row = make([]string, 5)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"group_txt.csv",
		[]string{"model_id", "group_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelGroup.GroupTxt) {
				row[1] = strconv.Itoa(modelGroup.GroupTxt[idx].GroupId)
				row[2] = modelGroup.GroupTxt[idx].LangCode
				row[3] = modelGroup.GroupTxt[idx].Descr

				if modelGroup.GroupTxt[idx].Note == "" { // empty "" string is NULL
					row[4] = "NULL"
				} else {
					row[4] = modelGroup.GroupTxt[idx].Note
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

	// convert options map to array of (key,value) rows
	kvArr := make([][2]string, len(modelProfile.Opts))
	k := 0
	for key, val := range modelProfile.Opts {
		kvArr[k][0] = key
		kvArr[k][1] = val
		k++
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
			if 0 <= idx && idx < len(kvArr) {
				row[1] = kvArr[idx][0]
				row[2] = kvArr[idx][1]
				idx++
				return false, row, nil
			}
			return true, row, nil // end of profile rows
		})
	if err != nil {
		return errors.New("failed to write model profile into csv " + err.Error())
	}

	return nil
}
