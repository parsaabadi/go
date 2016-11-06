// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"strconv"

	"go.openmpp.org/ompp/db"
)

// toModelTextCsv writes model text metadata (description and notes) into csv files.
func toModelTextCsv(dbConn *sql.DB, modelId int, outDir string) error {

	// get model text metadata in all languages
	modelTxt, err := db.GetModelText(dbConn, modelId, "")
	if err != nil {
		return err
	}

	// write model text (description and notes) rows into csv
	row := make([]string, 5)
	row[0] = strconv.Itoa(modelId)

	idx := 0
	err = toCsvFile(
		outDir,
		"model_dic_txt.csv",
		[]string{"model_id", "lang_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelTxt.ModelTxt) {
				row[1] = strconv.Itoa(modelTxt.ModelTxt[idx].LangId)
				row[2] = modelTxt.ModelTxt[idx].LangCode
				row[3] = modelTxt.ModelTxt[idx].Descr

				if modelTxt.ModelTxt[idx].Note == "" { // empty "" string is NULL
					row[4] = "NULL"
				} else {
					row[4] = modelTxt.ModelTxt[idx].Note
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of model text rows
		})
	if err != nil {
		return errors.New("failed to write model text into csv " + err.Error())
	}

	// write type text rows into csv
	row = make([]string, 6)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"type_dic_txt.csv",
		[]string{"model_id", "model_type_id", "lang_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelTxt.TypeTxt) {
				row[1] = strconv.Itoa(modelTxt.TypeTxt[idx].TypeId)
				row[2] = strconv.Itoa(modelTxt.TypeTxt[idx].LangId)
				row[3] = modelTxt.TypeTxt[idx].LangCode
				row[4] = modelTxt.TypeTxt[idx].Descr

				if modelTxt.TypeTxt[idx].Note == "" { // empty "" string is NULL
					row[5] = "NULL"
				} else {
					row[5] = modelTxt.TypeTxt[idx].Note
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of type text rows
		})
	if err != nil {
		return errors.New("failed to write type text into csv " + err.Error())
	}

	// write type enum text rows into csv
	row = make([]string, 7)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"type_enum_txt.csv",
		[]string{"model_id", "model_type_id", "enum_id", "lang_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelTxt.TypeEnumTxt) {
				row[1] = strconv.Itoa(modelTxt.TypeEnumTxt[idx].TypeId)
				row[2] = strconv.Itoa(modelTxt.TypeEnumTxt[idx].EnumId)
				row[3] = strconv.Itoa(modelTxt.TypeEnumTxt[idx].LangId)
				row[4] = modelTxt.TypeEnumTxt[idx].LangCode
				row[5] = modelTxt.TypeEnumTxt[idx].Descr

				if modelTxt.TypeEnumTxt[idx].Note == "" { // empty "" string is NULL
					row[6] = "NULL"
				} else {
					row[6] = modelTxt.TypeEnumTxt[idx].Note
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of type enum text rows
		})
	if err != nil {
		return errors.New("failed to write enum text into csv " + err.Error())
	}

	// write parameter text rows into csv
	row = make([]string, 6)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"parameter_dic_txt.csv",
		[]string{"model_id", "model_parameter_id", "lang_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelTxt.ParamTxt) {
				row[1] = strconv.Itoa(modelTxt.ParamTxt[idx].ParamId)
				row[2] = strconv.Itoa(modelTxt.ParamTxt[idx].LangId)
				row[3] = modelTxt.ParamTxt[idx].LangCode
				row[4] = modelTxt.ParamTxt[idx].Descr

				if modelTxt.ParamTxt[idx].Note == "" { // empty "" string is NULL
					row[5] = "NULL"
				} else {
					row[5] = modelTxt.ParamTxt[idx].Note
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of parameter text rows
		})
	if err != nil {
		return errors.New("failed to write model parameter text into csv " + err.Error())
	}

	// write parameter dimensions text rows into csv
	row = make([]string, 7)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"parameter_dims_txt.csv",
		[]string{"model_id", "model_parameter_id", "dim_id", "lang_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelTxt.ParamDimsTxt) {
				row[1] = strconv.Itoa(modelTxt.ParamDimsTxt[idx].ParamId)
				row[2] = strconv.Itoa(modelTxt.ParamDimsTxt[idx].DimId)
				row[3] = strconv.Itoa(modelTxt.ParamDimsTxt[idx].LangId)
				row[4] = modelTxt.ParamDimsTxt[idx].LangCode
				row[5] = modelTxt.ParamDimsTxt[idx].Descr

				if modelTxt.ParamDimsTxt[idx].Note == "" { // empty "" string is NULL
					row[6] = "NULL"
				} else {
					row[6] = modelTxt.ParamDimsTxt[idx].Note
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of parameter dimensions text rows
		})
	if err != nil {
		return errors.New("failed to write parameter dimensions text into csv " + err.Error())
	}

	// write output table text rows into csv
	row = make([]string, 8)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"table_dic_txt.csv",
		[]string{"model_id", "model_table_id", "lang_id", "lang_code", "descr", "note", "expr_descr", "expr_note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelTxt.TableTxt) {
				row[1] = strconv.Itoa(modelTxt.TableTxt[idx].TableId)
				row[2] = strconv.Itoa(modelTxt.TableTxt[idx].LangId)
				row[3] = modelTxt.TableTxt[idx].LangCode
				row[4] = modelTxt.TableTxt[idx].Descr

				if modelTxt.TableTxt[idx].Note == "" { // empty "" string is NULL
					row[5] = "NULL"
				} else {
					row[5] = modelTxt.TableTxt[idx].Note
				}

				row[6] = modelTxt.TableTxt[idx].ExprDescr

				if modelTxt.TableTxt[idx].ExprNote == "" { // empty "" string is NULL
					row[7] = "NULL"
				} else {
					row[7] = modelTxt.TableTxt[idx].ExprNote
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of output table text rows
		})
	if err != nil {
		return errors.New("failed to write output table text into csv " + err.Error())
	}

	// write output table dimension text rows into csv
	row = make([]string, 7)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"table_dims_txt.csv",
		[]string{"model_id", "model_table_id", "dim_id", "lang_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelTxt.TableDimsTxt) {
				row[1] = strconv.Itoa(modelTxt.TableDimsTxt[idx].TableId)
				row[2] = strconv.Itoa(modelTxt.TableDimsTxt[idx].DimId)
				row[3] = strconv.Itoa(modelTxt.TableDimsTxt[idx].LangId)
				row[4] = modelTxt.TableDimsTxt[idx].LangCode
				row[5] = modelTxt.TableDimsTxt[idx].Descr

				if modelTxt.TableDimsTxt[idx].Note == "" { // empty "" string is NULL
					row[6] = "NULL"
				} else {
					row[6] = modelTxt.TableDimsTxt[idx].Note
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of output table dimension text rows
		})
	if err != nil {
		return errors.New("failed to write output table dimensions text into csv " + err.Error())
	}

	// write output table accumulator text rows into csv
	row = make([]string, 7)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"table_acc_txt.csv",
		[]string{"model_id", "model_table_id", "acc_id", "lang_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelTxt.TableAccTxt) {
				row[1] = strconv.Itoa(modelTxt.TableAccTxt[idx].TableId)
				row[2] = strconv.Itoa(modelTxt.TableAccTxt[idx].AccId)
				row[3] = strconv.Itoa(modelTxt.TableAccTxt[idx].LangId)
				row[4] = modelTxt.TableAccTxt[idx].LangCode
				row[5] = modelTxt.TableAccTxt[idx].Descr

				if modelTxt.TableAccTxt[idx].Note == "" { // empty "" string is NULL
					row[6] = "NULL"
				} else {
					row[6] = modelTxt.TableAccTxt[idx].Note
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of output table accumulator text rows
		})
	if err != nil {
		return errors.New("failed to write output table accumulators text into csv " + err.Error())
	}

	// write output table expression text rows into csv
	row = make([]string, 7)
	row[0] = strconv.Itoa(modelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"table_expr_txt.csv",
		[]string{"model_id", "model_table_id", "expr_id", "lang_id", "lang_code", "descr", "note"},
		func() (bool, []string, error) {

			if 0 <= idx && idx < len(modelTxt.TableExprTxt) {
				row[1] = strconv.Itoa(modelTxt.TableExprTxt[idx].TableId)
				row[2] = strconv.Itoa(modelTxt.TableExprTxt[idx].ExprId)
				row[3] = strconv.Itoa(modelTxt.TableExprTxt[idx].LangId)
				row[4] = modelTxt.TableExprTxt[idx].LangCode
				row[5] = modelTxt.TableExprTxt[idx].Descr

				if modelTxt.TableExprTxt[idx].Note == "" { // empty "" string is NULL
					row[6] = "NULL"
				} else {
					row[6] = modelTxt.TableExprTxt[idx].Note
				}
				idx++
				return false, row, nil
			}
			return true, row, nil // end of output table expression text rows
		})
	if err != nil {
		return errors.New("failed to write output table expressions text into csv " + err.Error())
	}

	return nil
}
