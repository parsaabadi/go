// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// lineCsvConverter return csv file row []string or isEof = true
type lineCsvConverter func() (isEof bool, row []string, err error)

// write model metadata from database into text csv files
func dbToCsv(modelName string, modelDigest string, runOpts *config.RunOptions) error {

	// open source database connection and check is it valid
	cs, dn := db.IfEmptyMakeDefault(modelName, runOpts.String(dbConnStrArgKey), runOpts.String(dbDriverArgKey))
	srcDb, _, err := db.Open(cs, dn, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	nv, err := db.OpenmppSchemaVersion(srcDb)
	if err != nil || nv < db.MinSchemaVersion {
		return errors.New("invalid database, likely not an openM++ database")
	}

	// get model metadata
	modelDef, err := db.GetModel(srcDb, modelName, modelDigest)
	if err != nil {
		return err
	}
	modelName = modelDef.Model.Name // set model name: it can be empty and only model digest specified

	// create new output directory, use modelName subdirectory
	outDir := filepath.Join(runOpts.String(outputDirArgKey), modelName)
	err = os.MkdirAll(outDir, 0750)
	if err != nil {
		return err
	}

	// write model definition into csv files
	isWriteUtf8bom := runOpts.Bool(useUtf8CsvArgKey)

	if err = toModelCsv(srcDb, modelDef, outDir, isWriteUtf8bom); err != nil {
		return err
	}

	// write list of languages into csv file
	if err = toLanguageCsv(srcDb, outDir, isWriteUtf8bom); err != nil {
		return err
	}

	// write model language-specific strings into csv file
	if err = toModelWordCsv(srcDb, modelDef.Model.ModelId, outDir, isWriteUtf8bom); err != nil {
		return err
	}

	// write model text (description and notes) into csv file
	if err = toModelTextCsv(srcDb, modelDef.Model.ModelId, outDir, isWriteUtf8bom); err != nil {
		return err
	}

	// write model parameter and output table groups and groups text into csv file
	if err = toModelGroupCsv(srcDb, modelDef.Model.ModelId, outDir, isWriteUtf8bom); err != nil {
		return err
	}

	// write model profile into csv file
	if err = toModelProfileCsv(srcDb, modelName, outDir, isWriteUtf8bom); err != nil {
		return err
	}

	// write all model run data into csv files: parameters, output expressions and accumulators
	dblFmt := runOpts.String(doubleFormatArgKey)
	isIdCsv := runOpts.Bool(useIdCsvArgKey)

	if err = toRunListCsv(srcDb, modelDef, outDir, dblFmt, isIdCsv, isWriteUtf8bom); err != nil {
		return err
	}

	// write all readonly workset data into csv files: input parameters
	if err = toWorksetListCsv(srcDb, modelDef, outDir, dblFmt, isIdCsv, isWriteUtf8bom); err != nil {
		return err
	}

	// write all modeling tasks and task run history into csv files
	if err = toTaskListCsv(srcDb, modelDef.Model.ModelId, outDir, isWriteUtf8bom); err != nil {
		return err
	}

	// pack model metadata, run results and worksets into zip
	if runOpts.Bool(zipArgKey) {
		zipPath, err := helper.PackZip(outDir, "")
		if err != nil {
			return err
		}
		omppLog.Log("Packed ", zipPath)
	}

	return nil
}

// toModelCsv writes model metadata into csv files.
func toModelCsv(dbConn *sql.DB, modelDef *db.ModelMeta, outDir string, isWriteUtf8bom bool) error {

	// write model master row into csv
	row := make([]string, 7)
	row[0] = strconv.Itoa(modelDef.Model.ModelId)
	row[1] = modelDef.Model.Name
	row[2] = modelDef.Model.Digest
	row[3] = strconv.Itoa(modelDef.Model.Type)
	row[4] = modelDef.Model.Version
	row[5] = modelDef.Model.CreateDateTime
	row[6] = modelDef.Model.DefaultLangCode

	idx := 0
	err := toCsvFile(
		outDir,
		"model_dic.csv",
		isWriteUtf8bom,
		[]string{"model_id", "model_name", "model_digest", "model_type", "model_ver", "create_dt", "default_lang_code"},
		func() (bool, []string, error) {
			if idx == 0 { // only one model_dic row exist
				idx++
				return false, row, nil
			}
			return true, row, nil // end of model rows
		})
	if err != nil {
		return errors.New("failed to write model into csv " + err.Error())
	}

	// write type rows into csv
	row = make([]string, 7)
	row[0] = strconv.Itoa(modelDef.Model.ModelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"type_dic.csv",
		isWriteUtf8bom,
		[]string{"model_id", "model_type_id", "type_hid", "type_name", "type_digest", "dic_id", "total_enum_id"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(modelDef.Type) {
				row[1] = strconv.Itoa(modelDef.Type[idx].TypeId)
				row[2] = strconv.Itoa(modelDef.Type[idx].TypeHid)
				row[3] = modelDef.Type[idx].Name
				row[4] = modelDef.Type[idx].Digest
				row[5] = strconv.Itoa(modelDef.Type[idx].DicId)
				row[6] = strconv.Itoa(modelDef.Type[idx].TotalEnumId)
				idx++
				return false, row, nil
			}
			return true, row, nil // end of type rows
		})
	if err != nil {
		return errors.New("failed to write model types into csv " + err.Error())
	}

	// write type enum rows into csv
	row = make([]string, 4)
	row[0] = strconv.Itoa(modelDef.Model.ModelId)

	idx = 0
	j := 0
	err = toCsvFile(
		outDir,
		"type_enum_lst.csv",
		isWriteUtf8bom,
		[]string{"model_id", "model_type_id", "enum_id", "enum_name"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(modelDef.Type) { // end of type rows
				return true, row, nil
			}

			// if end of current type enums then find next type with enum list
			if j < 0 || j >= len(modelDef.Type[idx].Enum) {
				j = 0
				for {
					idx++
					if idx < 0 || idx >= len(modelDef.Type) { // end of type rows
						return true, row, nil
					}
					if len(modelDef.Type[idx].Enum) > 0 {
						break
					}
				}
			}

			// make type enum []string row
			row[1] = strconv.Itoa(modelDef.Type[idx].Enum[j].TypeId)
			row[2] = strconv.Itoa(modelDef.Type[idx].Enum[j].EnumId)
			row[3] = modelDef.Type[idx].Enum[j].Name
			j++
			return false, row, nil
		})
	if err != nil {
		return errors.New("failed to write model enums into csv " + err.Error())
	}

	// write parameter rows into csv
	row = make([]string, 11)
	row[0] = strconv.Itoa(modelDef.Model.ModelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"parameter_dic.csv",
		isWriteUtf8bom,
		[]string{
			"model_id", "model_parameter_id", "parameter_hid", "parameter_name",
			"parameter_digest", "db_run_table", "db_set_table", "parameter_rank",
			"model_type_id", "is_hidden", "num_cumulated"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(modelDef.Param) {
				row[1] = strconv.Itoa(modelDef.Param[idx].ParamId)
				row[2] = strconv.Itoa(modelDef.Param[idx].ParamHid)
				row[3] = modelDef.Param[idx].Name
				row[4] = modelDef.Param[idx].Digest
				row[5] = modelDef.Param[idx].DbRunTable
				row[6] = modelDef.Param[idx].DbSetTable
				row[7] = strconv.Itoa(modelDef.Param[idx].Rank)
				row[8] = strconv.Itoa(modelDef.Param[idx].TypeId)
				row[9] = strconv.FormatBool(modelDef.Param[idx].IsHidden)
				row[10] = strconv.Itoa(modelDef.Param[idx].NumCumulated)
				idx++
				return false, row, nil
			}
			return true, row, nil // end of parameter rows
		})
	if err != nil {
		return errors.New("failed to write parameters into csv " + err.Error())
	}

	// write parameter dimension rows into csv
	row = make([]string, 5)
	row[0] = strconv.Itoa(modelDef.Model.ModelId)

	idx = 0
	j = 0
	err = toCsvFile(
		outDir,
		"parameter_dims.csv",
		isWriteUtf8bom,
		[]string{"model_id", "model_parameter_id", "dim_id", "dim_name", "model_type_id"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(modelDef.Param) { // end of parameter rows
				return true, row, nil
			}

			// if end of current parameter dimensions then find next parameter with dimension list
			if j < 0 || j >= len(modelDef.Param[idx].Dim) {
				j = 0
				for {
					idx++
					if idx < 0 || idx >= len(modelDef.Param) { // end of parameter rows
						return true, row, nil
					}
					if len(modelDef.Param[idx].Dim) > 0 {
						break
					}
				}
			}

			// make parameter dimension []string row
			row[1] = strconv.Itoa(modelDef.Param[idx].Dim[j].ParamId)
			row[2] = strconv.Itoa(modelDef.Param[idx].Dim[j].DimId)
			row[3] = modelDef.Param[idx].Dim[j].Name
			row[4] = strconv.Itoa(modelDef.Param[idx].Dim[j].TypeId)
			j++
			return false, row, nil
		})
	if err != nil {
		return errors.New("failed to write parameter dimensions into csv " + err.Error())
	}

	// write output table rows into csv
	row = make([]string, 12)
	row[0] = strconv.Itoa(modelDef.Model.ModelId)

	idx = 0
	err = toCsvFile(
		outDir,
		"table_dic.csv",
		isWriteUtf8bom,
		[]string{
			"model_id", "model_table_id", "table_hid", "table_name",
			"table_digest", "is_user", "table_rank", "is_sparse",
			"db_expr_table", "db_acc_table", "db_acc_table", "expr_dim_pos"},
		func() (bool, []string, error) {
			if 0 <= idx && idx < len(modelDef.Table) {
				row[1] = strconv.Itoa(modelDef.Table[idx].TableId)
				row[2] = strconv.Itoa(modelDef.Table[idx].TableHid)
				row[3] = modelDef.Table[idx].Name
				row[4] = modelDef.Table[idx].Digest
				row[5] = strconv.FormatBool(modelDef.Table[idx].IsUser)
				row[6] = strconv.Itoa(modelDef.Table[idx].Rank)
				row[7] = strconv.FormatBool(modelDef.Table[idx].IsSparse)
				row[8] = modelDef.Table[idx].DbExprTable
				row[9] = modelDef.Table[idx].DbAccTable
				row[10] = modelDef.Table[idx].DbAccAllView
				row[11] = strconv.Itoa(modelDef.Table[idx].ExprPos)
				idx++
				return false, row, nil
			}
			return true, row, nil // end of output table rows
		})
	if err != nil {
		return errors.New("failed to write output tables into csv " + err.Error())
	}

	// write output tables dimension rows into csv
	row = make([]string, 7)
	row[0] = strconv.Itoa(modelDef.Model.ModelId)

	idx = 0
	j = 0
	err = toCsvFile(
		outDir,
		"table_dims.csv",
		isWriteUtf8bom,
		[]string{"model_id", "model_table_id", "dim_id", "dim_name", "model_type_id", "is_total", "dim_size"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(modelDef.Table) { // end of output tables rows
				return true, row, nil
			}

			// if end of current output tables dimensions then find next output table with dimension list
			if j < 0 || j >= len(modelDef.Table[idx].Dim) {
				j = 0
				for {
					idx++
					if idx < 0 || idx >= len(modelDef.Table) { // end of output tables rows
						return true, row, nil
					}
					if len(modelDef.Table[idx].Dim) > 0 {
						break
					}
				}
			}

			// make output table dimension []string row
			row[1] = strconv.Itoa(modelDef.Table[idx].Dim[j].TableId)
			row[2] = strconv.Itoa(modelDef.Table[idx].Dim[j].DimId)
			row[3] = modelDef.Table[idx].Dim[j].Name
			row[4] = strconv.Itoa(modelDef.Table[idx].Dim[j].TypeId)
			row[5] = strconv.FormatBool(modelDef.Table[idx].Dim[j].IsTotal)
			row[6] = strconv.Itoa(modelDef.Table[idx].Dim[j].DimSize)
			j++
			return false, row, nil
		})
	if err != nil {
		return errors.New("failed to write output table dimensions into csv " + err.Error())
	}

	// write output tables accumulator rows into csv
	row = make([]string, 6)
	row[0] = strconv.Itoa(modelDef.Model.ModelId)

	idx = 0
	j = 0
	err = toCsvFile(
		outDir,
		"table_acc.csv",
		isWriteUtf8bom,
		[]string{"model_id", "model_table_id", "acc_id", "acc_name", "is_derived", "acc_src"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(modelDef.Table) { // end of output table rows
				return true, row, nil
			}

			// if end of current output tables accumulators then find next output table with accumulator list
			if j < 0 || j >= len(modelDef.Table[idx].Acc) {
				j = 0
				for {
					idx++
					if idx < 0 || idx >= len(modelDef.Table) { // end of output table rows
						return true, row, nil
					}
					if len(modelDef.Table[idx].Acc) > 0 {
						break
					}
				}
			}

			// make output table accumulator []string row
			row[1] = strconv.Itoa(modelDef.Table[idx].Acc[j].TableId)
			row[2] = strconv.Itoa(modelDef.Table[idx].Acc[j].AccId)
			row[3] = modelDef.Table[idx].Acc[j].Name
			row[4] = strconv.FormatBool(modelDef.Table[idx].Acc[j].IsDerived)
			row[5] = modelDef.Table[idx].Acc[j].SrcAcc
			j++
			return false, row, nil
		})
	if err != nil {
		return errors.New("failed to write output table accumulators into csv " + err.Error())
	}

	// write output tables expression rows into csv
	row = make([]string, 6)
	row[0] = strconv.Itoa(modelDef.Model.ModelId)

	idx = 0
	j = 0
	err = toCsvFile(
		outDir,
		"table_expr.csv",
		isWriteUtf8bom,
		[]string{"model_id", "model_table_id", "expr_id", "expr_name", "expr_decimals", "expr_src"},
		func() (bool, []string, error) {

			if idx < 0 || idx >= len(modelDef.Table) { // end of output table rows
				return true, row, nil
			}

			// if end of current output tables expressions then find next output table with expression list
			if j < 0 || j >= len(modelDef.Table[idx].Expr) {
				j = 0
				for {
					idx++
					if idx < 0 || idx >= len(modelDef.Table) { // end of output table rows
						return true, row, nil
					}
					if len(modelDef.Table[idx].Expr) > 0 {
						break
					}
				}
			}

			// make output table expression []string row
			row[1] = strconv.Itoa(modelDef.Table[idx].Expr[j].TableId)
			row[2] = strconv.Itoa(modelDef.Table[idx].Expr[j].ExprId)
			row[3] = modelDef.Table[idx].Expr[j].Name
			row[4] = strconv.Itoa(modelDef.Table[idx].Expr[j].Decimals)
			row[5] = modelDef.Table[idx].Expr[j].SrcExpr
			j++
			return false, row, nil
		})
	if err != nil {
		return errors.New("failed to write output table expressions into csv " + err.Error())
	}

	return nil
}

// toCsvFile write into csvDir/fileName.csv file.
func toCsvFile(
	csvDir string, fileName string, isWriteUtf8bom bool, columnNames []string, lineCvt lineCsvConverter) error {

	// create csv file
	f, err := os.OpenFile(filepath.Join(csvDir, fileName), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if isWriteUtf8bom { // if required then write utf-8 bom
		if _, err = f.Write(helper.Utf8bom); err != nil {
			return err
		}
	}

	wr := csv.NewWriter(f)

	// write header line: column names, if provided
	if len(columnNames) > 0 {
		if err = wr.Write(columnNames); err != nil {
			return err
		}
	}

	// write csv lines until eof
	for {
		isEof, row, err := lineCvt()
		if err != nil {
			return err
		}
		if isEof {
			break
		}
		if err = wr.Write(row); err != nil {
			return err
		}
	}

	// flush and return error, if any
	wr.Flush()
	return wr.Error()
}
