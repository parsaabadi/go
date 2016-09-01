// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"
)

// Cell is value of input parameter or output table accumulator or expression.
type Cell struct {
	DimIds []int       // dimensions enum ids or int values if dimension type simple
	Value  interface{} // value: int64, bool, float64 or string
}

// CellExpr is value of output table expression.
type CellExpr struct {
	Cell        // dimension and value
	IsNull bool // if true then value is NULL
	ExprId int  // output table expression id
}

// CellAcc is value of output table accumulator.
type CellAcc struct {
	Cell        // dimension and value
	IsNull bool // if true then value is NULL
	AccId  int  // output table accumulator id
	SubId  int  // output table subsample id
}

// CsvConverter provide methods to convert parameters or output table data from or to row []string for csv file.
type CsvConverter interface {
	CsvFileName(modelDef *ModelMeta, name string) (string, error)                                // return file name of csv file to store parameter or output table rows
	CsvHeader(modelDef *ModelMeta, name string) ([]string, error)                                // retrun file name and first line for csv file: column names
	CsvToRow() (func(interface{}, []string) error, error)                                        // return converter from parameter cell (dimensions and value) to csv row []string
	CsvToCell(modelDef *ModelMeta, name string) (func(row []string) (interface{}, error), error) // return converter from csv row []string to parameter cell (dimensions and value)
}

// CsvFileName return file name of csv file to store parameter rows
func (Cell) CsvFileName(modelDef *ModelMeta, name string) (string, error) {

	// validate parameters
	if modelDef == nil {
		return "", errors.New("invalid (empty) model metadata, look like model not found")
	}
	if name == "" {
		return "", errors.New("invalid (empty) output table name")
	}

	// find parameter by name
	k, ok := modelDef.ParamByName(name)
	if !ok {
		return "", errors.New("parameter not found: " + name)
	}

	return modelDef.Param[k].Name + ".csv", nil
}

// CsvHeader retrun first line for csv file: column names
func (Cell) CsvHeader(modelDef *ModelMeta, name string) ([]string, error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if name == "" {
		return nil, errors.New("invalid (empty) parameter name")
	}

	// find parameter by name
	k, ok := modelDef.ParamByName(name)
	if !ok {
		return nil, errors.New("parameter not found: " + name)
	}
	param := &modelDef.Param[k]

	// make first line columns
	h := make([]string, param.Rank+1)

	for k := range param.Dim {
		h[k] = param.Dim[k].Name
	}
	h[param.Rank] = "param_value"

	return h, nil
}

// CsvToRow return converter from parameter cell (dimensions and value) to csv row []string
// Converter will retrun error if len(row) not equal to number of fields in csv record.
func (Cell) CsvToRow() (func(interface{}, []string) error, error) {

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(Cell)
		if !ok {
			return errors.New("invalid type, expected: parameter cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+1 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+1))
		}

		for k, e := range cell.DimIds {
			row[k] = fmt.Sprint(e)
		}
		row[n] = fmt.Sprint(cell.Value)
		return nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to parameter cell (dimensions and value).
// It does retrun error if len(row) not equal to number of fields in cell db-record.
func (Cell) CsvToCell(modelDef *ModelMeta, name string) (func(row []string) (interface{}, error), error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if name == "" {
		return nil, errors.New("invalid (empty) parameter name")
	}

	// find parameter by name
	k, ok := modelDef.ParamByName(name)
	if !ok {
		return nil, errors.New("parameter not found: " + name)
	}
	param := &modelDef.Param[k]

	// cell value converter: float, bool, string or integer by default
	var fc func(src string) (interface{}, error)

	switch {
	case param.typeOf.IsFloat():
		fc = func(src string) (interface{}, error) { return strconv.ParseFloat(src, 64) }
	case param.typeOf.IsBool():
		fc = func(src string) (interface{}, error) { return strconv.ParseBool(src) }
	case param.typeOf.IsString():
		fc = func(src string) (interface{}, error) { return src, nil }
	case param.typeOf.IsInt():
		fc = func(src string) (interface{}, error) { return strconv.Atoi(src) }
	default:
		return nil, errors.New("invalid (not supported) parameter type")
	}

	// do conversion
	cvt := func(row []string) (interface{}, error) {

		// make conversion buffer and check input csv row size
		cell := Cell{DimIds: make([]int, param.Rank)}

		n := len(cell.DimIds)
		if len(row) != n+1 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+1))
		}

		// dimensions: integer expected, enum ids or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := strconv.Atoi(row[k])
			if err != nil {
				return nil, err
			}
			cell.DimIds[k] = i
		}

		// value conversion
		v, err := fc(row[n])
		if err != nil {
			return nil, err
		}
		cell.Value = v

		return cell, nil
	}

	return cvt, nil
}

// CsvFileName return file name of csv file to store output table expression rows
func (CellExpr) CsvFileName(modelDef *ModelMeta, name string) (string, error) {

	// validate parameters
	if modelDef == nil {
		return "", errors.New("invalid (empty) model metadata, look like model not found")
	}
	if name == "" {
		return "", errors.New("invalid (empty) output table name")
	}

	// find output table by name
	k, ok := modelDef.OutTableByName(name)
	if !ok {
		return "", errors.New("output table not found: " + name)
	}

	return modelDef.Table[k].Name + ".csv", nil
}

// CsvHeader retrun first line for csv file: column names
func (CellExpr) CsvHeader(modelDef *ModelMeta, name string) ([]string, error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if name == "" {
		return nil, errors.New("invalid (empty) output table name")
	}

	// find output table by name
	k, ok := modelDef.OutTableByName(name)
	if !ok {
		return nil, errors.New("output table not found: " + name)
	}
	table := &modelDef.Table[k]

	// make first line columns
	h := make([]string, table.Rank+2)

	for k := range table.Dim {
		h[k] = table.Dim[k].Name
	}
	h[table.Rank] = "expr_id"
	h[table.Rank+1] = "expr_value"

	return h, nil
}

// CsvToRow return converter from output table cell (dimensions, expr_id, value) to csv row []string
// Converter will retrun error if len(row) not equal to number of fields in csv record.
func (CellExpr) CsvToRow() (func(interface{}, []string) error, error) {

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellExpr)
		if !ok {
			return errors.New("invalid type, expected: output table expression cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+2))
		}

		for k, e := range cell.DimIds {
			row[k] = fmt.Sprint(e)
		}

		row[n] = fmt.Sprint(cell.ExprId)

		if cell.IsNull {
			row[n+1] = ""
		} else {
			row[n+1] = fmt.Sprint(cell.Value)
		}
		return nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to output table expression cell (dimensions and value).
// It does retrun error if len(row) not equal to number of fields in cell db-record.
func (CellExpr) CsvToCell(modelDef *ModelMeta, name string) (func(row []string) (interface{}, error), error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if name == "" {
		return nil, errors.New("invalid (empty) output table name")
	}

	// find output table by name
	k, ok := modelDef.OutTableByName(name)
	if !ok {
		return nil, errors.New("output table not found: " + name)
	}
	table := &modelDef.Table[k]

	// do conversion
	cvt := func(row []string) (interface{}, error) {

		// make conversion buffer and check input csv row size
		cell := CellExpr{Cell: Cell{DimIds: make([]int, table.Rank)}}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+2))
		}

		// dimensions: integer expected, enum ids or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := strconv.Atoi(row[k])
			if err != nil {
				return nil, err
			}
			cell.DimIds[k] = i
		}

		// expression id
		i, err := strconv.Atoi(row[n])
		if err != nil {
			return nil, err
		}
		cell.ExprId = i

		// value conversion
		cell.IsNull = row[n+1] == "" || row[n+1] == "null"

		if cell.IsNull {
			cell.Value = 0.0
		} else {
			v, err := strconv.ParseFloat(row[n+1], 64)
			if err != nil {
				return nil, err
			}
			cell.Value = v
		}
		return cell, nil
	}

	return cvt, nil
}

// CsvFileName return file name of csv file to store output table accumulator rows
func (CellAcc) CsvFileName(modelDef *ModelMeta, name string) (string, error) {

	// validate parameters
	if modelDef == nil {
		return "", errors.New("invalid (empty) model metadata, look like model not found")
	}
	if name == "" {
		return "", errors.New("invalid (empty) output table name")
	}

	// find output table by name
	k, ok := modelDef.OutTableByName(name)
	if !ok {
		return "", errors.New("output table not found: " + name)
	}

	return modelDef.Table[k].Name + ".acc.csv", nil
}

// CsvHeader retrun first line for csv file: column names
func (CellAcc) CsvHeader(modelDef *ModelMeta, name string) ([]string, error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if name == "" {
		return nil, errors.New("invalid (empty) output table name")
	}

	// find output table by name
	k, ok := modelDef.OutTableByName(name)
	if !ok {
		return nil, errors.New("output table not found: " + name)
	}
	table := &modelDef.Table[k]

	// make first line columns
	h := make([]string, table.Rank+3)

	for k := range table.Dim {
		h[k] = table.Dim[k].Name
	}
	h[table.Rank] = "acc_id"
	h[table.Rank+1] = "sub_id"
	h[table.Rank+2] = "acc_value"

	return h, nil
}

// CsvToRow return converter from output table cell (dimensions, acc_id, sub_id, value) to csv row []string
// Converter will retrun error if len(row) not equal to number of fields in csv record.
func (CellAcc) CsvToRow() (func(interface{}, []string) error, error) {

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellAcc)
		if !ok {
			return errors.New("invalid type, expected: output table accumulator cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+3 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+3))
		}

		for k, e := range cell.DimIds {
			row[k] = fmt.Sprint(e)
		}

		row[n] = fmt.Sprint(cell.AccId)
		row[n+1] = fmt.Sprint(cell.SubId)

		if cell.IsNull {
			row[n+2] = ""
		} else {
			row[n+2] = fmt.Sprint(cell.Value)
		}
		return nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to output table accumulator cell (dimensions and value).
// It does retrun error if len(row) not equal to number of fields in cell db-record.
func (CellAcc) CsvToCell(modelDef *ModelMeta, name string) (func(row []string) (interface{}, error), error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if name == "" {
		return nil, errors.New("invalid (empty) output table name")
	}

	// find output table by name
	k, ok := modelDef.OutTableByName(name)
	if !ok {
		return nil, errors.New("output table not found: " + name)
	}
	table := &modelDef.Table[k]

	// do conversion
	cvt := func(row []string) (interface{}, error) {

		// make conversion buffer and check input csv row size
		cell := CellAcc{Cell: Cell{DimIds: make([]int, table.Rank)}}

		n := len(cell.DimIds)
		if len(row) != n+3 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+3))
		}

		// dimensions: integer expected, enum ids or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := strconv.Atoi(row[k])
			if err != nil {
				return nil, err
			}
			cell.DimIds[k] = i
		}

		// accumulator id and subsample number
		i, err := strconv.Atoi(row[n])
		if err != nil {
			return nil, err
		}
		cell.AccId = i

		i, err = strconv.Atoi(row[n+1])
		if err != nil {
			return nil, err
		}
		cell.SubId = i

		// value conversion
		cell.IsNull = row[n+2] == "" || row[n+2] == "null"

		if cell.IsNull {
			cell.Value = 0.0
		} else {
			v, err := strconv.ParseFloat(row[n+2], 64)
			if err != nil {
				return nil, err
			}
			cell.Value = v
		}
		return cell, nil
	}

	return cvt, nil
}
