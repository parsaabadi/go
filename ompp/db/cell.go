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
	// return file name of csv file to store parameter or output table rows
	CsvFileName(modelDef *ModelMeta, name string) (string, error)

	// retrun file name and first line for csv file: column names
	CsvHeader(modelDef *ModelMeta, name string) ([]string, error)

	// return converter from parameter cell (dimensions and value) to csv row []string
	CsvToRow(modelDef *ModelMeta, name string, doubleFmt string) (func(interface{}, []string) error, error)

	// return converter from csv row []string to parameter cell (dimensions and value)
	CsvToCell(modelDef *ModelMeta, name string) (func(row []string) (interface{}, error), error)
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

// CsvToRow return converter from parameter cell (dimensions and value) to csv row []string.
//
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
func (Cell) CsvToRow(modelDef *ModelMeta, name string, doubleFmt string) (func(interface{}, []string) error, error) {

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

	// for each dimension create converter from item id to code
	fd := make([]func(itemId int, theName string, dimName string, enumArr []TypeEnumRow) (string, error), param.Rank)

	for k := 0; k < param.Rank; k++ {
		f, err := cvtDimItemIdToCode(name, param.Dim[k].Name, param.Dim[k].typeOf)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// for float model types use format if specified
	isUseFmt := param.typeOf.IsFloat() && doubleFmt != ""

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(Cell)
		if !ok {
			return errors.New("invalid type, expected: parameter cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+1 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+1))
		}

		// convert dimension item id to code
		for k, e := range cell.DimIds {
			v, err := fd[k](e, name, param.Dim[k].Name, param.Dim[k].typeOf.Enum)
			if err != nil {
				return err
			}
			row[k] = v
		}

		if isUseFmt {
			row[n] = fmt.Sprintf(doubleFmt, cell.Value)
		} else {
			row[n] = fmt.Sprint(cell.Value)
		}
		return nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to parameter cell (dimensions and value).
//
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
	idx, ok := modelDef.ParamByName(name)
	if !ok {
		return nil, errors.New("parameter not found: " + name)
	}
	param := &modelDef.Param[idx]

	// for each dimension create converter from item code to id
	fd := make([]func(src string, theName string, dimName string, enumArr []TypeEnumRow) (int, error), param.Rank)

	for k := 0; k < param.Rank; k++ {
		f, err := cvtDimItemCodeToId(name, param.Dim[k].Name, param.Dim[k].typeOf)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

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
		return nil, errors.New("invalid (not supported) parameter type: " + name)
	}

	// do conversion
	cvt := func(row []string) (interface{}, error) {

		// make conversion buffer and check input csv row size
		cell := Cell{DimIds: make([]int, param.Rank)}

		n := len(cell.DimIds)
		if len(row) != n+1 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+1))
		}

		// convert dimensions: enum code to enum id or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := fd[k](row[k], name, param.Dim[k].Name, param.Dim[k].typeOf.Enum)
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

	h[0] = "expr_id"
	for k := range table.Dim {
		h[k+1] = table.Dim[k].Name
	}
	h[table.Rank+1] = "expr_value"

	return h, nil
}

// CsvToRow return converter from output table cell (expr_id, dimensions, value) to csv row []string.
//
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
func (CellExpr) CsvToRow(modelDef *ModelMeta, name string, doubleFmt string) (func(interface{}, []string) error, error) {

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

	// for each dimension create converter from item id to code
	fd := make([]func(itemId int, theName string, dimName string, enumArr []TypeEnumRow) (string, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := cvtDimItemIdToCode(name, table.Dim[k].Name, table.Dim[k].typeOf)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellExpr)
		if !ok {
			return errors.New("invalid type, expected: output table expression cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+2))
		}

		row[0] = table.Expr[cell.ExprId].Name

		// convert dimension item id to code
		for k, e := range cell.DimIds {
			v, err := fd[k](e, name, table.Dim[k].Name, table.Dim[k].typeOf.Enum)
			if err != nil {
				return err
			}
			row[k+1] = v
		}

		// use "null" string for db NULL values and format for model float types
		if cell.IsNull {
			row[n+1] = "null"
		} else {
			if doubleFmt != "" {
				row[n+1] = fmt.Sprintf(doubleFmt, cell.Value)
			} else {
				row[n+1] = fmt.Sprint(cell.Value)
			}
		}
		return nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to output table expression cell (dimensions and value).
//
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

	// for each dimension create converter from item code to id
	fd := make([]func(src string, theName string, dimName string, enumArr []TypeEnumRow) (int, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := cvtDimItemCodeToId(name, table.Dim[k].Name, table.Dim[k].typeOf)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// do conversion
	cvt := func(row []string) (interface{}, error) {

		// make conversion buffer and check input csv row size
		cell := CellExpr{Cell: Cell{DimIds: make([]int, table.Rank)}}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+2))
		}

		// expression id by name
		cell.ExprId = -1
		for k := range table.Expr {
			if row[0] == table.Expr[k].Name {
				cell.ExprId = k
				break
			}
		}
		if cell.ExprId < 0 {
			return nil, errors.New("invalid expression name: " + row[0] + " output table: " + name)
		}

		// convert dimensions: enum code to enum id or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := fd[k](row[k+1], name, table.Dim[k].Name, table.Dim[k].typeOf.Enum)
			if err != nil {
				return nil, err
			}
			cell.DimIds[k] = i
		}

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

	h[0] = "acc_id"
	h[1] = "sub_id"
	for k := range table.Dim {
		h[k+2] = table.Dim[k].Name
	}
	h[table.Rank+2] = "acc_value"

	return h, nil
}

// CsvToRow return converter from output table cell (acc_id, sub_id, dimensions, value) to csv row []string.
//
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
func (CellAcc) CsvToRow(modelDef *ModelMeta, name string, doubleFmt string) (func(interface{}, []string) error, error) {

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

	// for each dimension create converter from item id to code
	fd := make([]func(itemId int, theName string, dimName string, enumArr []TypeEnumRow) (string, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := cvtDimItemIdToCode(name, table.Dim[k].Name, table.Dim[k].typeOf)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellAcc)
		if !ok {
			return errors.New("invalid type, expected: output table accumulator cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+3 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+3))
		}

		row[0] = table.Acc[cell.AccId].Name
		row[1] = fmt.Sprint(cell.SubId)

		// convert dimension item id to code
		for k, e := range cell.DimIds {
			v, err := fd[k](e, name, table.Dim[k].Name, table.Dim[k].typeOf.Enum)
			if err != nil {
				return err
			}
			row[k+2] = v
		}

		// use "null" string for db NULL values and format for model float types
		if cell.IsNull {
			row[n+2] = "null"
		} else {
			if doubleFmt != "" {
				row[n+2] = fmt.Sprintf(doubleFmt, cell.Value)
			} else {
				row[n+2] = fmt.Sprint(cell.Value)
			}
		}
		return nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to output table accumulator cell (dimensions and value).
//
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

	// for each dimension create converter from item code to id
	fd := make([]func(src string, theName string, dimName string, enumArr []TypeEnumRow) (int, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := cvtDimItemCodeToId(name, table.Dim[k].Name, table.Dim[k].typeOf)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// do conversion
	cvt := func(row []string) (interface{}, error) {

		// make conversion buffer and check input csv row size
		cell := CellAcc{Cell: Cell{DimIds: make([]int, table.Rank)}}

		n := len(cell.DimIds)
		if len(row) != n+3 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+3))
		}

		// accumulator id by name
		cell.AccId = -1
		for k := range table.Acc {
			if row[0] == table.Acc[k].Name {
				cell.AccId = k
				break
			}
		}
		if cell.AccId < 0 {
			return nil, errors.New("invalid accumulator name: " + row[0] + " output table: " + name)
		}

		// subsample number
		i, err := strconv.Atoi(row[1])
		if err != nil {
			return nil, err
		}
		cell.SubId = i

		// convert dimensions: enum code to enum id or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := fd[k](row[k+2], name, table.Dim[k].Name, table.Dim[k].typeOf.Enum)
			if err != nil {
				return nil, err
			}
			cell.DimIds[k] = i
		}

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

// cvtDimItemCodeToId return converter from dimension item code to id.
// If dimension is enum-based then from enum string to enum id;
// If dimension is simple integer type then parse integer;
// If dimension is boolean then false=>0, true=>1
func cvtDimItemCodeToId(
	theName string, dimName string, typeOf *TypeMeta,
) (
	func(src string, theName string, dimName string, enumArr []TypeEnumRow) (int, error), error,
) {
	var cvt func(src string, theName string, dimName string, enumArr []TypeEnumRow) (int, error)

	switch {
	case !typeOf.IsBuiltIn(): // enum dimension: find enum id by code

		cvt = func(src string, theName string, dimName string, enumArr []TypeEnumRow) (int, error) {
			for j := range enumArr {
				if src == enumArr[j].Name {
					return enumArr[j].EnumId, nil
				}
			}
			return 0, errors.New("invalid value: " + src + " of: " + theName + " dimension: " + dimName)
		}

	case typeOf.IsBool(): // boolean dimension: false=>0, true=>1

		cvt = func(src string, theName string, dimName string, enumArr []TypeEnumRow) (int, error) {
			is, err := strconv.ParseBool(src)
			if err != nil {
				return 0, errors.New("invalid value: " + src + " of: " + theName + " dimension: " + dimName)
			}
			if is {
				return 1, nil
			}
			return 0, nil
		}

	case typeOf.IsInt(): // integer dimension

		cvt = func(src string, theName string, dimName string, enumArr []TypeEnumRow) (int, error) {
			i, err := strconv.Atoi(src)
			if err != nil {
				return 0, errors.New("invalid value: " + src + " of: " + theName + " dimension: " + dimName)
			}
			return i, nil
		}

	default:
		return nil, errors.New("invalid (not supported) dimension type: " + typeOf.Name + " of: " + theName + " dimension: " + dimName)
	}

	return cvt, nil
}

// cvtDimItemIdToCode return converter from dimension item id to code.
// If dimension is enum-based then from enum id to enum name;
// If dimension is simple integer type then use Itoa(integer id) as code;
// If dimension is boolean then 0=>false, (1 or -1)=>true else error
func cvtDimItemIdToCode(
	theName string, dimName string, typeOf *TypeMeta,
) (
	func(itemId int, theName string, dimName string, enumArr []TypeEnumRow) (string, error), error,
) {
	var cvt func(itemId int, theName string, dimName string, enumArr []TypeEnumRow) (string, error)

	switch {
	case !typeOf.IsBuiltIn(): // enum dimension: find enum id by code

		cvt = func(itemId int, theName string, dimName string, enumArr []TypeEnumRow) (string, error) {
			for j := range enumArr {
				if itemId == enumArr[j].EnumId {
					return enumArr[j].Name, nil
				}
			}
			return "", errors.New("invalid value: " + strconv.Itoa(itemId) + " of: " + theName + " dimension: " + dimName)
		}

	case typeOf.IsBool(): // boolean dimension: 0=>false, (1 or -1)=>true else error

		cvt = func(itemId int, theName string, dimName string, enumArr []TypeEnumRow) (string, error) {
			switch itemId {
			case 0:
				return "false", nil
			case 1, -1:
				return "true", nil
			}
			return "", errors.New("invalid value: " + strconv.Itoa(itemId) + " of: " + theName + " dimension: " + dimName)
		}

	case typeOf.IsInt(): // integer dimension

		cvt = func(itemId int, theName string, dimName string, enumArr []TypeEnumRow) (string, error) {
			return strconv.Itoa(itemId), nil
		}

	default:
		return nil, errors.New("invalid (not supported) dimension type: " + typeOf.Name + " of: " + theName + " dimension: " + dimName)
	}

	return cvt, nil
}
