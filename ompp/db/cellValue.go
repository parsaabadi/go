// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"
)

// cellDims is dimensions of input parameter value, output table accumulator or expression.
type cellDims struct {
	DimIds []int // dimensions enum ids or int values if dimension type simple
}

// CellValue is dimensions and value of input parameter or output table.
type CellValue struct {
	cellDims             // dimensions
	Value    interface{} // value: int64, bool, float64 or string
}

// CsvConverter provide methods to convert parameters or output table data from or to row []string for csv file.
// Double format string is used for output bale values or if parameter type is float, double, long double.
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
// If parameter type is enum based then cell value is enum id and csv row value is enum code.
type CsvConverter interface {
	// return file name of csv file to store parameter or output table rows
	CsvFileName(modelDef *ModelMeta, name string) (string, error)

	// retrun first line of csv file with column names: expr_name,dim0,dim1,expr_value.
	// if isIdHeader is true: expr_id,dim0,dim1,expr_value
	// if isAllAcc is true: sub_id,dim0,dim1,acc0,acc1,acc2
	CsvHeader(modelDef *ModelMeta, name string, isIdHeader bool, valueName string) ([]string, error)

	// return converter from cell (dimensions and value) of parameter or output table to csv row []string.
	// it simply sprint() dimension id's and value into []string.
	CsvToIdRow(modelDef *ModelMeta, name string, doubleFmt string, valueName string) (
		func(interface{}, []string) error, error)

	// return converter from cell (dimensions and value) of parameter or output table to csv row []string.
	// it does convert from enum id to code for all dimensions and enum-based parameter value.
	CsvToRow(modelDef *ModelMeta, name string, doubleFmt string, valueName string) (
		func(interface{}, []string) error, error)

	// return converter from csv row []string to parameter cell (dimensions and value)
	CsvToCell(modelDef *ModelMeta, name string, valueName string) (
		func(row []string) (interface{}, error), error)
}

// CsvFileName return file name of csv file to store parameter rows
func (CellValue) CsvFileName(modelDef *ModelMeta, name string) (string, error) {

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

// CsvHeader retrun first line for csv file: column names, it's look like: dim0,dim1,param_value.
func (CellValue) CsvHeader(modelDef *ModelMeta, name string, isIdHeader bool, valueName string) ([]string, error) {

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

// CsvToIdRow return converter from parameter cell (dimensions and value) to csv row []string.
//
// Converter simply does Sprint() for each dimension item id and value.
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
func (CellValue) CsvToIdRow(
	modelDef *ModelMeta, name string, doubleFmt string, valueName string) (
	func(interface{}, []string) error, error) {

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

	// for float model types use format if specified
	isUseFmt := param.typeOf.IsFloat() && doubleFmt != ""

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellValue)
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
		if isUseFmt {
			row[n] = fmt.Sprintf(doubleFmt, cell.Value)
		} else {
			row[n] = fmt.Sprint(cell.Value)
		}
		return nil
	}

	return cvt, nil
}

// CsvToRow return converter from parameter cell (dimensions and value) to csv row []string.
//
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
// If parameter type is enum based then cell value is enum id and csv row value is enum code.
func (CellValue) CsvToRow(
	modelDef *ModelMeta, name string, doubleFmt string, valueName string) (
	func(interface{}, []string) error, error) {

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
	fd := make([]func(itemId int) (string, error), param.Rank)

	for k := 0; k < param.Rank; k++ {
		f, err := cvtItemIdToCode(name+"."+param.Dim[k].Name, param.Dim[k].typeOf, param.Dim[k].typeOf.Enum, false, 0)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// if parameter value type is float then use format, if not empty
	isUseFmt := param.typeOf.IsFloat() && doubleFmt != ""

	// if parameter value type is enum-based then convert from enum id to code
	isUseEnum := !param.typeOf.IsBuiltIn()
	var fv func(itemId int) (string, error)

	if isUseEnum {
		f, err := cvtItemIdToCode(name, param.typeOf, param.typeOf.Enum, false, 0)
		if err != nil {
			return nil, err
		}
		fv = f
	}

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellValue)
		if !ok {
			return errors.New("invalid type, expected: parameter cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+1 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+1))
		}

		// convert dimension item id to code
		for k, e := range cell.DimIds {
			v, err := fd[k](e)
			if err != nil {
				return err
			}
			row[k] = v
		}

		// convert cell value:
		// if float then use format, if enum then find code by id, default: Sprint(value)
		if isUseFmt {
			row[n] = fmt.Sprintf(doubleFmt, cell.Value)
		}
		if !isUseFmt && isUseEnum {

			// depending on sql + driver it can be different type
			var iv int
			switch e := cell.Value.(type) {
			case int64:
				iv = int(e)
			case uint64:
				iv = int(e)
			case int32:
				iv = int(e)
			case uint32:
				iv = int(e)
			case int16:
				iv = int(e)
			case uint16:
				iv = int(e)
			case int8:
				iv = int(e)
			case uint8:
				iv = int(e)
			case uint:
				iv = int(e)
			case float32: // oracle (very unlikely)
				iv = int(e)
			case float64: // oracle (often)
				iv = int(e)
			case int:
				iv = e
			default:
				return errors.New("invalid parameter value type, expected: integer enum")
			}

			v, err := fv(int(iv))
			if err != nil {
				return err
			}
			row[n] = v
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
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
// If parameter type is enum based then cell value is enum id and csv row value is enum code.
func (CellValue) CsvToCell(
	modelDef *ModelMeta, name string, valueName string) (
	func(row []string) (interface{}, error), error) {

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
	fd := make([]func(src string) (int, error), param.Rank)

	for k := 0; k < param.Rank; k++ {
		f, err := cvtItemCodeToId(name+"."+param.Dim[k].Name, param.Dim[k].typeOf, param.Dim[k].typeOf.Enum, false, 0)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// cell value converter: float, bool, string or integer by default
	var fc func(src string) (interface{}, error)
	var fe func(src string) (int, error)
	isEnum := !param.typeOf.IsBuiltIn()

	switch {
	case isEnum:
		f, err := cvtItemCodeToId(name, param.typeOf, param.typeOf.Enum, false, 0)
		if err != nil {
			return nil, err
		}
		fe = f
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
		cell := CellValue{cellDims: cellDims{DimIds: make([]int, param.Rank)}}

		n := len(cell.DimIds)
		if len(row) != n+1 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+1))
		}

		// convert dimensions: enum code to enum id or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := fd[k](row[k])
			if err != nil {
				return nil, err
			}
			cell.DimIds[k] = i
		}

		// value conversion
		var v interface{}
		var err error
		if isEnum {
			v, err = fe(row[n])
		} else {
			v, err = fc(row[n])
		}
		if err != nil {
			return nil, err
		}
		cell.Value = v

		return cell, nil
	}

	return cvt, nil
}

// cvtItemCodeToId return converter from dimension item code to id.
// It is also used for parameter values if parameter type is enum-based.
// If dimension is enum-based then from enum code to enum id or to the total enum id;
// If dimension is simple integer type then parse integer;
// If dimension is boolean then false=>0, true=>1
func cvtItemCodeToId(msgName string, typeOf *TypeMeta, enumArr []TypeEnumRow, isTotalEnabled bool, totalEnumId int,
) (
	func(src string) (int, error), error,
) {
	var cvt func(src string) (int, error)

	switch {
	case !typeOf.IsBuiltIn(): // enum dimension: find enum id by code

		cvt = func(src string) (int, error) {
			for j := range enumArr {
				if src == enumArr[j].Name {
					return enumArr[j].EnumId, nil
				}
			}
			if isTotalEnabled && src == totalEnumCode { // check is it total item
				return totalEnumId, nil
			}
			return 0, errors.New("invalid value: " + src + " of: " + msgName)
		}

	case typeOf.IsBool(): // boolean dimension: false=>0, true=>1

		cvt = func(src string) (int, error) {
			is, err := strconv.ParseBool(src)
			if err != nil {
				return 0, errors.New("invalid value: " + src + " of: " + msgName)
			}
			if is {
				return 1, nil
			}
			return 0, nil
		}

	case typeOf.IsInt(): // integer dimension

		cvt = func(src string) (int, error) {
			i, err := strconv.Atoi(src)
			if err != nil {
				return 0, errors.New("invalid value: " + src + " of: " + msgName)
			}
			return i, nil
		}

	default:
		return nil, errors.New("invalid (not supported) type: " + typeOf.Name + " of: " + msgName)
	}

	return cvt, nil
}

// cvtItemIdToCode return converter from dimension item id to code.
// It is also used for parameter values if parameter type is enum-based.
// If dimension is enum-based then from enum id to enum code or to the "all" total enum code;
// If dimension is simple integer type then use Itoa(integer id) as code;
// If dimension is boolean then 0=>false, (1 or -1)=>true else error
func cvtItemIdToCode(msgName string, typeOf *TypeMeta, enumArr []TypeEnumRow, isTotalEnabled bool, totalEnumId int,
) (
	func(itemId int) (string, error), error,
) {
	var cvt func(itemId int) (string, error)

	switch {
	case !typeOf.IsBuiltIn(): // enum dimension: find enum id by code

		cvt = func(itemId int) (string, error) {
			for j := range enumArr {
				if itemId == enumArr[j].EnumId {
					return enumArr[j].Name, nil
				}
			}
			if isTotalEnabled && itemId == totalEnumId { // check is it total item
				return totalEnumCode, nil
			}
			return "", errors.New("invalid value: " + strconv.Itoa(itemId) + " of: " + msgName)
		}

	case typeOf.IsBool(): // boolean dimension: 0=>false, (1 or -1)=>true else error

		cvt = func(itemId int) (string, error) {
			switch itemId {
			case 0:
				return "false", nil
			case 1, -1:
				return "true", nil
			}
			return "", errors.New("invalid value: " + strconv.Itoa(itemId) + " of: " + msgName)
		}

	case typeOf.IsInt(): // integer dimension

		cvt = func(itemId int) (string, error) {
			return strconv.Itoa(itemId), nil
		}

	default:
		return nil, errors.New("invalid (not supported) type: " + typeOf.Name + " of: " + msgName)
	}

	return cvt, nil
}
