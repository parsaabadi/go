// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"strconv"
)

// cellValue is dimensions item as id and value of input parameter or output table.
type cellValue struct {
	DimIds []int       // dimensions enum ids or int values if dimension type simple
	IsNull bool        // if true then value is NULL
	Value  interface{} // value: int64, bool, float64 or string
}

// cellCodeValue is dimensions item as code and value of input parameter or output table.
// Value is enum code if parameter is enum-based.
type cellCodeValue struct {
	Dims   []string    // dimensions as enum code or string converted built-in type
	IsNull bool        // if true then value is NULL
	Value  interface{} // value: int64, bool, float64 or string
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
	CsvToCell(modelDef *ModelMeta, name string, subCount int, valueName string) (
		func(row []string) (interface{}, error), error)
}

// CellToCodeConverter provide methods to convert parameters or output table row from enum id to enum code.
// If dimension type is enum based then dimensions enum ids can be converted to enum code.
// If dimension type is simple (bool or int) then dimension value converted to string.
// If parameter type is enum based then cell value enum id converted to enum code.
type CellToCodeConverter interface {

	// IdToCodeCell return converter from id cell to code cell.
	// Cell is dimensions and value of parameter or output table.
	// It does convert from enum id to code for all dimensions and enum-based parameter value.
	IdToCodeCell(modelDef *ModelMeta, name string) (
		func(interface{}) (interface{}, error), error)
}

// CellToIdConverter provide methods to convert parameters or output table row from enum code to enum id.
// If dimension type is enum based then dimensions enum codes converted to enum ids.
// If dimension type is simple (bool or int) then dimension code converted from string to dimension type.
// If parameter type is enum based then cell value enum code converted to enum id.
type CellToIdConverter interface {

	// CodeToIdCell return converter from code cell to id cell.
	// Cell is dimensions and value of parameter or output table.
	// It does convert from enum code to id for all dimensions and enum-based parameter value.
	CodeToIdCell(modelDef *ModelMeta, name string) (
		func(interface{}) (interface{}, error), error)
}

// cvtItemCodeToId return converter from dimension item code to id.
// It is also used for parameter values if parameter type is enum-based.
// If dimension is enum-based then from enum code to enum id or to the total enum id;
// If dimension is simple integer type then parse integer;
// If dimension is boolean then false=>0, true=>1
func cvtItemCodeToId(msgName string, typeOf *TypeMeta, isTotalEnabled bool,
) (
	func(src string) (int, error), error,
) {
	var cvt func(src string) (int, error)

	switch {
	case !typeOf.IsBuiltIn(): // enum dimension: find enum id by code

		cvt = func(src string) (int, error) {
			for j := range typeOf.Enum {
				if src == typeOf.Enum[j].Name {
					return typeOf.Enum[j].EnumId, nil
				}
			}
			if isTotalEnabled && src == TotalEnumCode { // check is it total item
				return typeOf.TotalEnumId, nil
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
func cvtItemIdToCode(msgName string, typeOf *TypeMeta, isTotalEnabled bool,
) (
	func(itemId int) (string, error), error,
) {
	var cvt func(itemId int) (string, error)

	switch {
	case !typeOf.IsBuiltIn(): // enum dimension: find enum id by code

		cvt = func(itemId int) (string, error) {
			for j := range typeOf.Enum {
				if itemId == typeOf.Enum[j].EnumId {
					return typeOf.Enum[j].Name, nil
				}
			}
			if isTotalEnabled && itemId == typeOf.TotalEnumId { // check is it total item
				return TotalEnumCode, nil
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
