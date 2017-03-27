// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"strconv"
)

// cellDims is dimensions of input parameter value, output table accumulator or expression.
type cellDims struct {
	DimIds []int // dimensions enum ids or int values if dimension type simple
}

// CellValue is dimensions and value of input parameter or output table.
type cellValue struct {
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
	CsvToCell(modelDef *ModelMeta, name string, subCount int, valueName string) (
		func(row []string) (interface{}, error), error)
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
