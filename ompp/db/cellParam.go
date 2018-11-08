// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"
)

// CellParam is value of input parameter.
type CellParam struct {
	cellValue     // dimensions and value
	SubId     int // parameter subvalue id
}

// CellCodeParam is value of input parameter.
// Dimension(s) items are enum codes, not enum ids.
type CellCodeParam struct {
	cellCodeValue     // dimensions as enum codes and value
	SubId         int // parameter subvalue id
}

// CsvFileName return file name of csv file to store parameter rows
func (CellParam) CsvFileName(modelDef *ModelMeta, name string) (string, error) {

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

// CsvHeader retrun first line for csv file: column names, it's look like: sub_id,dim0,dim1,param_value.
func (CellParam) CsvHeader(modelDef *ModelMeta, name string, isIdHeader bool, valueName string) ([]string, error) {

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
	h := make([]string, param.Rank+2)

	h[0] = "sub_id"
	for k := range param.Dim {
		h[k+1] = param.Dim[k].Name
	}
	h[param.Rank+1] = "param_value"

	return h, nil
}

// CsvToIdRow return converter from parameter cell (sub id, dimensions, value) to csv row []string.
//
// Converter simply does Sprint() for each sub-value id, dimension item id and value.
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
func (CellParam) CsvToIdRow(
	modelDef *ModelMeta, name string, doubleFmt string, valueName string,
) (
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

		cell, ok := src.(CellParam)
		if !ok {
			return errors.New("invalid type, expected: parameter cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+2))
		}

		row[0] = fmt.Sprint(cell.SubId)

		for k, e := range cell.DimIds {
			row[k+1] = fmt.Sprint(e)
		}

		// use "null" string for db NULL values and format for model float types
		if cell.IsNull {
			row[n+1] = "null"
		} else {
			if isUseFmt {
				row[n+1] = fmt.Sprintf(doubleFmt, cell.Value)
			} else {
				row[n+1] = fmt.Sprint(cell.Value)
			}
		}
		return nil
	}
	return cvt, nil
}

// CsvToRow return converter from parameter cell (sub id, dimensions, value) to csv row []string.
//
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
// If parameter type is enum based then csv row value is enum code and cell value is enum id.
func (CellParam) CsvToRow(
	modelDef *ModelMeta, name string, doubleFmt string, valueName string,
) (
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
		f, err := cvtItemIdToCode(name+"."+param.Dim[k].Name, param.Dim[k].typeOf, false)
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
		f, err := cvtItemIdToCode(name, param.typeOf, false)
		if err != nil {
			return nil, err
		}
		fv = f
	}

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellParam)
		if !ok {
			return errors.New("invalid type, expected: parameter cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+2))
		}

		row[0] = fmt.Sprint(cell.SubId)

		// convert dimension item id to code
		for k, e := range cell.DimIds {
			v, err := fd[k](e)
			if err != nil {
				return err
			}
			row[k+1] = v
		}

		// convert cell value:
		// if float then use format, if enum then find code by id, default: Sprint(value)
		// use "null" string for db NULL values and format for model float types
		switch {
		case cell.IsNull:
			row[n+1] = "null"

		case isUseFmt:
			row[n+1] = fmt.Sprintf(doubleFmt, cell.Value)

		case isUseEnum:
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
			row[n+1] = v

		default:
			row[n+1] = fmt.Sprint(cell.Value)
		}

		return nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to parameter cell (sub id, dimensions, value).
//
// It does retrun error if len(row) not equal to number of fields in cell db-record.
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
// If parameter type is enum based then csv row value is enum code and cell value is enum id.
func (CellParam) CsvToCell(
	modelDef *ModelMeta, name string, subCount int, valueName string,
) (
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
		f, err := cvtItemCodeToId(name+"."+param.Dim[k].Name, param.Dim[k].typeOf, false)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// cell value converter: float, bool, string or integer by default
	var fc func(src string) (interface{}, error)
	var fe func(src string) (int, error)
	var ff func(src string) (bool, float64, error)
	isFloat := param.typeOf.IsFloat()
	isEnum := !param.typeOf.IsBuiltIn()
	isNullable := param.IsExtendable // only extended parameter value can be NULL

	switch {
	case isEnum:
		f, err := cvtItemCodeToId(name, param.typeOf, false)
		if err != nil {
			return nil, err
		}
		fe = f
	case isFloat:
		ff = func(src string) (bool, float64, error) {

			if src == "" || src == "null" {
				if isNullable {
					return true, 0.0, nil
				}
				// else parameter is not nullable
				return true, 0.0, errors.New("invalid parameter value, it cannot be NULL")
			}
			vf, e := strconv.ParseFloat(src, 64)
			if e != nil {
				return false, 0.0, e
			}
			return false, vf, nil
		}
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
		cell := CellParam{cellValue: cellValue{DimIds: make([]int, param.Rank)}}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+2))
		}

		// subvalue number
		nSub, err := strconv.Atoi(row[0])
		if err != nil {
			return nil, err
		}
		if nSub < 0 || nSub >= subCount {
			return nil, errors.New("invalid sub-value id: " + strconv.Itoa(nSub) + " parameter: " + name)
		}
		cell.SubId = nSub

		// convert dimensions: enum code to enum id or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := fd[k](row[k+1])
			if err != nil {
				return nil, err
			}
			cell.DimIds[k] = i
		}

		// value conversion, float value can be NULL
		var v interface{}
		var isNull bool
		switch {
		case isEnum:
			isNull = false
			v, err = fe(row[n+1])
		case isFloat:
			isNull, v, err = ff(row[n+1])
		default:
			isNull = false
			v, err = fc(row[n+1])
		}
		if err != nil {
			return nil, err
		}
		cell.IsNull = isNull
		cell.Value = v

		return cell, nil
	}

	return cvt, nil
}

// IdToCodeCell return converter from parameter cell of ids: (sub id, dimensions, value)
// to cell of codes: (sub id, dimensions as enum code, value)
//
// If dimension type is enum based then dimensions enum ids can be converted to enum code.
// If dimension type is simple (bool or int) then dimension value converted to string.
// If parameter type is enum based then cell value enum id converted to enum code.
func (CellParam) IdToCodeCell(modelDef *ModelMeta, name string,
) (
	func(interface{}) (interface{}, error), error) {

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
		f, err := cvtItemIdToCode(name+"."+param.Dim[k].Name, param.Dim[k].typeOf, false)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// if parameter value type is enum-based then convert from enum id to code
	isUseEnum := !param.typeOf.IsBuiltIn()
	var fv func(itemId int) (string, error)

	if isUseEnum {
		f, err := cvtItemIdToCode(name, param.typeOf, false)
		if err != nil {
			return nil, err
		}
		fv = f
	}

	// create cell converter
	cvt := func(src interface{}) (interface{}, error) {

		srcCell, ok := src.(CellParam)
		if !ok {
			return nil, errors.New("invalid type, expected: parameter cell (internal error)")
		}
		if len(srcCell.DimIds) != param.Rank {
			return nil, errors.New("invalid cell rank: " + strconv.Itoa(len(srcCell.DimIds)) + ", expected: " + strconv.Itoa(param.Rank))
		}

		dstCell := CellCodeParam{
			cellCodeValue: cellCodeValue{
				Dims:   make([]string, param.Rank),
				IsNull: srcCell.IsNull,
			},
			SubId: srcCell.SubId,
		}

		// convert dimension item id to code
		for k := range srcCell.DimIds {
			v, err := fd[k](srcCell.DimIds[k])
			if err != nil {
				return nil, err
			}
			dstCell.Dims[k] = v
		}

		// convert cell value:
		// if not enum then copy value else find code by id
		if !isUseEnum {
			dstCell.Value = srcCell.Value
		} else {

			// depending on sql + driver it can be different type
			var iv int
			switch e := srcCell.Value.(type) {
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
				return nil, errors.New("invalid parameter value type, expected: integer enum")
			}

			v, err := fv(int(iv)) // find value code by id
			if err != nil {
				return nil, err
			}
			dstCell.Value = v
		}

		return dstCell, nil // converted OK
	}

	return cvt, nil
}

// CodeToIdCell return converter from parameter cell of codes: (sub id, dimensions as enum code, value)
// to cell of ids: (sub id, dimensions, value)
//
// If dimension type is enum based then dimensions enum codes converted to enum ids.
// If dimension type is simple (bool or int) then dimension code converted from string to dimension type.
// If parameter type is enum based then cell value enum code converted to enum id.
func (CellCodeParam) CodeToIdCell(modelDef *ModelMeta, name string,
) (
	func(interface{}) (interface{}, error), error) {

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

	// for each dimension create converter from item code to id
	fd := make([]func(itemCode string) (int, error), param.Rank)

	for k := 0; k < param.Rank; k++ {
		f, err := cvtItemCodeToId(name+"."+param.Dim[k].Name, param.Dim[k].typeOf, false)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// if parameter value type is enum-based then convert from enum code to id
	isUseEnum := !param.typeOf.IsBuiltIn()
	var fv func(itemCode string) (int, error)

	if isUseEnum {
		f, err := cvtItemCodeToId(name, param.typeOf, false)
		if err != nil {
			return nil, err
		}
		fv = f
	}

	// create cell converter
	cvt := func(src interface{}) (interface{}, error) {

		srcCell, ok := src.(CellCodeParam)
		if !ok {
			return nil, errors.New("invalid type, expected: parameter code cell (internal error)")
		}
		if len(srcCell.Dims) != param.Rank {
			return nil, errors.New("invalid cell rank: " + strconv.Itoa(len(srcCell.Dims)) + ", expected: " + strconv.Itoa(param.Rank))
		}

		dstCell := CellParam{
			cellValue: cellValue{
				DimIds: make([]int, param.Rank),
				IsNull: srcCell.IsNull,
			},
			SubId: srcCell.SubId,
		}

		// convert dimension item code to id
		for k := range srcCell.Dims {
			v, err := fd[k](srcCell.Dims[k])
			if err != nil {
				return nil, err
			}
			dstCell.DimIds[k] = v
		}

		// convert cell value:
		// if not enum then copy value else find id by code
		if !isUseEnum {
			dstCell.Value = srcCell.Value
		} else {
			sv, ok := srcCell.Value.(string)
			if !ok {
				return nil, errors.New("invalid parameter value type, expected: string enum code")
			}
			v, err := fv(sv) // find value id by code
			if err != nil {
				return nil, err
			}
			dstCell.Value = v
		}

		return dstCell, nil // converted OK
	}

	return cvt, nil
}
