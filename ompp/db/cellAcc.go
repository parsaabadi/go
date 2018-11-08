// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"
)

// CellAcc is value of output table accumulator.
type CellAcc struct {
	cellValue     // dimensions and value
	AccId     int // output table accumulator id
	SubId     int // output table subvalue id
}

// CellCodeAcc is value of output table accumulator.
// Dimension(s) items are enum codes, not enum ids.
type CellCodeAcc struct {
	cellCodeValue     // dimensions as enum codes and value
	AccId         int // output table accumulator id
	SubId         int // output table subvalue id
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

// CsvHeader retrun first line for csv file: column names.
// It is like: acc_name,sub_id,dim0,dim1,acc_value
// or if isIdHeader is true: acc_id,sub_id,dim0,dim1,acc_value
func (CellAcc) CsvHeader(modelDef *ModelMeta, name string, isIdHeader bool, valueName string) ([]string, error) {

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

	if isIdHeader {
		h[0] = "acc_id"
	} else {
		h[0] = "acc_name"
	}

	h[1] = "sub_id"
	for k := range table.Dim {
		h[k+2] = table.Dim[k].Name
	}
	h[table.Rank+2] = "acc_value"

	return h, nil
}

// CsvToIdRow return converter from output table cell (acc_id, sub_id, dimensions, value) to csv row []string.
//
// Converter simply does Sprint() for each dimension item id, accumulator id, subvalue number and value.
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
func (CellAcc) CsvToIdRow(
	modelDef *ModelMeta, name string, doubleFmt string, valueName string,
) (
	func(interface{}, []string) error, error) {

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellAcc)
		if !ok {
			return errors.New("invalid type, expected: output table accumulator cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+3 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+3))
		}

		row[0] = fmt.Sprint(cell.AccId)
		row[1] = fmt.Sprint(cell.SubId)

		for k, e := range cell.DimIds {
			row[k+2] = fmt.Sprint(e)
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

// CsvToRow return converter from output table cell (acc_id, sub_id, dimensions, value)
// to csv row []string (acc_name, sub_id, dimensions, value).
//
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
func (CellAcc) CsvToRow(
	modelDef *ModelMeta, name string, doubleFmt string, valueName string,
) (
	func(interface{}, []string) error, error) {

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
	fd := make([]func(itemId int) (string, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := cvtItemIdToCode(name+"."+table.Dim[k].Name, table.Dim[k].typeOf, table.Dim[k].IsTotal)
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
			v, err := fd[k](e)
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
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
func (CellAcc) CsvToCell(
	modelDef *ModelMeta, name string, subCount int, valueName string,
) (
	func(row []string) (interface{}, error), error) {

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
	fd := make([]func(src string) (int, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := cvtItemCodeToId(name+"."+table.Dim[k].Name, table.Dim[k].typeOf, table.Dim[k].IsTotal)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// do conversion
	cvt := func(row []string) (interface{}, error) {

		// make conversion buffer and check input csv row size
		cell := CellAcc{cellValue: cellValue{DimIds: make([]int, table.Rank)}}

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

		// subvalue number
		nSub, err := strconv.Atoi(row[1])
		if err != nil {
			return nil, err
		}
		if nSub < 0 || nSub >= subCount {
			return nil, errors.New("invalid sub-value id: " + strconv.Itoa(nSub) + " output table: " + name)
		}
		cell.SubId = nSub

		// convert dimensions: enum code to enum id or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := fd[k](row[k+2])
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

// IdToCodeCell return converter from output table cell of ids: (acc_id, sub_id, dimensions, value)
// to cell of codes: (acc_id, sub_id, dimensions as enum code, value)
//
// If dimension type is enum based then dimensions enum ids can be converted to enum code.
// If dimension type is simple (bool or int) then dimension value converted to string.
func (CellAcc) IdToCodeCell(
	modelDef *ModelMeta, name string,
) (
	func(interface{}) (interface{}, error), error) {

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
	fd := make([]func(itemId int) (string, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := cvtItemIdToCode(name+"."+table.Dim[k].Name, table.Dim[k].typeOf, table.Dim[k].IsTotal)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// create cell converter
	cvt := func(src interface{}) (interface{}, error) {

		srcCell, ok := src.(CellAcc)
		if !ok {
			return nil, errors.New("invalid type, expected: output table accumulator id cell (internal error)")
		}
		if len(srcCell.DimIds) != table.Rank {
			return nil, errors.New("invalid cell rank: " + strconv.Itoa(len(srcCell.DimIds)) + ", expected: " + strconv.Itoa(table.Rank))
		}

		dstCell := CellCodeAcc{
			cellCodeValue: cellCodeValue{
				Dims:   make([]string, table.Rank),
				IsNull: srcCell.IsNull,
				Value:  srcCell.Value,
			},
			AccId: srcCell.AccId,
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

		return dstCell, nil // converted OK
	}

	return cvt, nil
}
