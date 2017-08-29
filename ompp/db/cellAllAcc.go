// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"
)

// CellAllAcc is value of multiple output table accumulators.
type CellAllAcc struct {
	DimIds []int     // dimensions item as enum ids or int values if dimension type simple
	SubId  int       // output table subvalue id
	IsNull []bool    // if true then value is NULL
	Value  []float64 // accumulator value(s)
}

// CellCodeAllAcc is value of multiple output table accumulators.
// Dimension(s) items are enum codes, not enum ids.
type CellCodeAllAcc struct {
	Dims   []string  // dimensions as enum codes or string of item if dimension type simple then
	SubId  int       // output table subvalue id
	IsNull []bool    // if true then value is NULL
	Value  []float64 // accumulator value(s)
}

// CsvFileName return file name of csv file to store all accumulators rows
func (CellAllAcc) CsvFileName(modelDef *ModelMeta, name string) (string, error) {

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

	return modelDef.Table[k].Name + ".acc-all.csv", nil
}

// CsvHeader retrun first line for csv file: column names.
// It is like: sub_id,dim0,dim1,acc0,acc1,acc2
// If valueName is "" empty then all accumulators use for csv else one
func (CellAllAcc) CsvHeader(modelDef *ModelMeta, name string, isIdHeader bool, valueName string) ([]string, error) {

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

	// make first line columns:
	// if accumulator name specified then only one column else all accumlators
	nAcc := 1
	if valueName == "" {
		nAcc = len(table.Acc)
	}
	h := make([]string, 1+table.Rank+nAcc)

	h[0] = "sub_id"
	for k := range table.Dim {
		h[k+1] = table.Dim[k].Name
	}
	if valueName != "" {
		h[table.Rank+1] = valueName
	} else {
		for k := range table.Acc {
			h[table.Rank+1+k] = table.Acc[k].Name
		}
	}

	return h, nil
}

// CsvToIdRow return converter from output table cell (sub_id, dimensions, acc0, acc1, acc2) to csv row []string.
//
// Converter simply does Sprint() for each dimension item id, subvalue number and value(s).
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
// If valueName is "" empty then all accumulators converted else one
func (CellAllAcc) CsvToIdRow(
	modelDef *ModelMeta, name string, doubleFmt string, valueName string) (
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

	// number of dimensions and number of accumulators to be converted
	nAcc := 1
	if valueName == "" {
		nAcc = len(table.Acc)
	}
	nRank := table.Rank

	// make converter
	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellAllAcc)
		if !ok {
			return errors.New("invalid type, expected: all accumulators cell (internal error)")
		}

		if len(row) != 1+nRank+nAcc || len(cell.DimIds) != nRank || len(cell.IsNull) != nAcc || len(cell.Value) != nAcc {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(1+nRank+nAcc))
		}

		row[0] = fmt.Sprint(cell.SubId)

		for k, e := range cell.DimIds {
			row[k+1] = fmt.Sprint(e)
		}

		// use "null" string for db NULL values and format for model float types
		for k := 0; k < nAcc; k++ {

			if cell.IsNull[k] {
				row[1+nRank+k] = "null"
			} else {
				if doubleFmt != "" {
					row[1+nRank+k] = fmt.Sprintf(doubleFmt, cell.Value[k])
				} else {
					row[1+nRank+k] = fmt.Sprint(cell.Value[k])
				}
			}
		}
		return nil
	}

	return cvt, nil
}

// CsvToRow return converter from output table cell (sub_id, dimensions, acc0, acc1, acc2
// to csv row []string (acc_name, sub_id, dimensions, value).
//
// Converter will retrun error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
// If valueName is "" empty then all accumulators converted else one
func (CellAllAcc) CsvToRow(
	modelDef *ModelMeta, name string, doubleFmt string, valueName string) (
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

	// number of dimensions and number of accumulators to be converted
	nAcc := 1
	if valueName == "" {
		nAcc = len(table.Acc)
	}
	nRank := table.Rank

	// for each dimension create converter from item id to code
	fd := make([]func(itemId int) (string, error), nRank)

	for k := 0; k < nRank; k++ {
		f, err := cvtItemIdToCode(name+"."+table.Dim[k].Name, table.Dim[k].typeOf, table.Dim[k].IsTotal)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellAllAcc)
		if !ok {
			return errors.New("invalid type, expected: output table accumulator cell (internal error)")
		}

		if len(row) != 1+nRank+nAcc || len(cell.DimIds) != nRank || len(cell.IsNull) != nAcc || len(cell.Value) != nAcc {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(1+nRank+nAcc))
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

		// use "null" string for db NULL values and format for model float types
		for k := 0; k < nAcc; k++ {

			if cell.IsNull[k] {
				row[1+nRank+k] = "null"
			} else {
				if doubleFmt != "" {
					row[1+nRank+k] = fmt.Sprintf(doubleFmt, cell.Value[k])
				} else {
					row[1+nRank+k] = fmt.Sprint(cell.Value[k])
				}
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
func (CellAllAcc) CsvToCell(
	modelDef *ModelMeta, name string, subCount int, valueName string) (
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

	// number of dimensions and number of accumulators to be converted
	nAcc := 1
	if valueName == "" {
		nAcc = len(table.Acc)
	}
	nRank := table.Rank

	// for each dimension create converter from item code to id
	fd := make([]func(src string) (int, error), nRank)

	for k := 0; k < nRank; k++ {
		f, err := cvtItemCodeToId(name+"."+table.Dim[k].Name, table.Dim[k].typeOf, table.Dim[k].IsTotal)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// do conversion
	cvt := func(row []string) (interface{}, error) {

		// make conversion buffer and check input csv row size
		cell := CellAllAcc{
			DimIds: make([]int, nRank),
			IsNull: make([]bool, nAcc),
			Value:  make([]float64, nAcc)}

		if len(row) != 1+nRank+nAcc {
			return nil, errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(1+nRank+nAcc))
		}

		// subvalue number
		nSub, err := strconv.Atoi(row[0])
		if err != nil {
			return nil, err
		}
		if nSub < 0 || nSub >= subCount {
			return nil, errors.New("invalid sub-value id: " + strconv.Itoa(nSub) + " output table: " + name)
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

		// value conversion
		for k := 0; k < nAcc; k++ {

			cell.IsNull[k] = row[1+nRank+k] == "" || row[1+nRank+k] == "null"

			if cell.IsNull[k] {
				cell.Value[k] = 0.0
			} else {
				v, err := strconv.ParseFloat(row[1+nRank+k], 64)
				if err != nil {
					return nil, err
				}
				cell.Value[k] = v
			}
		}
		return cell, nil
	}

	return cvt, nil
}

// IdToCodeCell return converter from output table cell of ids:  (sub_id, dimensions, acc0, acc1, acc2)
// to cell of codes: (sub_id, dimensions as enum code, acc0, acc1, acc2)
//
// If dimension type is enum based then dimensions enum ids can be converted to enum code.
// If dimension type is simple (bool or int) then dimension value converted to string.
func (CellAllAcc) IdToCodeCell(modelDef *ModelMeta, name string) (
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

		srcCell, ok := src.(CellAllAcc)
		if !ok {
			return nil, errors.New("invalid type, expected: output table accumulator cell (internal error)")
		}
		if len(srcCell.DimIds) != table.Rank {
			return nil, errors.New("invalid cell rank: " + strconv.Itoa(len(srcCell.DimIds)) + ", expected: " + strconv.Itoa(table.Rank))
		}

		dstCell := CellCodeAllAcc{
			Dims:   make([]string, table.Rank),
			SubId:  srcCell.SubId,
			IsNull: append([]bool{}, srcCell.IsNull...),
			Value:  append([]float64{}, srcCell.Value...),
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
