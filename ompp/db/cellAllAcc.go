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

// CellAllAccConverter is a converter for multiple output table accumulators to implement CsvConverter interface.
type CellAllAccConverter struct {
	CellTableConverter        // model metadata and output table name
	IsIdCsv            bool   // if true then use enum id's else use enum codes
	DoubleFmt          string // if not empty then format string is used to sprintf if value type is float, double, long double
	ValueName          string // If ValueName is "" empty then all accumulators use for csv else one
}

// retrun true if csv converter is using enum id's for dimensions
func (cellCvt CellAllAccConverter) IsUseEnumId() bool { return cellCvt.IsIdCsv }

// CsvFileName return file name of csv file to store all accumulators rows
func (cellCvt CellAllAccConverter) CsvFileName() (string, error) {

	// find output table by name
	_, err := cellCvt.tableByName()
	if err != nil {
		return "", err
	}

	// make csv file name
	if cellCvt.IsIdCsv {
		return cellCvt.TableName + ".id.acc-all.csv", nil
	}
	return cellCvt.TableName + ".acc-all.csv", nil
}

// CsvHeader return first line for csv file: column names.
// Column names can be like: sub_id,dim0,dim1,acc0,acc1,acc2
// If ValueName is "" empty then use all accumulators for csv else only one where accumulator name is ValueName
func (cellCvt CellAllAccConverter) CsvHeader() ([]string, error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return []string{}, err
	}

	// make first line columns:
	// if accumulator name specified then only one column else all accumlators
	nAcc := 1
	if cellCvt.ValueName == "" {
		nAcc = len(table.Acc)
	}
	h := make([]string, 1+table.Rank+nAcc)

	h[0] = "sub_id"
	for k := range table.Dim {
		h[k+1] = table.Dim[k].Name
	}
	if cellCvt.ValueName != "" {
		h[table.Rank+1] = cellCvt.ValueName
	} else {
		for k := range table.Acc {
			h[table.Rank+1+k] = table.Acc[k].Name
		}
	}

	return h, nil
}

// KeyIds return converter to copy primary key: (sub id, dimension ids) into key []int.
//
// Converter will return error if len(key) not equal to row key size.
func (cellCvt CellAllAccConverter) KeyIds(name string) (func(interface{}, []int) error, error) {

	cvt := func(src interface{}, key []int) error {

		cell, ok := src.(CellAllAcc)
		if !ok {
			return errors.New("invalid type, expected: CellAllAcc (internal error): " + name)
		}

		n := len(cell.DimIds)
		if len(key) != n+1 {
			return errors.New("invalid size of key buffer, expected: " + strconv.Itoa(n+1) + ": " + name)
		}

		key[0] = cell.SubId

		for k, e := range cell.DimIds {
			key[k+1] = e
		}
		return nil
	}

	return cvt, nil
}

// ToCsvIdRow return converter from output table cell (sub_id, dimensions, acc0, acc1, acc2) to csv row []string.
//
// Converter simply does Sprint() for each dimension item id, subvalue number and value(s).
// Converter will return error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
// If ValueName is "" empty then all accumulators converted else one
func (cellCvt CellAllAccConverter) ToCsvIdRow() (func(interface{}, []string) error, error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return nil, err
	}

	// number of dimensions and number of accumulators to be converted
	nAcc := 1
	if cellCvt.ValueName == "" {
		nAcc = len(table.Acc)
	}
	nRank := table.Rank

	// make converter
	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellAllAcc)
		if !ok {
			return errors.New("invalid type, expected: CellAllAcc (internal error): " + cellCvt.TableName)
		}

		if len(row) != 1+nRank+nAcc || len(cell.DimIds) != nRank || len(cell.IsNull) != nAcc || len(cell.Value) != nAcc {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(1+nRank+nAcc) + ": " + cellCvt.TableName)
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
				if cellCvt.DoubleFmt != "" {
					row[1+nRank+k] = fmt.Sprintf(cellCvt.DoubleFmt, cell.Value[k])
				} else {
					row[1+nRank+k] = fmt.Sprint(cell.Value[k])
				}
			}
		}
		return nil
	}

	return cvt, nil
}

// ToCsvRow return converter from output table cell (sub_id, dimensions, acc0, acc1, acc2
// to csv row []string (acc_name, sub_id, dimensions, value).
//
// Converter will return error if len(row) not equal to number of fields in csv record.
// Double format string is used if parameter type is float, double, long double
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
// If ValueName is "" empty then all accumulators converted else one
func (cellCvt CellAllAccConverter) ToCsvRow() (func(interface{}, []string) error, error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return nil, err
	}

	// number of dimensions and number of accumulators to be converted
	nAcc := 1
	if cellCvt.ValueName == "" {
		nAcc = len(table.Acc)
	}
	nRank := table.Rank

	// for each dimension create converter from item id to code
	fd := make([]func(itemId int) (string, error), nRank)

	for k := 0; k < nRank; k++ {
		f, err := table.Dim[k].typeOf.itemIdToCode(cellCvt.TableName+"."+table.Dim[k].Name, table.Dim[k].IsTotal)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellAllAcc)
		if !ok {
			return errors.New("invalid type, expected: output table accumulator cell (internal error): " + cellCvt.TableName)
		}

		if len(row) != 1+nRank+nAcc || len(cell.DimIds) != nRank || len(cell.IsNull) != nAcc || len(cell.Value) != nAcc {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(1+nRank+nAcc) + ": " + cellCvt.TableName)
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
				if cellCvt.DoubleFmt != "" {
					row[1+nRank+k] = fmt.Sprintf(cellCvt.DoubleFmt, cell.Value[k])
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
// It does return error if len(row) not equal to number of fields in cell db-record.
// If dimension type is enum based then csv row is enum code and it is converted into cell.DimIds (into dimension type type enum ids).
func (cellCvt CellAllAccConverter) CsvToCell() (func(row []string) (interface{}, error), error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return nil, err
	}

	// number of dimensions and number of accumulators to be converted
	nAcc := 1
	if cellCvt.ValueName == "" {
		nAcc = len(table.Acc)
	}
	nRank := table.Rank

	// for each dimension create converter from item code to id
	fd := make([]func(src string) (int, error), nRank)

	for k := 0; k < nRank; k++ {
		f, err := table.Dim[k].typeOf.itemCodeToId(cellCvt.TableName+"."+table.Dim[k].Name, table.Dim[k].IsTotal)
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
			return nil, errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(1+nRank+nAcc) + ": " + cellCvt.TableName)
		}

		// subvalue number
		nSub, err := strconv.Atoi(row[0])
		if err != nil {
			return nil, err
		}
		/* validation done at writing
		if subCount == 1 && nSub != defaultSubId || nSub < 0 || nSub >= subCount {
			return nil, errors.New("invalid sub-value id: " + strconv.Itoa(nSub) + " output table: " + name)
		}
		*/
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
func (cellCvt CellAllAccConverter) IdToCodeCell(modelDef *ModelMeta, name string) (func(interface{}) (interface{}, error), error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return nil, err
	}

	// for each dimension create converter from item id to code
	fd := make([]func(itemId int) (string, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := table.Dim[k].typeOf.itemIdToCode(name+"."+table.Dim[k].Name, table.Dim[k].IsTotal)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// create cell converter
	cvt := func(src interface{}) (interface{}, error) {

		srcCell, ok := src.(CellAllAcc)
		if !ok {
			return nil, errors.New("invalid type, expected: output table accumulator cell (internal error): " + name)
		}
		if len(srcCell.DimIds) != table.Rank {
			return nil, errors.New("invalid cell rank: " + strconv.Itoa(len(srcCell.DimIds)) + ", expected: " + strconv.Itoa(table.Rank) + ": " + name)
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
