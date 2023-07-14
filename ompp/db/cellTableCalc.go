// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"
)

// CellTableCalc is value of output table calculated expression.
type CellTableCalc struct {
	cellIdValue     // dimensions as enum id's and value
	CalcId      int // calculated expression id
	RunId       int // model run id
}

// CellCodeTableCalc is value of output table calculated expression.
// Dimension(s) items are enum codes, not enum ids.
type CellCodeTableCalc struct {
	cellCodeValue        // dimensions as enum codes and value
	CalcId        int    // calculated expression id
	RunDigest     string // model run digest
}

// CellTableCalcConverter is a converter for output table calculated cell to implement CsvConverter interface.
type CellTableCalcConverter struct {
	CellTableConverter                // model metadata and output table name
	IsIdCsv            bool           // if true then use enum id's else use enum codes
	DoubleFmt          string         // if not empty then format string is used to sprintf if value type is float, double, long double
	IdToDigest         map[int]string // map of run id's to run digests
	DigestToId         map[string]int // map of run digests to run id's
}

// return true if csv converter is using enum id's for dimensions
func (cellCvt *CellTableCalcConverter) IsUseEnumId() bool { return cellCvt.IsIdCsv }

// CsvFileName return file name of csv file to store output table calculated rows
func (cellCvt *CellTableCalcConverter) CsvFileName() (string, error) {

	// find output table by name
	_, err := cellCvt.tableByName()
	if err != nil {
		return "", err
	}

	// make csv file name
	if cellCvt.IsIdCsv {
		return cellCvt.Name + ".id.calc.csv", nil
	}
	return cellCvt.Name + ".calc.csv", nil
}

// CsvHeader return first line for csv file: column names, it's look like: run_digest,calc_id,dim0,dim1,calc_value
func (cellCvt *CellTableCalcConverter) CsvHeader() ([]string, error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return []string{}, err
	}

	// make first line columns
	h := make([]string, table.Rank+3)

	if cellCvt.IsIdCsv {
		h[0] = "run_id"
	} else {
		h[0] = "run_digest"
	}
	h[1] = "calc_id"
	for k := range table.Dim {
		h[k+2] = table.Dim[k].Name
	}
	h[table.Rank+2] = "calc_value"

	return h, nil
}

// KeyIds return converter to copy primary key: (run_id, calc_id, dimension ids) into key []int.
//
// Converter will return error if len(key) not equal to row key size.
func (cellCvt *CellTableCalcConverter) KeyIds(name string) (func(interface{}, []int) error, error) {

	cvt := func(src interface{}, key []int) error {

		cell, ok := src.(CellTableCalc)
		if !ok {
			return errors.New("invalid type, expected: CellTableCalc (internal error): " + name)
		}

		n := len(cell.DimIds)
		if len(key) != n+2 {
			return errors.New("invalid size of key buffer, expected: " + strconv.Itoa(n+2) + ": " + name)
		}

		key[0] = cell.RunId
		key[1] = cell.CalcId

		for k, e := range cell.DimIds {
			key[k+2] = e
		}
		return nil
	}

	return cvt, nil
}

// ToCsvIdRow return converter from output table calculated cell (run_id, calc_id, dimensions, calc_value) to csv row []string.
//
// Converter simply does Sprint() for each dimension item id, run id and value.
// Converter will return error if len(row) not equal to number of fields in csv record.
func (cellCvt *CellTableCalcConverter) ToCsvIdRow() (func(interface{}, []string) error, error) {

	// find output table by name
	_, err := cellCvt.tableByName()
	if err != nil {
		return nil, err
	}

	// return converter from id based cell to csv string array
	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellTableCalc)
		if !ok {
			return errors.New("invalid type, expected: CellTableCalc (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+3 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+3) + ": " + cellCvt.Name)
		}

		row[0] = fmt.Sprint(cell.RunId)
		row[1] = fmt.Sprint(cell.CalcId)

		for k, e := range cell.DimIds {
			row[k+2] = fmt.Sprint(e)
		}

		// use "null" string for db NULL values and format for model float types
		if cell.IsNull {
			row[n+2] = "null"
		} else {
			if cellCvt.DoubleFmt != "" {
				row[n+2] = fmt.Sprintf(cellCvt.DoubleFmt, cell.Value)
			} else {
				row[n+2] = fmt.Sprint(cell.Value)
			}
		}
		return nil
	}

	return cvt, nil
}

// ToCsvRow return converter from output table calculated cell (run_id, calc_id, dimensions, calc_value)
// to csv row []string (run digest, calc_id, dimensions, calc_value).
//
// Converter will return error if len(row) not equal to number of fields in csv record.
// Converter will return error if run_id not exist in the list of model runs (in run_lst table).
// Double format string is used if parameter type is float, double, long double.
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
func (cellCvt *CellTableCalcConverter) ToCsvRow() (func(interface{}, []string) error, error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return nil, err
	}

	// for each dimension create converter from item id to code
	fd := make([]func(itemId int) (string, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := table.Dim[k].typeOf.itemIdToCode(cellCvt.Name+"."+table.Dim[k].Name, table.Dim[k].IsTotal)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellTableCalc)
		if !ok {
			return errors.New("invalid type, expected: output table calculated cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+3 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+3) + ": " + cellCvt.Name)
		}

		row[0] = cellCvt.IdToDigest[cell.RunId]
		if row[0] == "" {
			return errors.New("invalid (missing) run id: " + strconv.Itoa(cell.RunId) + " output table: " + cellCvt.Name)
		}
		row[1] = fmt.Sprint(cell.CalcId)

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
			if cellCvt.DoubleFmt != "" {
				row[n+2] = fmt.Sprintf(cellCvt.DoubleFmt, cell.Value)
			} else {
				row[n+2] = fmt.Sprint(cell.Value)
			}
		}
		return nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to output table calculated cell (run id, calc_id, dimensions and calc_value).
//
// It does return error if len(row) not equal to number of fields in cell db-record.
// If dimension type is enum based then csv row is enum code and it is converted into cell.DimIds (into dimension type type enum ids).
func (cellCvt *CellTableCalcConverter) CsvToCell() (func(row []string) (interface{}, error), error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return nil, err
	}

	// for each dimension create converter from item code to id
	fd := make([]func(src string) (int, error), table.Rank)

	for k := 0; k < table.Rank; k++ {
		f, err := table.Dim[k].typeOf.itemCodeToId(cellCvt.Name+"."+table.Dim[k].Name, table.Dim[k].IsTotal)
		if err != nil {
			return nil, err
		}
		fd[k] = f
	}

	// do conversion
	cvt := func(row []string) (interface{}, error) {

		// make conversion buffer and check input csv row size
		cell := CellTableCalc{cellIdValue: cellIdValue{DimIds: make([]int, table.Rank)}}

		n := len(cell.DimIds)
		if len(row) != n+3 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+3) + ": " + cellCvt.Name)
		}

		// run id by digest
		cell.RunId = cellCvt.DigestToId[row[0]]
		if cell.RunId <= 0 {
			return nil, errors.New("invalid (or empty) run digest: " + row[0] + " output table: " + cellCvt.Name)
		}
		cell.CalcId = cellCvt.DigestToId[row[1]]

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

// IdToCodeCell return converter from output table calculated cell of ids: (run_id, calc_id, dimensions enum ids, calc_value)
// to cell of codes: (run_digest, dimensions as enum codes, calc_value).
//
// If dimension type is enum based then dimensions enum ids can be converted to enum code.
// If dimension type is simple (bool or int) then dimension value converted to string.
func (cellCvt *CellTableCalcConverter) IdToCodeCell(modelDef *ModelMeta, name string) (func(interface{}) (interface{}, error), error) {

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

		srcCell, ok := src.(CellTableCalc)
		if !ok {
			return nil, errors.New("invalid type, expected: output table calculated cell (internal error): " + name)
		}
		if len(srcCell.DimIds) != table.Rank {
			return nil, errors.New("invalid cell rank: " + strconv.Itoa(len(srcCell.DimIds)) + ", expected: " + strconv.Itoa(table.Rank) + ": " + name)
		}

		dgst := cellCvt.IdToDigest[srcCell.RunId]
		if dgst == "" {
			return nil, errors.New("invalid (missing) run id: " + strconv.Itoa(srcCell.RunId) + " output table: " + name)
		}

		dstCell := CellCodeTableCalc{
			cellCodeValue: cellCodeValue{
				Dims:   make([]string, table.Rank),
				IsNull: srcCell.IsNull,
				Value:  srcCell.Value,
			},
			CalcId:    srcCell.CalcId,
			RunDigest: dgst,
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
