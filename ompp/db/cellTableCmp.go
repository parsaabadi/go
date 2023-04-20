// Copyright (c) 2021 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"
)

// CellTableCmp is value of output table comparison expression.
type CellTableCmp struct {
	cellIdValue     // dimensions as enum id's and value
	RunId       int // model run id
}

// CellCodeTableCmp is value of output table comparison expression.
// Dimension(s) items are enum codes, not enum ids.
type CellCodeTableCmp struct {
	cellCodeValue        // dimensions as enum codes and value
	RunDigest     string // model run digest
}

// CellTableCmpConverter is a converter for output table comparison cell to implement CsvConverter interface.
type CellTableCmpConverter struct {
	CellTableConverter                // model metadata and output table name
	IsIdCsv            bool           // if true then use enum id's else use enum codes
	DoubleFmt          string         // if not empty then format string is used to sprintf if value type is float, double, long double
	IdToDigest         map[int]string // map of run id's to run digests
	DigestToId         map[string]int // map of run digests to run id's
}

// return true if csv converter is using enum id's for dimensions
func (cellCvt *CellTableCmpConverter) IsUseEnumId() bool { return cellCvt.IsIdCsv }

// CsvFileName return file name of csv file to store output table comparison rows
func (cellCvt *CellTableCmpConverter) CsvFileName() (string, error) {

	// find output table by name
	_, err := cellCvt.tableByName()
	if err != nil {
		return "", err
	}

	// make csv file name
	if cellCvt.IsIdCsv {
		return cellCvt.Name + ".id.cmp.csv", nil
	}
	return cellCvt.Name + ".cmp.csv", nil
}

// CsvHeader return first line for csv file: column names, it's look like: run_digest,dim0,dim1,value
func (cellCvt *CellTableCmpConverter) CsvHeader() ([]string, error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return []string{}, err
	}

	// make first line columns
	h := make([]string, table.Rank+2)

	h[0] = "run_digest"
	for k := range table.Dim {
		h[k+1] = table.Dim[k].Name
	}
	h[table.Rank+1] = "value"

	return h, nil
}

// KeyIds return converter to copy primary key: (run_id, dimension ids) into key []int.
//
// Converter will return error if len(key) not equal to row key size.
func (cellCvt *CellTableCmpConverter) KeyIds(name string) (func(interface{}, []int) error, error) {

	cvt := func(src interface{}, key []int) error {

		cell, ok := src.(CellTableCmp)
		if !ok {
			return errors.New("invalid type, expected: CellTableCmp (internal error): " + name)
		}

		n := len(cell.DimIds)
		if len(key) != n+1 {
			return errors.New("invalid size of key buffer, expected: " + strconv.Itoa(n+1) + ": " + name)
		}

		key[0] = cell.RunId

		for k, e := range cell.DimIds {
			key[k+1] = e
		}
		return nil
	}

	return cvt, nil
}

// ToCsvIdRow return converter from output table comparison cell (run_id, dimensions, value) to csv row []string.
//
// Converter simply does Sprint() for each dimension item id, run id and value.
// Converter will return error if len(row) not equal to number of fields in csv record.
func (cellCvt *CellTableCmpConverter) ToCsvIdRow() (func(interface{}, []string) error, error) {

	// find output table by name
	_, err := cellCvt.tableByName()
	if err != nil {
		return nil, err
	}

	// return converter from id based cell to csv string array
	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellTableCmp)
		if !ok {
			return errors.New("invalid type, expected: CellTableCmp (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+2) + ": " + cellCvt.Name)
		}

		row[0] = fmt.Sprint(cell.RunId)

		for k, e := range cell.DimIds {
			row[k+1] = fmt.Sprint(e)
		}

		// use "null" string for db NULL values and format for model float types
		if cell.IsNull {
			row[n+1] = "null"
		} else {
			if cellCvt.DoubleFmt != "" {
				row[n+1] = fmt.Sprintf(cellCvt.DoubleFmt, cell.Value)
			} else {
				row[n+1] = fmt.Sprint(cell.Value)
			}
		}
		return nil
	}

	return cvt, nil
}

// ToCsvRow return converter from output table comparison cell (run_id, dimensions, value)
// to csv row []string (run digest, dimensions, value).
//
// Converter will return error if len(row) not equal to number of fields in csv record.
// Converter will return error if run_id not exist in the list of model runs (in run_lst table).
// Double format string is used if parameter type is float, double, long double.
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
func (cellCvt *CellTableCmpConverter) ToCsvRow() (func(interface{}, []string) error, error) {

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

		cell, ok := src.(CellTableCmp)
		if !ok {
			return errors.New("invalid type, expected: output table comparison cell (internal error)")
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+2) + ": " + cellCvt.Name)
		}

		row[0] = cellCvt.IdToDigest[cell.RunId]
		if row[0] == "" {
			return errors.New("invalid (missing) run id: " + strconv.Itoa(cell.RunId) + " output table: " + cellCvt.Name)
		}

		// convert dimension item id to code
		for k, e := range cell.DimIds {
			v, err := fd[k](e)
			if err != nil {
				return err
			}
			row[k+1] = v
		}

		// use "null" string for db NULL values and format for model float types
		if cell.IsNull {
			row[n+1] = "null"
		} else {
			if cellCvt.DoubleFmt != "" {
				row[n+1] = fmt.Sprintf(cellCvt.DoubleFmt, cell.Value)
			} else {
				row[n+1] = fmt.Sprint(cell.Value)
			}
		}
		return nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to output table comparison cell (run id, dimensions and value).
//
// It does return error if len(row) not equal to number of fields in cell db-record.
// If dimension type is enum based then csv row is enum code and it is converted into cell.DimIds (into dimension type type enum ids).
func (cellCvt *CellTableCmpConverter) CsvToCell() (func(row []string) (interface{}, error), error) {

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
		cell := CellTableCmp{cellIdValue: cellIdValue{DimIds: make([]int, table.Rank)}}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+2) + ": " + cellCvt.Name)
		}

		// run id by digest
		cell.RunId = cellCvt.DigestToId[row[0]]
		if cell.RunId <= 0 {
			return nil, errors.New("invalid (or empty) run digest: " + row[0] + " output table: " + cellCvt.Name)
		}

		// convert dimensions: enum code to enum id or integer value for simple type dimension
		for k := range cell.DimIds {
			i, err := fd[k](row[k+1])
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

// IdToCodeCell return converter from output table comparison cell of ids: (run_id, dimensions enum ids, value)
// to cell of codes: (run_digest, dimensions as enum codes, value).
//
// If dimension type is enum based then dimensions enum ids can be converted to enum code.
// If dimension type is simple (bool or int) then dimension value converted to string.
func (cellCvt *CellTableCmpConverter) IdToCodeCell(modelDef *ModelMeta, name string) (func(interface{}) (interface{}, error), error) {

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

		srcCell, ok := src.(CellTableCmp)
		if !ok {
			return nil, errors.New("invalid type, expected: output table comparison cell (internal error): " + name)
		}
		if len(srcCell.DimIds) != table.Rank {
			return nil, errors.New("invalid cell rank: " + strconv.Itoa(len(srcCell.DimIds)) + ", expected: " + strconv.Itoa(table.Rank) + ": " + name)
		}

		dgst := cellCvt.IdToDigest[srcCell.RunId]
		if dgst == "" {
			return nil, errors.New("invalid (missing) run id: " + strconv.Itoa(srcCell.RunId) + " output table: " + name)
		}

		dstCell := CellCodeTableCmp{
			cellCodeValue: cellCodeValue{
				Dims:   make([]string, table.Rank),
				IsNull: srcCell.IsNull,
				Value:  srcCell.Value,
			},
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
