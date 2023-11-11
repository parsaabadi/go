// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"
)

// CellExpr is value of output table expression.
type CellExpr struct {
	cellIdValue     // dimensions as enum id's and value
	ExprId      int // output table expression id
}

// CellCodeExpr is value of output table expression.
// Dimension(s) items are enum codes, not enum ids.
type CellCodeExpr struct {
	cellCodeValue     // dimensions as enum codes and value
	ExprId        int // output table expression id
}

// CellTableConverter is a parent for for output table converters.
type CellTableConverter struct {
	ModelDef *ModelMeta // model metadata
	Name     string     // output table name
	theTable *TableMeta // if not nil then output table already found
}

// CellExprConverter is a converter for output table expression to implement CsvConverter interface.
type CellExprConverter struct {
	CellTableConverter        // model metadata and output table name
	IsIdCsv            bool   // if true then use enum id's else use enum codes
	DoubleFmt          string // if not empty then format string is used to sprintf if value type is float, double, long double
	IsNoZeroCsv        bool   // if true then do not write zero values into csv output
	IsNoNullCsv        bool   // if true then do not write NULL values into csv output
}

// return true if csv converter is using enum id's for dimensions
func (cellCvt *CellExprConverter) IsUseEnumId() bool { return cellCvt.IsIdCsv }

// CsvFileName return file name of csv file to store output table expression rows
func (cellCvt *CellExprConverter) CsvFileName() (string, error) {

	// find output table by name
	_, err := cellCvt.tableByName()
	if err != nil {
		return "", err
	}

	// make csv file name
	if cellCvt.IsIdCsv {
		return cellCvt.Name + ".id.csv", nil
	}
	return cellCvt.Name + ".csv", nil
}

// CsvHeader return first line for csv file: column names.
// Column names can be like: expr_name,dim0,dim1,expr_value
// or if IsIdCsv is true: expr_id,dim0,dim1,expr_value
func (cellCvt *CellExprConverter) CsvHeader() ([]string, error) {

	// find output table by name
	table, err := cellCvt.tableByName()
	if err != nil {
		return []string{}, err
	}

	// make first line columns
	h := make([]string, table.Rank+2)

	if cellCvt.IsIdCsv {
		h[0] = "expr_id"
	} else {
		h[0] = "expr_name"
	}
	for k := range table.Dim {
		h[k+1] = table.Dim[k].Name
	}
	h[table.Rank+1] = "expr_value"

	return h, nil
}

// KeyIds return converter to copy primary key: (expr_id, dimension ids) into key []int.
//
// Converter will return error if len(key) not equal to row key size.
func (cellCvt *CellExprConverter) KeyIds(name string) (func(interface{}, []int) error, error) {

	cvt := func(src interface{}, key []int) error {

		cell, ok := src.(CellExpr)
		if !ok {
			return errors.New("invalid type, expected: CellExpr (internal error): " + name)
		}

		n := len(cell.DimIds)
		if len(key) != n+1 {
			return errors.New("invalid size of key buffer, expected: " + strconv.Itoa(n+1) + ": " + name)
		}

		key[0] = cell.ExprId

		for k, e := range cell.DimIds {
			key[k+1] = e
		}
		return nil
	}

	return cvt, nil
}

// ToCsvIdRow return converter from output table cell (expr_id, dimensions, value) to csv row []string.
//
// Converter return isNotEmpty flag, it return false if IsNoZero or IsNoNull is set and cell value is empty or zero.
// Converter simply does Sprint() for each dimension item id, expression id and value.
// Converter will return error if len(row) not equal to number of fields in csv record.
func (cellCvt *CellExprConverter) ToCsvIdRow() (func(interface{}, []string) (bool, error), error) {

	// find output table by name
	_, err := cellCvt.tableByName()
	if err != nil {
		return nil, err
	}

	// return converter from id based cell to csv string array
	cvt := func(src interface{}, row []string) (bool, error) {

		cell, ok := src.(CellExpr)
		if !ok {
			return false, errors.New("invalid type, expected: CellExpr (internal error): " + cellCvt.Name)
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return false, errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+2) + ": " + cellCvt.Name)
		}

		row[0] = fmt.Sprint(cell.ExprId)

		for k, e := range cell.DimIds {
			row[k+1] = fmt.Sprint(e)
		}

		// use "null" string for db NULL values and format for model float types
		isNotEmpty := true

		if cell.IsNull {
			row[n+1] = "null"
			isNotEmpty = !cellCvt.IsNoNullCsv
		} else {

			if cellCvt.IsNoZeroCsv {
				fv, ok := cell.Value.(float64)
				isNotEmpty = ok && fv != 0
			}

			if cellCvt.DoubleFmt != "" {
				row[n+1] = fmt.Sprintf(cellCvt.DoubleFmt, cell.Value)
			} else {
				row[n+1] = fmt.Sprint(cell.Value)
			}
		}
		return isNotEmpty, nil
	}

	return cvt, nil
}

// ToCsvRow return converter from output table cell (expr_id, dimensions, value)
// to csv row []string (expr_name, dimensions, value).
//
// Converter return isNotEmpty flag, it return false if IsNoZero or IsNoNull is set and cell value is empty or zero.
// Converter return error if len(row) not equal to number of fields in csv record.
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
func (cellCvt *CellExprConverter) ToCsvRow() (func(interface{}, []string) (bool, error), error) {

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

	cvt := func(src interface{}, row []string) (bool, error) {

		cell, ok := src.(CellExpr)
		if !ok {
			return false, errors.New("invalid type, expected: output table expression cell (internal error): " + cellCvt.Name)
		}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return false, errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(n+2) + ": " + cellCvt.Name)
		}

		row[0] = table.Expr[cell.ExprId].Name

		// convert dimension item id to code
		for k, e := range cell.DimIds {
			v, err := fd[k](e)
			if err != nil {
				return false, err
			}
			row[k+1] = v
		}

		// use "null" string for db NULL values and format for model float types
		isNotEmpty := true

		if cell.IsNull {
			row[n+1] = "null"
			isNotEmpty = !cellCvt.IsNoNullCsv
		} else {

			if cellCvt.IsNoZeroCsv {
				fv, ok := cell.Value.(float64)
				isNotEmpty = ok && fv != 0
			}

			if cellCvt.DoubleFmt != "" {
				row[n+1] = fmt.Sprintf(cellCvt.DoubleFmt, cell.Value)
			} else {
				row[n+1] = fmt.Sprint(cell.Value)
			}
		}
		return isNotEmpty, nil
	}

	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to output table expression cell (dimensions and value).
//
// Converter return error if len(row) not equal to number of fields in cell db-record.
// If dimension type is enum based then csv row is enum code and it is converted into cell.DimIds (into dimension type type enum ids).
func (cellCvt *CellExprConverter) CsvToCell() (func(row []string) (interface{}, error), error) {

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
		cell := CellExpr{cellIdValue: cellIdValue{DimIds: make([]int, table.Rank)}}

		n := len(cell.DimIds)
		if len(row) != n+2 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(n+2) + ": " + cellCvt.Name)
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
			return nil, errors.New("invalid expression name: " + row[0] + " output table: " + cellCvt.Name)
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

// IdToCodeCell return converter from output table cell of ids: (expr_id, dimensions enum ids, value)
// to cell of codes: (expr_id, dimensions as enum codes, value).
//
// If dimension type is enum based then dimensions enum ids can be converted to enum code.
// If dimension type is simple (bool or int) then dimension value converted to string.
func (cellCvt *CellExprConverter) IdToCodeCell(modelDef *ModelMeta, name string) (func(interface{}) (interface{}, error), error) {

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

		srcCell, ok := src.(CellExpr)
		if !ok {
			return nil, errors.New("invalid type, expected: output table expression cell (internal error): " + name)
		}
		if len(srcCell.DimIds) != table.Rank {
			return nil, errors.New("invalid cell rank: " + strconv.Itoa(len(srcCell.DimIds)) + ", expected: " + strconv.Itoa(table.Rank) + ": " + name)
		}

		dstCell := CellCodeExpr{
			cellCodeValue: cellCodeValue{
				Dims:   make([]string, table.Rank),
				IsNull: srcCell.IsNull,
				Value:  srcCell.Value,
			},
			ExprId: srcCell.ExprId,
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

// return output table metadata by output table name
func (cellCvt *CellTableConverter) tableByName() (*TableMeta, error) {

	if cellCvt.theTable != nil {
		return cellCvt.theTable, nil // output table already found
	}

	// validate parameters
	if cellCvt.ModelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if cellCvt.Name == "" {
		return nil, errors.New("invalid (empty) output table name")
	}

	// find output table by name
	idx, ok := cellCvt.ModelDef.OutTableByName(cellCvt.Name)
	if !ok {
		return nil, errors.New("output table not found: " + cellCvt.Name)
	}
	cellCvt.theTable = &cellCvt.ModelDef.Table[idx]

	return cellCvt.theTable, nil
}
