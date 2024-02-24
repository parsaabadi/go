// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

// cellIdValue is dimensions item as id and value of input parameter or output table.
type cellIdValue struct {
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

// map between runs digest and id and calculations name and id
type CalcMaps struct {
	IdToDigest   map[int]string // map of run id's to run digests
	DigestToId   map[string]int // map of run digests to run id's
	CalcIdToName map[int]string // map of calculation id to name
	CalcNameToId map[string]int // map of calculation name to id
}

// return empty value of calculation maps
func EmptyCalcMaps() CalcMaps {
	return CalcMaps{
		IdToDigest:   map[int]string{},
		DigestToId:   map[string]int{},
		CalcIdToName: map[int]string{},
		CalcNameToId: map[string]int{},
	}
}

// CsvConverter provide methods to convert parameters or output table data from or to row []string for csv file.
type CsvIntKeysConverter interface {
	CsvConverter // convert parameter row or output table row from or to row []string for csv file
	CellIntKeys  // provide a method to get row keys as []int, for example, get sub id and dimension ids
}

// CsvConverter provide methods to convert parameter row, output table row or microdata row from or to row []string for csv file.
// Double format string is used for output bale values or if parameter type is float, double, long double.
// If dimension type is enum based then csv row is enum code and cell.DimIds is enum id.
// If parameter type is enum based then cell value is enum id and csv row value is enum code.
type CsvConverter interface {
	// return file name of csv file to store parameter or output table rows
	CsvFileName() (string, error)

	// return true if csv converter is using enum id's for dimensions or attributes
	IsUseEnumId() bool

	// return first line of csv file with column names: expr_name,dim0,dim1,expr_value.
	// if IsIdCsv is true: expr_id,dim0,dim1,expr_value
	// if isAllAcc is true: sub_id,dim0,dim1,acc0,acc1,acc2
	CsvHeader() ([]string, error)

	// return converter from cell of parameter, output table or microdata to csv id's row []string.
	// converter simply sprint() dimension id's and value into []string buffer.
	// converter return isNotEmpty flag if cell value is not empty.
	ToCsvIdRow() (func(interface{}, []string) (bool, error), error)

	// return converter from cell of parameter, output table or microdata to csv row []string.
	// it does convert from enum id to code for all dimensions into []string buffer.
	// if this is a enum-based parameter value then it is also converted to enum code.
	// converter return isNotEmpty flag if cell value is not empty.
	ToCsvRow() (func(interface{}, []string) (bool, error), error)

	// return converter from csv row []string to parameter, output table or microdata cell (dimensions and value or microdata key and attributes value)
	CsvToCell() (func(row []string) (interface{}, error), error)
}

// CellIntKeys provide method to get a copy of cell keys as []int for parameter or output table row,
// for example, return [parameter row sub id and dimension ids].
type CellIntKeys interface {

	// KeyIds return converter to copy row primary key into key []int.
	// Row primary keys are:
	//   parameter: (sub_id, dimension ids)
	//   accumulators: (acc_id, sub_id, dimension ids)
	//   all accumulators: (sub_id, dimension ids)
	//   expressions: (expr_id, dimension ids)
	KeyIds(name string) (func(interface{}, []int) error, error)
}

// CellToCodeConverter provide methods to convert parameters or output table row from enum id to enum code.
// If dimension type is enum based then dimensions enum ids can be converted to enum code.
// If dimension type is simple (bool or int) then dimension value converted to string.
// If parameter type is enum based then cell value enum id converted to enum code.
type CellToCodeConverter interface {

	// IdToCodeCell return converter from id cell to code cell.
	// Cell is dimensions and value of parameter or output table.
	// It does convert from enum id to code for all dimensions and enum-based parameter value.
	IdToCodeCell(modelDef *ModelMeta, name string) (func(interface{}) (interface{}, error), error)
}

// CellToIdConverter provide methods to convert parameters or output table row from enum code to enum id.
// If dimension type is enum based then dimensions enum codes converted to enum ids.
// If dimension type is simple (bool or int) then dimension code converted from string to dimension type.
// If parameter type is enum based then cell value enum code converted to enum id.
type CellToIdConverter interface {

	// CodeToIdCell return converter from code cell to id cell.
	// Cell is dimensions and value of parameter or output table.
	// It does convert from enum code to id for all dimensions and enum-based parameter value.
	CodeToIdCell(modelDef *ModelMeta, name string) (func(interface{}) (interface{}, error), error)
}
