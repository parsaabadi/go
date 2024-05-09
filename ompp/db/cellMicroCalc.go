// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/openmpp/go/ompp/helper"
)

// CellMicroCalc is value of entity microdata calculated expression.
// if attribute type is enum based value is enum id
type CellMicroCalc struct {
	Attr   []attrValue // group by attributes and calculated value attribute, using enum id's for enum based attributes
	CalcId int         // aggregation id
	RunId  int         // model run id
}

// CellCodeMicroCalc is value of entity microdata calculated expression.
// if attribute type is enum based value is enum code
type CellCodeMicroCalc struct {
	Attr      []attrValue // group by attributes and calculated value attribute, using enum codes for enum based attributes
	CalcName  string      // aggregation name
	RunDigest string      // model run digest
}

// CellMicroCalcConverter is a converter for calculated microdata row to implement CsvConverter interface.
type CellMicroCalcConverter struct {
	CellEntityConverter                 // model metadata, entity generation and and attributes
	CalcMaps                            // map between runs digest and id and calculations name and id
	GroupBy             []string        // attributes to group by
	theGroupBy          []EntityAttrRow // if not empty then entity generation attributes
}

// Set calculation name to Id and Id to name maps
func (cellCvt *CellMicroCalcConverter) SetCalcIdNameMap(calcLt []CalculateLayout) error {

	cellCvt.CalcIdToName = map[int]string{}
	cellCvt.CalcNameToId = map[string]int{}

	for k, c := range calcLt {

		if c.Name == "" {
			return errors.New("invalid (empty) calculation name at index: [" + strconv.Itoa(k) + "], id: " + strconv.Itoa(c.CalcId) + ": " + cellCvt.Name)
		}
		cellCvt.CalcIdToName[c.CalcId] = c.Name
		cellCvt.CalcNameToId[c.Name] = c.CalcId
	}
	return nil
}

// return true if csv converter is using enum id's for dimensions
func (cellCvt *CellMicroCalcConverter) IsUseEnumId() bool { return cellCvt.IsIdCsv }

// CsvFileName return file name of csv file to store calculated microdata rows
func (cellCvt *CellMicroCalcConverter) CsvFileName() (string, error) {

	// find entity by name
	_, err := cellCvt.entityByName()
	if err != nil {
		return "", err
	}

	// make csv file name
	if cellCvt.IsIdCsv {
		return cellCvt.Name + ".id.csv", nil
	}
	return cellCvt.Name + ".csv", nil
}

// CsvHeader return first line for csv file column names: names of group by attributes.
// For example: run_digest,calc_name,AgeGroup,Income,calc_value
// Or:          run_id,calc_id,AgeGroup,Income,calc_value
func (cellCvt *CellMicroCalcConverter) CsvHeader() ([]string, error) {

	// find group by attributes by generation Hid
	aGroupBy, err := cellCvt.groupByAttrs()
	if err != nil {
		return []string{}, err
	}

	// make first line columns
	h := make([]string, 3+len(aGroupBy))
	h[0] = "key"

	if cellCvt.IsIdCsv {
		h[0] = "run_id"
		h[1] = "calc_id"
	} else {
		h[0] = "run_digest"
		h[1] = "calc_name"
	}
	for k := range aGroupBy {
		h[k+2] = aGroupBy[k].Name
	}
	h[len(aGroupBy)+2] = "calc_value"

	return h, nil
}

// ToCsvIdRow return converter from microdata cell: (microdata key, attributes as enum id or built-in type value) to csv id's row []string.
//
// Converter return isNotEmpty flag, it is always true if there were no error during conversion.
// Converter simply does Sprint() for key and each attribute value, if value is NULL then empty "" string used.
// Converter will return error if len(row) not equal to number of fields in csv record.
func (cellCvt *CellMicroCalcConverter) ToCsvIdRow() (func(interface{}, []string) (bool, error), error) {

	// find group by attributes
	aGroupBy, err := cellCvt.groupByAttrs()
	if err != nil {
		return nil, err
	}
	nGrp := len(aGroupBy)

	// convert group by attributes string using Sprint()
	fa := make([]func(v interface{}) string, nGrp+1)

	for k := 0; k < nGrp; k++ {
		fa[k] = func(v interface{}) string { return fmt.Sprint(v) }
	}

	// for calculated value use format if specified
	if cellCvt.DoubleFmt != "" {
		fa[nGrp] = func(v interface{}) string { return fmt.Sprintf(cellCvt.DoubleFmt, v) }
	} else {
		fa[nGrp] = func(v interface{}) string { return fmt.Sprint(v) }
	}

	// return converter for run id, calc_id, group by attributes and calculated value
	cvt := func(src interface{}, row []string) (bool, error) {

		cell, ok := src.(CellMicroCalc)
		if !ok {
			return false, errors.New("invalid type, expected: CellMicroCalc (internal error): " + cellCvt.Name)
		}

		n := len(cell.Attr)
		if n != nGrp+1 || len(row) != n+2 {
			return false, errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(nGrp+3) + ": " + cellCvt.Name)
		}

		// row starts from run id and calc_id
		row[0] = fmt.Sprint(cell.RunId)
		row[1] = fmt.Sprint(cell.CalcId)

		// convert group by attributes and calculated values
		for k, a := range cell.Attr {

			// use "null" string for db NULL values
			if a.IsNull || a.Value == nil {
				row[k+2] = "null"
			} else {
				row[k+2] = fa[k](a.Value)
			}
		}

		return true, nil
	}
	return cvt, nil
}

// ToCsvRow return converter from microdata cell: (microdata key, attributes as enum code or built-in type value) to csv row []string.
//
// Converter return isNotEmpty flag, it is always true if there were no error during conversion.
// Converter simply does Sprint() for key and each attribute value, if value is NULL then empty "" string used.
// If attribute type is float and double format is not empty "" string then converter does Sprintf(using double format).
// If attribute type is enum based then converter return enum code for attribute enum id.
// Converter will return error if len(row) not equal to number of fields in csv record.
func (cellCvt *CellMicroCalcConverter) ToCsvRow() (func(interface{}, []string) (bool, error), error) {

	// find group by attributes
	aGroupBy, err := cellCvt.groupByAttrs()
	if err != nil {
		return nil, err
	}
	nGrp := len(aGroupBy)

	// convert group by attributes value to string:
	// for built-in attribute type use Sprint(), only float attribute is a calculated value
	// for enum attribute type return enum code by enum id
	fa := make([]func(v interface{}) (string, error), nGrp+1)

	for k, ga := range aGroupBy {

		if ga.typeOf.IsBuiltIn() { // built-in attribute type: format value by Sprint()

			fa[k] = func(v interface{}) (string, error) { return fmt.Sprint(v), nil }

		} else { // enum based attribute type: find and return enum code by enum id

			msgName := cellCvt.Name + "." + ga.Name // for error message, ex: Person.Income
			f, err := ga.typeOf.itemIdToCode(msgName, false)
			if err != nil {
				return nil, err
			}

			fa[k] = func(v interface{}) (string, error) { // convereter return enum code by enum id

				// depending on sql + driver it can be different type
				if iv, ok := helper.ToIntValue(v); ok {
					return f(iv)
				} else {
					return "", errors.New("invalid attribute value, must be integer enum id: " + msgName)
				}
			}
		}
	}

	// for calculated value use format if specified
	if cellCvt.DoubleFmt != "" {
		fa[nGrp] = func(v interface{}) (string, error) { return fmt.Sprintf(cellCvt.DoubleFmt, v), nil }
	} else {
		fa[nGrp] = func(v interface{}) (string, error) { return fmt.Sprint(v), nil }
	}

	// return converter for run name, CalcName, group by attributes and calculated value
	cvt := func(src interface{}, row []string) (bool, error) {

		cell, ok := src.(CellMicroCalc)
		if !ok {
			return false, errors.New("invalid type, expected: CellMicroCalc (internal error): " + cellCvt.Name)
		}

		n := len(cell.Attr)
		if n != nGrp+1 || len(row) != n+2 {
			return false, errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(nGrp+3) + ": " + cellCvt.Name)
		}

		// row starts from run digest and CalcName
		row[0] = cellCvt.IdToDigest[cell.RunId]
		if row[0] == "" {
			return false, errors.New("invalid (missing) run id: " + strconv.Itoa(cell.RunId) + " entity: " + cellCvt.Name)
		}
		row[1] = cellCvt.CalcIdToName[cell.CalcId]
		if row[1] == "" {
			return false, errors.New("invalid (missing) calculation id: " + strconv.Itoa(cell.CalcId) + " entity: " + cellCvt.Name)
		}

		// convert group by attributes and calculated value
		for k, a := range cell.Attr {

			// use "null" string for db NULL values
			if a.IsNull || a.Value == nil {
				row[k+2] = "null"
			} else {
				if s, e := fa[k](a.Value); e != nil { // use attribute value converter
					return false, e
				} else {
					row[k+2] = s
				}
			}
		}
		return true, nil
	}
	return cvt, nil
}

// IdToCodeCell return converter
// from calculated microdata cell of ids: (run id, calc_id, group by attributes as built-in values or enum id's, calculated value attribute)
// into cell of codes: (run_digest, CalcName, group by attributes as enum codes or built-in values, calc_value).
//
// If attribute type is enum based then attribute enum id converted to enum code.
// If attribute type is built-in (bool, int, float) then return attribute value as is, no conversion.
func (cellCvt *CellMicroCalcConverter) IdToCodeCell(modelDef *ModelMeta, _ string) (func(interface{}) (interface{}, error), error) {

	// find group by attributes
	aGroupBy, err := cellCvt.groupByAttrs()
	if err != nil {
		return nil, err
	}
	nGrp := len(aGroupBy)

	// convert attributes value to string if attribute is enum based: return enum code by enum id
	// do not convert built-in attribute type, converter function is nil
	fa := make([]func(v interface{}) (string, error), nGrp+1)

	for k, ga := range aGroupBy {

		if ga.typeOf.IsBuiltIn() {

			fa[k] = nil // built-in attribute type: do not convert, do copy value

		} else { // enum based attribute type: find and return enum code by enum id

			msgName := cellCvt.Name + "." + ga.Name // for error message, ex: Person.Income
			f, err := ga.typeOf.itemIdToCode(msgName, false)
			if err != nil {
				return nil, err
			}

			fa[k] = func(v interface{}) (string, error) { // convereter return enum code by enum id

				// depending on sql + driver it can be different type
				if iv, ok := helper.ToIntValue(v); ok {
					return f(iv)
				} else {
					return "", errors.New("invalid attribute value, must be integer enum id: " + msgName)
				}
			}
		}
	}
	fa[nGrp] = nil // do not convert calculated values, copy it

	// create cell converter
	cvt := func(src interface{}) (interface{}, error) {

		srcCell, ok := src.(CellMicroCalc)
		if !ok {
			return nil, errors.New("invalid type, expected: CellMicroCalc (internal error): " + cellCvt.Name)
		}
		if len(srcCell.Attr) != nGrp+1 {
			return nil, errors.New("invalid number of attributes, expected: " + strconv.Itoa(nGrp+1) + ": " + cellCvt.Name)
		}

		dgst := cellCvt.IdToDigest[srcCell.RunId]
		if dgst == "" {
			return nil, errors.New("invalid (missing) run id: " + strconv.Itoa(srcCell.RunId) + " entity: " + cellCvt.Name)
		}
		cName := cellCvt.CalcIdToName[srcCell.CalcId]
		if cName == "" {
			return nil, errors.New("invalid (missing) calculation id: " + strconv.Itoa(srcCell.CalcId) + " entity: " + cellCvt.Name)
		}

		dstCell := CellCodeMicroCalc{
			Attr:      make([]attrValue, nGrp+1),
			CalcName:  cName,
			RunDigest: dgst,
		}

		// convert group by attributes enum id's to enum codes, copy built-in values to string
		for k, a := range srcCell.Attr {

			if a.IsNull || a.Value == nil {
				dstCell.Attr[k] = attrValue{IsNull: true, Value: nil}
			} else {
				if fa[k] == nil {
					dstCell.Attr[k].Value = a.Value // converter not defined for built-in types: copy value as is
				} else {
					if s, e := fa[k](a.Value); e != nil { // use attribute value converter
						return nil, err
					} else {
						dstCell.Attr[k].Value = s
					}
				}
			}
		}

		return dstCell, nil // converted OK
	}

	return cvt, nil
}

// return entity metadata by entity name and entity generation attributes by generation Hid
func (cellCvt *CellMicroCalcConverter) groupByAttrs() ([]EntityAttrRow, error) {

	if len(cellCvt.theGroupBy) > 0 {
		return cellCvt.theGroupBy, nil // group by attributes already found
	}
	// validate parameters
	if len(cellCvt.GroupBy) <= 0 {
		return []EntityAttrRow{}, errors.New("invalid (empty) entity group by attributes list: " + cellCvt.Name)
	}

	// set entity metadata by entity name and attributes by generation Hid
	ent, attrs, err := cellCvt.entityAttrs()
	if err != nil {
		return []EntityAttrRow{}, err
	}

	// find group by microdata attributes by name
	aGroupBy := []EntityAttrRow{}

	for k := 0; k < len(attrs); k++ {
		for _, name := range cellCvt.GroupBy {
			if name == attrs[k].Name {
				aGroupBy = append(aGroupBy, attrs[k])
				break
			}
		}
	}

	// check: all group by attributes must be found and it must boolean or not built-in
	for _, name := range cellCvt.GroupBy {

		isFound := false
		for k := 0; !isFound && k < len(aGroupBy); k++ {
			isFound = aGroupBy[k].Name == name
		}
		if !isFound {
			return []EntityAttrRow{}, errors.New("entity group by attribute not found by: " + ent.Name + "." + name)
		}
	}

	for k := range aGroupBy {
		if aGroupBy[k].typeOf.IsBuiltIn() && !aGroupBy[k].typeOf.IsBool() {
			return []EntityAttrRow{}, errors.New("invalid type of entity group by attribute not found by: " + ent.Name + "." + aGroupBy[k].Name + " : " + aGroupBy[k].typeOf.Name)
		}
	}

	cellCvt.theGroupBy = aGroupBy

	return cellCvt.theGroupBy, nil
}
