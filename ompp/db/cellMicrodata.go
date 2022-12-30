// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/openmpp/go/ompp/helper"
)

// CellMicro is a row of entity microdata: entity_key and attribute values,
// if attribute type is enum based value is enum id
type CellMicro struct {
	Key  uint64      // microdata key, part of row primary key: entity_key
	Attr []attrValue // attributes value: built-in type value or enum id
}

// CellCodeMicro is a row of entity microdata: entity_key and attribute values,
// if attribute type is enum based value is enum code
type CellCodeMicro struct {
	Key   uint64      // microdata key, part of row primary key: entity_key
	Attrs []attrValue // attributes value: built-in type value or enum id
}

// Entity attribute value: value of built-in type or enum value,
// if attribute type is enum based value is enum id or enum code
type attrValue struct {
	IsNull bool        // if true then value is NULL
	Value  interface{} // value: int64, bool, float64 or string
}

// CellMicroConverter is a converter for entity microdata row to implement CsvConverter interface.
type CellMicroConverter struct {
	ModelDef   *ModelMeta      // model metadata
	EntityName string          // model entity name
	RunDef     *RunMeta        // model run metadata
	GenHid     int             // entity generation Hid
	IsIdCsv    bool            // if true then use enum id's else use enum codes
	DoubleFmt  string          // if not empty then format string is used to sprintf if value type is float, double, long double
	theEntity  *EntityMeta     // if not nil then entity found
	theAttrs   []EntityAttrRow // if not empty then entity generation attributes
}

// retrun true if csv converter is using enum id's for dimensions
func (cellCvt CellMicroConverter) IsUseEnumId() bool { return cellCvt.IsIdCsv }

// CsvFileName return file name of csv file to store entity microdata rows
func (cellCvt CellMicroConverter) CsvFileName() (string, error) {

	// find entity by name
	_, err := cellCvt.entityByName()
	if err != nil {
		return "", err
	}

	// make csv file name
	if cellCvt.IsIdCsv {
		return cellCvt.EntityName + ".id.csv", nil
	}
	return cellCvt.EntityName + ".csv", nil
}

// CsvHeader return first line for csv file: column names, it's look like: key,AgeGroup,Income.
func (cellCvt CellMicroConverter) CsvHeader() ([]string, error) {

	// find entity metadata by entity name and attributes by generation Hid
	_, attrs, err := cellCvt.entityAttrs()
	if err != nil {
		return []string{}, err
	}

	// make first line columns
	h := make([]string, 1+len(attrs))
	h[0] = "key"

	for k, ea := range attrs {
		h[k+1] = ea.Name
	}
	return h, nil
}

// ToCsvIdRow return converter from microdata cell: (microdata key, attributes as enum id or built-in type value) to csv row []string.
//
// Converter simply does Sprint() for key and each attribute value, if value is NULL then empty "" string used.
// Converter will return error if len(row) not equal to number of fields in csv record.
func (cellCvt CellMicroConverter) ToCsvIdRow() (func(interface{}, []string) error, error) {

	// find entity metadata by entity name and attributes by generation Hid
	_, attrs, err := cellCvt.entityAttrs()
	if err != nil {
		return nil, err
	}
	nAttr := len(attrs)

	// convert attributes value to string using Sprint() or Sprintf(double format)
	fd := make([]func(v interface{}) string, nAttr)

	for k, ea := range attrs {

		// for float attributes use format if specified
		if cellCvt.DoubleFmt != "" && ea.typeOf.IsFloat() {
			fd[k] = func(v interface{}) string { return fmt.Sprintf(cellCvt.DoubleFmt, v) }
		} else {
			fd[k] = func(v interface{}) string { return fmt.Sprint(v) }
		}
	}

	// return converter for microdata key and attribute values
	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellMicro)
		if !ok {
			return errors.New("invalid type, expected: CellMicro (internal error): " + cellCvt.EntityName)
		}

		n := len(cell.Attr)
		if n != nAttr || len(row) != n+1 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(nAttr+1) + ": " + cellCvt.EntityName)
		}

		row[0] = fmt.Sprint(cell.Key)

		for k, a := range cell.Attr {

			// use "null" string for db NULL values
			if a.IsNull || a.Value == nil {
				row[k+1] = "null"
			} else {
				row[k+1] = fd[k](a.Value)
			}
		}
		return nil
	}
	return cvt, nil
}

// ToCsvIdRow return converter from microdata cell: (microdata key, attributes as enum code or built-in type value) to csv row []string.
//
// Converter simply does Sprint() for key and each attribute value, if value is NULL then empty "" string used.
// If attribute type is float and double format is not empty "" string then converter does Sprintf(using double format).
// If attribute type is enum based then converter retrun enum code for attribute enum id.
// Converter will return error if len(row) not equal to number of fields in csv record.
func (cellCvt CellMicroConverter) ToCsvRow() (func(interface{}, []string) error, error) {

	// find entity metadata by entity name and attributes by generation Hid
	_, attrs, err := cellCvt.entityAttrs()
	if err != nil {
		return nil, err
	}
	nAttr := len(attrs)

	// convert attributes value to string:
	// for built-in attribute type use Sprint() or Sprintf(double format)
	// for enum attribute type return enum code by enum id
	fd := make([]func(v interface{}) (string, error), nAttr)

	for k, ea := range attrs {

		if ea.typeOf.IsBuiltIn() { // built-in attribute type: format value by Sprint()

			// for float attributes use format if specified
			if cellCvt.DoubleFmt != "" && ea.typeOf.IsFloat() {

				fd[k] = func(v interface{}) (string, error) { return fmt.Sprintf(cellCvt.DoubleFmt, v), nil }
			} else {
				fd[k] = func(v interface{}) (string, error) { return fmt.Sprint(v), nil }
			}
		} else { // enum based attribute type: frind and return enum code by enum id

			msgName := cellCvt.EntityName + "." + ea.Name // for error message, ex: Person.Income
			f, err := ea.typeOf.itemIdToCode(msgName, false)
			if err != nil {
				return nil, err
			}

			fd[k] = func(v interface{}) (string, error) { // convereter retrun enum code by enum id

				// depending on sql + driver it can be different type
				if iv, ok := helper.ToIntValue(v); ok {
					return f(iv)
				} else {
					return "", errors.New("invalid attribute value, must be integer enum id: " + msgName)
				}
			}
		}
	}

	// return converter for microdata key and attribute values
	cvt := func(src interface{}, row []string) error {

		cell, ok := src.(CellMicro)
		if !ok {
			return errors.New("invalid type, expected: CellMicro (internal error): " + cellCvt.EntityName)
		}

		n := len(cell.Attr)
		if n != nAttr || len(row) != n+1 {
			return errors.New("invalid size of csv row buffer, expected: " + strconv.Itoa(nAttr+1) + ": " + cellCvt.EntityName)
		}

		row[0] = fmt.Sprint(cell.Key)

		for k, a := range cell.Attr {

			// use "null" string for db NULL values
			if a.IsNull || a.Value == nil {
				row[k+1] = "null"
			} else {
				if s, e := fd[k](a.Value); e != nil { // use attribute value converter
					return e
				} else {
					row[k+1] = s
				}
			}
		}
		return nil
	}
	return cvt, nil
}

// CsvToCell return closure to convert csv row []string to microdata cell (key, attributes value).
//
// It does return error if len(row) not equal to number of fields in cell db-record.
// If attribute type is enum based then csv row contains enum code and it is converted into cell attribute enum id.
// Converter will return error if len(row) not equal to number of fields in csv record.
func (cellCvt CellMicroConverter) CsvToCell() (func(row []string) (interface{}, error), error) {

	// find entity metadata by entity name and attributes by generation Hid
	_, attrs, err := cellCvt.entityAttrs()
	if err != nil {
		return nil, err
	}
	nAttr := len(attrs)

	// convert attributes string to value:
	// for built-in attribute type use Sprint() or Sprintf(double format)
	// for enum attribute type return enum code by enum id
	fd := make([]func(src string) (interface{}, error), nAttr)

	for k, ea := range attrs {

		msgName := cellCvt.EntityName + "." + ea.Name // for error message, ex: Person.Income

		// attribute type to create converter

		switch {
		case !ea.typeOf.IsBuiltIn(): // enum based attribute type: frind and return enum id by enum code

			f, err := ea.typeOf.itemCodeToId(msgName, false)
			if err != nil {
				return nil, err
			}

			fd[k] = func(src string) (interface{}, error) { // convereter retrun enum id by code
				return f(src)
			}

		case ea.typeOf.IsFloat(): // float types, only 64 or 32 bits supported

			n := 64
			if ea.typeOf.IsFloat32() {
				n = 32
			}

			fd[k] = func(src string) (interface{}, error) { // convereter retrun enum id by code
				vf, e := strconv.ParseFloat(src, n)
				if e != nil {
					return 0.0, e
				}
				return vf, nil
			}

		case ea.typeOf.IsBool():
			fd[k] = func(src string) (interface{}, error) { return strconv.ParseBool(src) }
		case ea.typeOf.IsString():
			fd[k] = func(src string) (interface{}, error) { return src, nil }
		case ea.typeOf.IsInt():
			fd[k] = func(src string) (interface{}, error) { return strconv.Atoi(src) }
		default:
			return nil, errors.New("invalid (not supported) entity attribute type: " + msgName)
		}
	}

	// return converter from csv strings into microdata key and attribute values
	cvt := func(row []string) (interface{}, error) {

		cell := CellMicro{Attr: make([]attrValue, nAttr)}

		n := len(cell.Attr)
		if n != nAttr || len(row) != n+1 {
			return nil, errors.New("invalid size of csv row, expected: " + strconv.Itoa(nAttr+1) + ": " + cellCvt.EntityName)
		}

		// convert microdata key, it is uint 64 bit
		if row[0] == "" || row[0] == "null" {
			return nil, errors.New("invalid microdata key, it cannot be NULL: " + cellCvt.EntityName)
		}

		mKey, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil {
			return nil, err
		}
		cell.Key = mKey

		// convert attributes
		for k := 0; k < nAttr; k++ {

			cell.Attr[k].IsNull = row[k+1] == "" || row[k+1] == "null"

			if !cell.Attr[k].IsNull {
				v, e := fd[k](row[k+1])
				if e != nil {
					return nil, err
				}
				cell.Attr[k].Value = v
			}
		}

		return cell, nil
	}
	return cvt, nil
}

// return entity metadata by entity name
func (cellCvt CellMicroConverter) entityByName() (*EntityMeta, error) {

	if cellCvt.theEntity != nil {
		return cellCvt.theEntity, nil // entity already found
	}

	// validate parameters
	if cellCvt.ModelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if cellCvt.EntityName == "" {
		return nil, errors.New("invalid (empty) entity name")
	}

	// find entity index by name
	idx, ok := cellCvt.ModelDef.EntityByName(cellCvt.EntityName)
	if !ok {
		return nil, errors.New("entity not found: " + cellCvt.EntityName)
	}
	cellCvt.theEntity = &cellCvt.ModelDef.Entity[idx]

	return cellCvt.theEntity, nil
}

// return entity metadata by entity name and entity generation attributes by generation Hid
func (cellCvt CellMicroConverter) entityAttrs() (*EntityMeta, []EntityAttrRow, error) {

	if cellCvt.theEntity != nil && len(cellCvt.theAttrs) > 0 {
		return cellCvt.theEntity, cellCvt.theAttrs, nil // attributes already found
	}
	// validate parameters
	if cellCvt.RunDef == nil {
		return nil, []EntityAttrRow{}, errors.New("invalid (empty) entity microdata generation, look like model run not found or there is no microdata: " + cellCvt.EntityName)
	}

	// find entity by name and entity generation by Hid
	ent, err := cellCvt.entityByName()
	if err != nil {
		return nil, []EntityAttrRow{}, err
	}

	// find entity generation in model run
	idx, ok := cellCvt.RunDef.EntityGenByGenHid(cellCvt.GenHid)
	if !ok {
		return nil, []EntityAttrRow{}, errors.New("entity microdata generation not found: " + strconv.Itoa(cellCvt.GenHid) + " " + cellCvt.EntityName)
	}
	entGen := &cellCvt.RunDef.EntityGen[idx]

	// collect generation attribues
	attrs := make([]EntityAttrRow, len(entGen.GenAttr))

	for k, ga := range entGen.GenAttr {

		aIdx, ok := ent.AttrByKey(ga.AttrId)
		if !ok {
			return nil, []EntityAttrRow{}, errors.New("entity attribute not found by id: " + strconv.Itoa(ga.AttrId) + " " + cellCvt.EntityName)
		}
		attrs[k] = ent.Attr[aIdx]
	}
	cellCvt.theAttrs = attrs

	return ent, cellCvt.theAttrs, nil
}
