// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// set json response headers: Content-Type: application/json
func jsonSetHeaders(w http.ResponseWriter, r *http.Request) {

	// if Content-Type not set then use json
	if _, isSet := w.Header()["Content-Type"]; !isSet {
		w.Header().Set("Content-Type", "application/json")
	}

	// if request from localhost then allow response to any protocol or port
	/*
		if strings.HasPrefix(r.Host, "localhost") {
			if _, isSet := w.Header()["Access-Control-Allow-Origin"]; !isSet {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			}
		}
	*/
}

// jsonResponse set json response headers and writes src as json into w response writer.
// On error it writes 500 internal server error response.
func jsonResponse(w http.ResponseWriter, r *http.Request, src interface{}) {

	jsonSetHeaders(w, r)

	err := json.NewEncoder(w).Encode(src)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// jsonResponseBytes set json response headers and writes src bytes into w response writer.
// If src bytes empty then "{}" is written.
func jsonResponseBytes(w http.ResponseWriter, r *http.Request, src []byte) {

	jsonSetHeaders(w, r)

	if len(src) <= 0 {
		w.Write([]byte("{}"))
	}
	w.Write(src)
}

// jsonCellWriter writes each row of data page into response as list of comma separated json values.
// It is an append to response and response headers must already be set.
func jsonCellWriter(w http.ResponseWriter, enc *json.Encoder, cvtCell func(interface{}) (interface{}, error)) func(src interface{}) (bool, error) {

	isNext := false

	// write data page into response as list of comma separated json values
	cvtWr := func(src interface{}) (bool, error) {

		if isNext {
			w.Write([]byte{','}) // until the last separate array items with , comma
		}

		val := src // id's cell
		var err error

		// convert cell from id's to code if converter specified
		if cvtCell != nil {
			if val, err = cvtCell(src); err != nil {
				return false, err
			}
		}

		// write actual value
		if err := enc.Encode(val); err != nil {
			return false, err
		}
		isNext = true
		return true, nil
	}
	return cvtWr
}

// jsonRequestDecode validate Content-Type: application/json and decode json body.
// Destination for json decode: dst must be a pointer.
// If isRequired is true then json body is required else it can be empty by default.
// On error it writes error response 400 or 415 and return false.
func jsonRequestDecode(w http.ResponseWriter, r *http.Request, isRequired bool, dst interface{}) bool {

	// json body expected
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "Expected Content-Type: application/json", http.StatusUnsupportedMediaType)
		return false
	}

	// decode json
	err := json.NewDecoder(r.Body).Decode(dst)
	if err != nil {
		if err == io.EOF {
			if !isRequired {
				return true // empty default values
			} else {
				http.Error(w, "Invalid (empty) json at "+r.URL.String(), http.StatusBadRequest)
				return false
			}
		}
		omppLog.Log("Json decode error at " + r.URL.String() + ": " + err.Error())
		http.Error(w, "Json decode error at "+r.URL.String(), http.StatusBadRequest)
		return false
	}
	return true // completed OK
}

// jsonMultipartDecode decode json part of multipart form reader.
// It does move to next part, check part form name and decode json content from part body.
// Destination for json decode: dst must be a pointer.
// On error it writes error response 400 or 415 and return false.
func jsonMultipartDecode(w http.ResponseWriter, mr *multipart.Reader, name string, dst interface{}) bool {

	// open next part and check part name
	part, err := mr.NextPart()
	if err == io.EOF {
		http.Error(w, "Invalid (empty) next part of multipart form "+name, http.StatusBadRequest)
		return false
	}
	if err != nil {
		http.Error(w, "Failed to get next part of multipart form "+name+" : "+err.Error(), http.StatusBadRequest)
		return false
	}
	defer part.Close()

	if part.FormName() != name {
		http.Error(w, "Invalid part of multipart form, expected name: "+name, http.StatusBadRequest)
		return false
	}

	// decode json
	err = json.NewDecoder(part).Decode(dst)
	if err != nil {
		if err == io.EOF {
			http.Error(w, "Invalid (empty) json part of multipart form "+name, http.StatusBadRequest)
			return false
		}
		http.Error(w, "Json decode error at part of multipart form "+name, http.StatusBadRequest)
		return false
	}
	return true // completed OK
}

// jsonRequestToFile validate Content-Type: application/json and copy request body into output file as is.
// If output file already exists then it is truncated.
// On error it writes error response 400 or 500 and return false.
func jsonRequestToFile(w http.ResponseWriter, r *http.Request, outPath string) bool {

	// json body expected
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "Expected Content-Type: application/json", http.StatusUnsupportedMediaType)
		return false
	}

	// create or truncate output file
	fName := filepath.Base(outPath)

	err := helper.SaveTo(outPath, r.Body)
	if err != nil {
		omppLog.Log("Error: unable to write into ", outPath, err)
		http.Error(w, "Error: unable to write into "+fName, http.StatusInternalServerError)
		return false
	}
	return true // completed OK
}

// model type metadata, "unpacked" during marshaled to json (range enums restored)
type typeMetaUnpack struct {
	TypeDicRow *db.TypeDicRow   // model type rows: type_dic join to model_type_dic
	Enum       []db.TypeEnumRow // type enum rows: type_enum_lst join to model_type_dic
}

// copy of ModelMeta, using alias for TypeMeta to do a special range type marshaling
type modelMetaUnpack struct {
	Model       *db.ModelDicRow      // model_dic table row
	Type        []typeMetaUnpack     // types metadata: type name and enums
	Param       []db.ParamMeta       // parameters metadata: parameter name, type, dimensions
	Table       []db.TableMeta       // output tables metadata: table name, dimensions, accumulators, expressions
	Entity      []db.EntityMeta      // model entities and attributes
	Group       []db.GroupMeta       // groups of parameters or output tables
	EntityGroup []db.EntityGroupMeta // groups of entity attributes
}

func copyModelMetaToUnpack(meta *db.ModelMeta) *modelMetaUnpack {
	if meta == nil {
		return nil
	}

	mcp := modelMetaUnpack{
		Model:       &meta.Model,
		Type:        make([]typeMetaUnpack, len(meta.Type)),
		Param:       meta.Param,
		Table:       meta.Table,
		Entity:      meta.Entity,
		Group:       meta.Group,
		EntityGroup: meta.EntityGroup,
	}
	for k := range meta.Type {
		mcp.Type[k].TypeDicRow = &meta.Type[k].TypeDicRow
		mcp.Type[k].Enum = meta.Type[k].Enum
	}
	return &mcp
}

// marshal type row and type enums[] to json, "unpack" range enums which may be not loaded from database
func (src *typeMetaUnpack) MarshalJSON() ([]byte, error) {

	tm := struct {
		*db.TypeDicRow
		Enum []db.TypeEnumRow
	}{
		TypeDicRow: src.TypeDicRow,
		Enum:       src.Enum,
	}

	// if it is a range and enums not loaded from database then create enums
	if tm.IsRange && len(tm.Enum) <= 0 {

		n := 1 + tm.MaxEnumId - tm.MinEnumId
		tm.Enum = make([]db.TypeEnumRow, n)

		for k := 0; k < n; k++ {

			nId := tm.MinEnumId + k
			tm.Enum[k] = db.TypeEnumRow{
				ModelId: tm.ModelId,
				TypeId:  tm.TypeId,
				EnumId:  nId,
				Name:    strconv.Itoa(nId),
			}
		}
	}

	return json.Marshal(tm)
}

type aDescrNote struct {
	LangCode *string // lang_code VARCHAR(32)  NOT NULL
	Descr    *string // descr     VARCHAR(255) NOT NULL
	Note     *string // note      VARCHAR(32000)
}

// typeEnumDescrNote is join of type_enum_lst, model_type_dic, type_enum_txt
type typeEnumDescrNote struct {
	Enum      *db.TypeEnumRow // type enum row: type_enum_lst join to model_type_dic
	DescrNote aDescrNote      // from type_enum_txt
}

// TypeDescrNote is join of type_dic_txt, model_type_dic, type_dic_txt
type typeUnpackDescrNote struct {
	Type        *db.TypeDicRow      // model type row: type_dic join to model_type_dic
	DescrNote   *aDescrNote         // from type_dic_txt
	TypeEnumTxt []typeEnumDescrNote // type enum text rows: type_enum_txt join to model_type_dic
	langCode    string              // language for description and notes
}

// marshal type text metadata to json, "unpack" range enums which may be not loaded from database
func (src *typeUnpackDescrNote) MarshalJSON() ([]byte, error) {

	tm := struct {
		Type        *db.TypeDicRow
		DescrNote   *aDescrNote
		TypeEnumTxt []typeEnumDescrNote // type enum text rows: type_enum_txt join to model_type_dic
	}{
		Type:        src.Type,
		DescrNote:   src.DescrNote,
		TypeEnumTxt: src.TypeEnumTxt,
	}
	// if type not a range or enums loaded from database then use standard json marshal
	if !tm.Type.IsRange {
		return json.Marshal(tm)
	}
	if len(tm.TypeEnumTxt) > 0 {
		return json.Marshal(tm) // all range enums are loaded from database
	}
	// else it is a range type and there no enums: marshal array of [min, max] enum Id, Name, Descr

	n := 1 + (tm.Type.MaxEnumId - tm.Type.MinEnumId)
	tm.TypeEnumTxt = make([]typeEnumDescrNote, n)
	emptyNote := ""

	for k := 0; k < n; k++ {

		nId := k + tm.Type.MinEnumId
		et := typeEnumDescrNote{
			Enum: &db.TypeEnumRow{
				ModelId: tm.Type.ModelId,
				TypeId:  tm.Type.TypeId,
				EnumId:  nId,
				Name:    strconv.Itoa(nId),
			},
			DescrNote: aDescrNote{
				LangCode: &src.langCode,
				Note:     &emptyNote,
			},
		}
		et.DescrNote.Descr = &et.Enum.Name // for range type enum code same as description and same as enum id

		tm.TypeEnumTxt[k] = et
	}

	return json.Marshal(tm)
}

// marshal enum text to json, if not description or notes not epty, otherwise return "DescrNote": null
func (src typeEnumDescrNote) MarshalJSON() ([]byte, error) {

	tm := struct {
		Enum      *db.TypeEnumRow
		DescrNote *aDescrNote
	}{
		Enum: src.Enum,
	}
	if src.DescrNote.Descr != nil && *src.DescrNote.Descr != "" || src.DescrNote.Note != nil && *src.DescrNote.Note != "" {
		tm.DescrNote = &src.DescrNote
	}

	return json.Marshal(tm)
}
