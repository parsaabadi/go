// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/csv"
	"io"
	"net/http"
	"path"
	"strings"

	"go.openmpp.org/ompp/db"
)

// worksetReadonlyUpdateHandler update workset read-only status by model digest-or-name and workset name:
// POST /api/workset-readonly?model=modelOne&set=mySet&readonly=true
// POST /api/model/:model/workset/:set/readonly/:readonly
// If multiple models with same name exist then result is undefined.
// If no such workset exist in database then empty result returned.
func worksetReadonlyUpdateHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")
	wsn := getRequestParam(r, "set")

	// convert readonly flag
	isReadonly, ok := getBoolRequestParam(r, "readonly")
	if !ok {
		http.Error(w, "Invalid value of workset read-only flag "+wsn, http.StatusBadRequest)
		return
	}

	// update workset read-only status
	digest, ws, ok := theCatalog.UpdateWorksetReadonly(dn, wsn, isReadonly)
	if ok {
		w.Header().Set("Location", "/api/model/"+digest+"/workset/"+ws.Name)
	}
}

// worksetDeleteHandler delete workset and workset parameters:
// DELETE /api/model/:model/workset/:set
// POST /api/model/:model/workset/:set/delete
// POST /api/workset/delete?model=modelOne&set=mySet
// If multiple models with same name exist then result is undefined.
// If no such workset exist in database then no error, empty operation.
func worksetDeleteHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")
	wsn := getRequestParam(r, "set")

	// update workset metadata
	ok, err := theCatalog.DeleteWorkset(dn, wsn)
	if err != nil {
		http.Error(w, "Workset delete failed "+dn+": "+wsn, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Location", "/api/model/"+dn+"/workset/"+wsn)
	}
}

// worksetReplaceHandler replace workset and all parameters from multipart-form:
// PUT /api/workset-new
// POST /api/workset-new
// Expected multipart form parts:
// first workset part with workset metadata in json
// and multiple parts file-csv=parameterName.csv.
// Json keys: model digest or name and workset name.
// Json content: workset "public" metadata.
// If multiple models with same name exist then result is undefined, it is recommended to use model digest.
// If no such workset exist in database then new workset created.
// If workset already exist then it is delete-insert operation:
// existing metadata, parameter list, parameter metadata and parameter values deleted from database
// and new metadata, parameters metadata and parameters values inserted.
func worksetReplaceHandler(w http.ResponseWriter, r *http.Request) {
	worksetUpdateHandler(true, w, r)
}

// worksetMergeHandler merge workset metadata and parameters metadata and values from multipart-form:
// PATCH /api/workset
// POST /api/workset
// Expected multipart form parts:
// first workset part with workset metadata in json
// and optional multiple parts file-csv=parameterName.csv.
// Json keys: model digest or name and workset name.
// Json content: workset "public" metadata.
// If multiple models with same name exist then result is undefined, it is recommended to use model digest.
// If no such workset exist in database then new workset created.
// If workset already exist then merge operation existing workset metadata with new.
// If workset not exist then create new workset.
// Merge parameter list: if parameter exist then merge metadata.
// If new parameter values supplied then replace paramter values.
// If parameter not already exist in workset then parameter values must be supplied.
func worksetMergeHandler(w http.ResponseWriter, r *http.Request) {
	worksetUpdateHandler(false, w, r)
}

// worksetUpdateHandler replace or merge workset metadata and parameters from multipart-form:
// Expected multipart form parts:
// first workset part with workset metadata in json
// and optional multiple parts file-csv=parameterName.csv.
// Json keys: model digest or name and workset name.
// Json content: workset "public" metadata.
// If parameter not already exist in workset then parameter values must be supplied.
// It is an error to add parameter metadata without parameter values.
func worksetUpdateHandler(isReplace bool, w http.ResponseWriter, r *http.Request) {

	// parse multipart form: first part must be workset metadata
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Error at multipart form open ", http.StatusBadRequest)
		return
	}

	var newWp db.WorksetPub
	if !jsonMultipartDecode(w, mr, "workset", &newWp) {
		return // error at json decode, response done with http error
	}

	// get existing workset metadata
	dn := newWp.ModelDigest
	if dn == "" {
		dn = newWp.ModelName
	}
	wsn := newWp.Name

	oldWp, _, err := theCatalog.WorksetTextFull(dn, wsn, true, nil)
	if err != nil {
		http.Error(w, "Failed to get existing workset metadata "+dn+" : "+wsn, http.StatusBadRequest)
		return
	}

	// make starting list of parameters as new parameters which already exist in workset
	newParamLst := append([]db.ParamRunSetPub{}, newWp.Param...)
	newWp.Param = make([]db.ParamRunSetPub, 0, len(newParamLst))

	for k := range newParamLst {
		for j := range oldWp.Param {

			// if this new parameter already exist in workset then keep it
			if newParamLst[k].Name == oldWp.Param[j].Name {
				newWp.Param = append(newWp.Param, newParamLst[k])
				break
			}
		}
	}

	// update workset metadata, postpone read-only status until update completed
	isReadonly := newWp.IsReadonly
	newWp.IsReadonly = false

	ok, _, err := theCatalog.UpdateWorkset(isReplace, &newWp)
	if err != nil {
		http.Error(w, "Failed update workset metadata "+dn+" : "+wsn+" : "+err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		http.Error(w, "Failed update workset metadata "+dn+" : "+wsn, http.StatusBadRequest)
		return
	}

	// each parameter will be replaced in separate transaction
	// decode multipart form csv files
	pim := make(map[int]bool) // if index of parameter name in the map then csv supplied for that parameter
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break // end of posted data
		}
		if err != nil {
			http.Error(w, "Failed to get next part of multipart form "+dn+" : "+wsn, http.StatusBadRequest)
			return
		}

		// skip non parameter-csv data
		if part.FormName() != "parameter-csv" {
			part.Close()
			continue
		}

		// validate: parameter name must be in the list of workset parameters
		ext := path.Ext(part.FileName())
		if ext != ".csv" {
			part.Close()
			http.Error(w, "Error: parameter file must have .csv extension "+wsn+" : "+part.FileName(), http.StatusBadRequest)
			return
		}
		name := strings.TrimSuffix(path.Base(part.FileName()), ext)

		np := -1
		for k := range newParamLst {
			if name == newParamLst[k].Name {
				np = k
				break
			}
		}
		if np < 0 {
			part.Close()
			http.Error(w, "Error: parameter must be in workset parameters list: "+wsn+" : "+name, http.StatusBadRequest)
			return
		}

		// read csv values and update parameter
		rd := csv.NewReader(part)
		rd.TrimLeadingSpace = true

		_, err = theCatalog.UpdateWorksetParameter(isReplace, &newWp, &newParamLst[np], rd)
		part.Close() // done with csv parameter data
		if err != nil {
			http.Error(w, "Failed update workset parameter "+wsn+" : "+name+" : "+err.Error(), http.StatusBadRequest)
			return
		}
		pim[np] = true // parameter metadata and csv values updated
	}

	// update parameter(s) metadata where parameter csv values not supplied
	for k := range newParamLst {

		if pim[k] {
			continue // parameter metadata already updated together with csv parameter values
		}

		// update only parameter metadata
		_, err = theCatalog.UpdateWorksetParameter(isReplace, &newWp, &newParamLst[k], nil)
		if err != nil {
			http.Error(w, "Failed update workset parameter "+wsn+" : "+newParamLst[k].Name+" : "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// if required make workset read-only
	if isReadonly {
		theCatalog.UpdateWorksetReadonly(dn, wsn, isReadonly)
	}

	w.Header().Set("Location", "/api/model/"+dn+"/workset/"+wsn) // respond with workset location
}

// worksetParameterDeleteHandler delete workset parameter:
// DELETE /api/model/:model/workset/:set/parameter/:name
// POST /api/model/:model/workset/:set/parameter/:name/delete
// POST /api/workset-parameter/delete?model=modelOne&set=mySet&parameter=name
// If multiple models with same name exist then result is undefined.
// If no such parameter or workset exist in database then no error, empty operation.
func worksetParameterDeleteHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")
	wsn := getRequestParam(r, "set")
	name := getRequestParam(r, "name")

	// delete workset parameter
	ok, err := theCatalog.DeleteWorksetParameter(dn, wsn, name)
	if err != nil {
		http.Error(w, "Workset parameter delete failed "+wsn+": "+name, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Location", "/api/model/"+dn+"/workset/"+wsn+"/parameter/"+name)
	}
}
