// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"container/list"
	"encoding/csv"
	"io"
	"net/http"
	"path"
	"strings"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// profileReplaceHandler replace existing or insert new profile and all profile options.
// PATCH /api/model/:model/profile
// POST /api/model-profile?model=modelNameOrDigest
// Json content: same as return of GET /api/model/:model/profile/:profile.
// Existing profile rows deleted from database and replaced with new content.
func profileReplaceHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")

	var pm db.ProfileMeta
	if !jsonRequestDecode(w, r, &pm) {
		return // error at json decode, response done with http error
	}

	// replace profile in model catalog
	ok, err := theCatalog.ReplaceProfile(dn, &pm)
	if err != nil {
		omppLog.Log(err.Error())
		http.Error(w, "Profile update failed: "+pm.Name, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/profile/"+pm.Name) // respond with model and profile location
	}
}

// profileDeleteHandler delete profile and all profile options:
// DELETE /api/model/:model/profile/:profile
// POST /api/model-profile-delete?model=modelNameOrDigest&profile=profileName
// If multiple models with same name exist then result is undefined.
// If no such profile exist in database then no error, empty operation.
func profileDeleteHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")
	profile := getRequestParam(r, "profile")

	ok, err := theCatalog.DeleteProfile(dn, profile)
	if err != nil {
		http.Error(w, "Profile delete failed "+dn+": "+profile, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/profile/"+profile)
	}
}

// profileOptionReplaceHandler insert new or replace existsing profile and profile option key-value:
// POST /api/model/:model/profile/:profile/key/:key/value/:value
// If multiple models with same name exist then result is undefined.
// If no such profile or option exist in database then new profile and option inserted.
func profileOptionReplaceHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")
	profile := getRequestParam(r, "profile")
	key := getRequestParam(r, "key")
	val := getRequestParam(r, "value")

	ok, err := theCatalog.ReplaceProfileOption(dn, profile, key, val)
	if err != nil {
		http.Error(w, "Profile option update failed: "+profile+": "+key, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/profile/"+profile+"/key/"+key)
	}
}

// profileOptionDeleteHandler delete profile option key-value pair:
// DELETE /api/model/:model/profile/:profile/key/:key
// POST /api/model-profile-key-delete?model=modelNameOrDigest&profile=profileName&key=someKey
// If multiple models with same name exist then result is undefined.
// If no such profile or profile option key exist in database then no error, empty operation.
func profileOptionDeleteHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")
	profile := getRequestParam(r, "profile")
	key := getRequestParam(r, "key")

	ok, err := theCatalog.DeleteProfileOption(dn, profile, key)
	if err != nil {
		http.Error(w, "Profile option delete failed: "+profile+": "+key, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/profile/"+profile+"/key/"+key)
	}
}

// worksetReadonlyUpdateHandler update workset read-only status by model digest-or-name and workset name:
// POST /api/model/:model/workset/:set/readonly/:readonly
// POST /api/workset-readonly?model=modelNameOrDigest&set=setName&readonly=true
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
		w.Header().Set("Content-Location", "/api/model/"+digest+"/workset/"+ws.Name)
	} else {
		ws = &db.WorksetRow{}
	}
	jsonResponse(w, r, ws)
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

	w.Header().Set("Content-Location", "/api/model/"+dn+"/workset/"+wsn) // respond with workset location
}

// worksetDeleteHandler delete workset and workset parameters:
// DELETE /api/model/:model/workset/:set
// POST /api/workset-delete?model=modelNameOrDigest&set=setName
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
		w.Header().Set("Content-Location", "/api/model/"+dn+"/workset/"+wsn)
	}
}

// parameterPageUpdateHandler update a "page" of workset parameter values.
// PATCH /api/model/:model/workset/:set/parameter/:name/new/value
// POST /api/workset-parameter-new-value?model=modelNameOrDigest&set=setName&name=parameterName
// Dimension(s) and enum-based parameters expected to be as enum codes.
// Input parameter "page" json expected to be identical to output of read parameter "page".
func parameterPageUpdateHandler(w http.ResponseWriter, r *http.Request) {
	doUpdateParameterPageHandler(w, r, true)
}

// parameterIdPageUpdateHandler update a "page" of workset parameter values.
// PATCH /api/model/:model/workset/:set/parameter/:name/new/value-id
// POST /api/workset-parameter-new-value-id?model=modelNameOrDigest&set=setName&name=parameterName
// Dimension(s) and enum-based parameters expected to be as enum id, not enum codes.
// Input parameter "page" json expected to be identical to output of read parameter "page".
func parameterIdPageUpdateHandler(w http.ResponseWriter, r *http.Request) {
	doUpdateParameterPageHandler(w, r, false)
}

// doUpdateParameterPageHandler update a "page" of workset parameter values.
// Page is part of parameter values defined by zero-based "start" row number and row count.
// Dimension(s) and enum-based parameters can be as enum codes or enum id's.
func doUpdateParameterPageHandler(w http.ResponseWriter, r *http.Request, isCode bool) {

	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	wsn := getRequestParam(r, "set")   // workset name
	name := getRequestParam(r, "name") // parameter name

	// decode json body and convert to id cells, if required
	cLst := list.New()

	if !isCode {
		var cArr []db.CellParam
		if !jsonRequestDecode(w, r, &cArr) {
			return // error at json decode, response done with http error
		}
		for k := range cArr {
			cLst.PushBack(cArr[k])
		}
	} else {

		// decode code cells from json body
		var cArr []db.CellCodeParam
		if !jsonRequestDecode(w, r, &cArr) {
			return // error at json decode, response done with http error
		}

		// convert from enum code cells to id cells
		cvt, ok := theCatalog.ParameterCellConverter(true, dn, wsn, name)
		if !ok {
			http.Error(w, "Workset parameter update failed "+wsn+": "+name, http.StatusBadRequest)
			return
		}

		for k := range cArr {
			c, err := cvt(cArr[k])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			cLst.PushBack(c)
		}

	}
	if cLst.Len() <= 0 {
		http.Error(w, "Workset parameter update failed "+wsn+" parameter empty: "+name, http.StatusInternalServerError)
		return
	}

	// update parameter values
	err := theCatalog.UpdateWorksetParameterPage(dn, wsn, name, cLst)
	if err != nil {
		omppLog.Log(err.Error())
		http.Error(w, "Workset parameter update failed "+wsn+": "+name, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Location", "/api/model/"+dn+"/workset/"+wsn+"/parameter/"+name) // respond with workset parameter location
}

// worksetParameterDeleteHandler delete workset parameter:
// DELETE /api/model/:model/workset/:set/parameter/:name
// POST /api/workset-parameter-delete?model=modelNameOrDigest&set=setName&parameter=name
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
		w.Header().Set("Content-Location", "/api/model/"+dn+"/workset/"+wsn+"/parameter/"+name)
	}
}

// worksetParameterRunCopyHandler copy parameter into workset from model run:
// PUT /api/model/:model/workset/:set/copy/parameter/:name/from-run/:run
// POST /api/copy-parameter-from-run?model=modelNameOrDigest&set=setName&name=parameterName&run=runNameOrDigest"
// If multiple models with same name exist then result is undefined.
// If such parameter already exist in destination workset then return error.
// Destination workset must be in read-write state.
// Run must be completed, run status one of: s=success, x=exit, e=error.
func worksetParameterRunCopyHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model")  // model digest-or-name
	wsn := getRequestParam(r, "set")   // workset name
	name := getRequestParam(r, "name") // parameter name
	rdn := getRequestParam(r, "run")   // source run digest or name

	// copy workset parameter from model run
	err := theCatalog.CopyParameterToWsFromRun(dn, wsn, name, rdn)
	if err != nil {
		omppLog.Log(err.Error())
		http.Error(w, "Workset parameter copy failed "+wsn+": "+name+" from run: "+rdn, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Location", "/api/model/"+dn+"/workset/"+wsn+"/parameter/"+name)
}

// worksetParameterCopyFromWsHandler copy parameter from one workset to another:
// PUT /api/model/:model/workset/:set/copy/parameter/:name/from-workset/:from-set
// POST /api/copy-parameter-from-workset?model=modelNameOrDigest&set=dstSetName&name=parameterName&from-set=srcSetName"
// If multiple models with same name exist then result is undefined.
// If such parameter already exist in destination workset then return error.
// Destination workset must be in read-write state, source workset must be read-only.
func worksetParameterCopyFromWsHandler(w http.ResponseWriter, r *http.Request) {

	// url or query parameters
	dn := getRequestParam(r, "model")           // model digest-or-name
	dstWsName := getRequestParam(r, "set")      // workset name
	name := getRequestParam(r, "name")          // parameter name
	srcWsName := getRequestParam(r, "from-set") // source run digest or name

	// copy workset parameter from other workset
	err := theCatalog.CopyParameterBetweenWs(dn, dstWsName, name, srcWsName)
	if err != nil {
		omppLog.Log(err.Error())
		http.Error(w, "Workset parameter copy failed "+dstWsName+": "+name+" from run: "+srcWsName, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Location", "/api/model/"+dn+"/workset/"+dstWsName+"/parameter/"+name)
}

// runDeleteHandler delete model run including output table values and run input parameters
// by model digest-or-name and run digest-or-name:
// DELETE /api/model/:model/run/:run
// POST   /api/run-delete?model=modelNameOrDigest&run=runNameOrDigest
// If multiple models with same name exist then result is undefined.
// If multiple runs with same name exist then result is undefined.
// If no such model run exist in database then no error, empty operation.
func runDeleteHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")
	rdn := getRequestParam(r, "run")

	// delete model run
	ok, err := theCatalog.DeleteRun(dn, rdn)
	if err != nil {
		http.Error(w, "Model run delete failed "+dn+": "+rdn, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/run/"+rdn)
	}
}

// runTextMergeHandler merge model run text (description and notes) and run parameter value notes into database.
// PATCH /api/run/text
// POST  /api/run/text
// Model can be identified by digest or name and model run also identified by run digest-or-name.
// If multiple models with same name exist then result is undefined.
// If multiple runs with same name exist then result is undefined.
// If no such model run exist in database then no error, empty operation.
func runTextMergeHandler(w http.ResponseWriter, r *http.Request) {

	// decode json run "public" metadata
	var rp db.RunPub
	if !jsonRequestDecode(w, r, &rp) {
		return // error at json decode, response done with http error
	}

	// update run text in model catalog
	ok, dn, rdn, err := theCatalog.UpdateRunText(&rp)
	if err != nil {
		omppLog.Log(err.Error())
		http.Error(w, "Model run update failed "+dn+": "+rdn, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/run/"+rdn)
	}
}

// taskDeleteHandler do delete modeling task, task run history from database.
// DELETE /api/model/:model/task/:task
// POST   /api/task-delete?model=modelNameOrDigest&task=taskName
// Task run history deleted only from task_run_lst and task_run_set tables,
// it does not delete model runs or any model input sets (worksets).
// If multiple models with same name exist then result is undefined.
// If task does not exists in database then it is empty operation.
// If modeling task is running during delete then result is undefined and model may fail with database error.
func taskDeleteHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")
	tn := getRequestParam(r, "task")

	// delete modeling task
	ok, err := theCatalog.DeleteTask(dn, tn)
	if err != nil {
		http.Error(w, "Task delete failed "+dn+": "+tn, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/task/"+tn)
	}
}

// taskDefReplaceHandler replace task definition: task text (description and notes) and task input worksets into database.
// PUT  /api/task-new
// POST /api/task-new
// It does delete existing and insert new rows into task_txt and task_set db tables.
// If task does not exist then new task created.
// Json body expected to contain TaskDefPub, any other TaskPub data silently ignored.
// Model can be identified by digest or name and model run also identified by run digest-or-name.
// If multiple models with same name exist then result is undefined.
func taskDefReplaceHandler(w http.ResponseWriter, r *http.Request) {
	taskDefUpdateHandler(w, r, true)
}

// taskDefMergeHandler merge task definition: task text (description and notes) and task input worksets into database.
// PATCH /api/task
// POST  /api/task
// It does update existing or insert new rows into task_txt and task_set db tables.
// If task does not exist then new task created.
// Json body expected to contain TaskDefPub, any other TaskPub data silently ignored.
// Model can be identified by digest or name and model run also identified by run digest-or-name.
// If multiple models with same name exist then result is undefined.
func taskDefMergeHandler(w http.ResponseWriter, r *http.Request) {
	taskDefUpdateHandler(w, r, false)
}

// taskDefUpdateHandler replace or merge task definition: task text (description and notes) and task input worksets into database.
// It does replace or merge task_txt and task_set db rows.
// If task does not exist then new task created.
func taskDefUpdateHandler(w http.ResponseWriter, r *http.Request, isReplace bool) {

	// decode json run "public" metadata
	var tpd db.TaskDefPub
	if !jsonRequestDecode(w, r, &tpd) {
		return // error at json decode, response done with http error
	}

	// update task definition in model catalog
	ok, dn, tn, err := theCatalog.UpdateTaskDef(isReplace, &tpd)
	if err != nil {
		omppLog.Log(err.Error())
		http.Error(w, "Modeling task merge failed "+dn+": "+tn, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/task/"+tn)
	}
}
