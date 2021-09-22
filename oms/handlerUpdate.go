// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// profileReplaceHandler replace existing or insert new profile and all profile options.
// PATCH /api/model/:model/profile
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
		w.Header().Set("Content-Type", "text/plain")
	}
}

// profileDeleteHandler delete profile and all profile options:
// DELETE /api/model/:model/profile/:profile
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
		w.Header().Set("Content-Type", "text/plain")
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
		w.Header().Set("Content-Type", "text/plain")
	}
}

// profileOptionDeleteHandler delete profile option key-value pair:
// DELETE /api/model/:model/profile/:profile/key/:key
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
		w.Header().Set("Content-Type", "text/plain")
	}
}

// runDeleteHandler delete model run including output table values and run input parameters
// by model digest-or-name and run digest-or-stamp-or-name:
// DELETE /api/model/:model/run/:run
// If multiple models with same name exist then result is undefined.
// If multiple runs with same stamp or name exist then result is undefined.
// If no such model run exist in database then no error, empty operation.
func runDeleteHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "model")
	rdsn := getRequestParam(r, "run")

	// delete model run
	ok, err := theCatalog.DeleteRun(dn, rdsn)
	if err != nil {
		http.Error(w, "Model run delete failed "+dn+": "+rdsn, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/run/"+rdsn)
		w.Header().Set("Content-Type", "text/plain")
	}
}

// runTextMergeHandler merge model run text (description and notes) and run parameter value notes into database.
// PATCH /api/run/text
// Model can be identified by digest or name and model run also identified by run digest-or-stamp-or-name.
// If multiple models with same name exist then result is undefined.
// If multiple runs with same stamp or name exist then result is undefined.
// If no such model run exist in database then no error, empty operation.
func runTextMergeHandler(w http.ResponseWriter, r *http.Request) {

	// decode json run "public" metadata
	var rp db.RunPub
	if !jsonRequestDecode(w, r, &rp) {
		return // error at json decode, response done with http error
	}

	// update run text in model catalog
	ok, dn, rdsn, err := theCatalog.UpdateRunText(&rp)
	if err != nil {
		omppLog.Log(err.Error())
		http.Error(w, "Model run update failed "+dn+": "+rdsn, http.StatusBadRequest)
		return
	}
	if ok {
		w.Header().Set("Content-Location", "/api/model/"+dn+"/run/"+rdsn)
		w.Header().Set("Content-Type", "text/plain")
	}
}

// taskDeleteHandler do delete modeling task, task run history from database.
// DELETE /api/model/:model/task/:task
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
		w.Header().Set("Content-Type", "text/plain")
	}
}

// taskDefReplaceHandler replace task definition: task text (description and notes) and task input worksets into database.
// PUT  /api/task-new
// It does delete existing and insert new rows into task_txt and task_set db tables.
// If task does not exist then new task created.
// If task name is empty in json request then automatically generate unique task name.
// Json body expected to contain TaskDefPub, any other TaskPub data silently ignored.
// Model can be identified by digest or name and model run also identified by run digest-or-name.
// If multiple models with same name exist then result is undefined.
func taskDefReplaceHandler(w http.ResponseWriter, r *http.Request) {
	taskDefUpdateHandler(w, r, true)
}

// taskDefMergeHandler merge task definition: task text (description and notes) and task input worksets into database.
// PATCH /api/task
// It does update existing or insert new rows into task_txt and task_set db tables.
// If task does not exist then new task created.
// If task name is empty in json request then automatically generate unique task name.
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

	// if task name is empty then automatically generate name
	if tpd.Name == "" {
		ts, _ := theCatalog.getNewTimeStamp()
		tpd.Name = "task_" + ts
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
		jsonResponse(w, r,
			struct {
				Name string // task name
			}{
				Name: tn,
			},
		)
	}
}
