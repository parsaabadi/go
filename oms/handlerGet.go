// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"go.openmpp.org/ompp/db"
)

// homeHandler is static pages handler for front-end UI served on web / root.
// Only GET requests expected.
func homeHandler(w http.ResponseWriter, r *http.Request) {
	setContentType(http.FileServer(http.Dir(htmlSubDir))).ServeHTTP(w, r)
}

// modelListHandler return list of model_dic rows in json:
// GET /api/model-list-text
func modelListHandler(w http.ResponseWriter, r *http.Request) {

	// list of models digest and for each model in catalog and get model_dic row
	ds := theCatalog.AllModelDigests()

	ml := make([]db.ModelDicRow, len(ds))
	for idx := range ds {
		ml[idx], _ = theCatalog.ModelDicByDigest(ds[idx])
	}

	// write json response
	jsonResponse(w, r, ml)
}

// modelTextListHandler return list of model_dic and model_dic_txt rows in json:
// GET /api/model-list-text/:lang
// GET /api/model-list-text?lang=en
// If optional lang specified then result in that language else in browser language or model default.
func modelTextListHandler(w http.ResponseWriter, r *http.Request) {

	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	// get model name, description, notes
	ds := theCatalog.AllModelDigests()

	mtl := make([]ModelDicDescrNote, 0, len(ds))
	for idx := range ds {
		if mt, ok := theCatalog.ModelTextByDigest(ds[idx], rqLangTags); ok {
			mtl = append(mtl, *mt)
		}
	}

	// write json response
	jsonResponse(w, r, mtl)
}

// modelMetaHandler return language-indepedent model metadata in json:
// GET /api/model?dn=a1b2c3d
// GET /api/model?dn=modelName
// GET /api/model/:dn
// If multiple models with same name exist only one is returned.
func modelMetaHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	m, _ := theCatalog.ModelMetaByDigestOrName(dn)
	jsonResponse(w, r, m)
}

// modelTextHandler return language-specific model metadata in json:
// GET /api/model-text?dn=a1b2c3d
// GET /api/model-text?dn=modelName&lang=en
// GET /api/model-text/:dn
// GET /api/model-text/:dn/:lang
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
// If optional lang specified then result in that language else in browser language or model default.
func modelTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	mt, _ := theCatalog.ModelMetaTextByDigestOrName(dn, rqLangTags)
	jsonResponse(w, r, mt)
}

// langListHandler return list of model langauages in json:
// GET /api/model-lang?dn=a1b2c3d
// GET /api/model-lang?dn=modelName
// GET /api/model-lang/:dn
// If multiple models with same name exist only one is returned.
func langListHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	m, _ := theCatalog.LangListByDigestOrName(dn)
	jsonResponse(w, r, m)
}

// modelGroupHandler return parameter and output table groups (language-neutral part) in json:
// GET /api/model-group?dn=a1b2c3d
// GET /api/model-group?dn=modelName
// GET /api/model-group/:dn
// If multiple models with same name exist only one is returned.
func modelGroupHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	mt, _ := theCatalog.GroupsByDigestOrName(dn)
	jsonResponse(w, r, mt)
}

// modelGroupTextHandler return parameter and output table groups with text (description and notes) in json:
// GET /api/model-group-text?dn=a1b2c3d
// GET /api/model-group-text?dn=modelName&lang=en
// GET /api/model-group-text/:dn
// GET /api/model-group-text/:dn/:lang
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
// If optional lang specified then result in that language else in browser language or model default.
func modelGroupTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	mt, _ := theCatalog.GroupsTextByDigestOrName(dn, rqLangTags)
	jsonResponse(w, r, mt)
}

// modelProfileHandler return profile db rows by model digest and profile name in json:
// GET /api/model-profile?digest=a1b2c3d?name=profileName
// GET /api/model-profile/:digest/:name
// If no such profile exist in database then empty result returned.
func modelProfileHandler(w http.ResponseWriter, r *http.Request) {

	digest := getRequestParam(r, "digest")
	profile := getRequestParam(r, "name")

	mt, _ := theCatalog.ModelProfileByName(digest, profile)
	jsonResponse(w, r, mt)
}

// runStatusHandler return run_lst db row by model digest-or-name and run digest-or-name in json:
// GET /api/run/status?dn=a1b2c3d&rdn=1f2e3d4
// GET /api/run/status?dn=modelName&rdn=runName
// GET /api/run/status/:dn/:rdn
// If multiple models or runs with same name exist only one is returned.
// If no such run exist in database then empty result returned.
func runStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rdn := getRequestParam(r, "rdn")

	rst, _ := theCatalog.RunStatus(dn, rdn)
	jsonResponse(w, r, rst)
}

// firstRunStatusHandler return first run_lst db row by model digest-or-name in json:
// GET /api/run/first-status?dn=a1b2c3d
// GET /api/run/first-status?dn=modelName
// GET /api/run/first-status/:dn
// If multiple models or runs with same name exist only one is returned.
// If no run exist in database then empty result returned.
func firstRunStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	rst, _ := theCatalog.FirstOrLastRunStatus(dn, true)
	jsonResponse(w, r, rst)
}

// lastRunStatusHandler return last run_lst db row by model digest-or-name in json:
// GET /api/run/last-status?dn=a1b2c3d
// GET /api/run/last-status?dn=modelName
// GET /api/run/last-status/:dn
// If multiple models or runs with same name exist only one is returned.
// If no run exist in database then empty result returned.
func lastRunStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	rst, _ := theCatalog.FirstOrLastRunStatus(dn, false)
	jsonResponse(w, r, rst)
}

// lastCompletedRunHandler return last compeleted run_lst and run_txt db rows by model digest-or-name in json:
// GET /api/run/last-completed?dn=a1b2c3d
// GET /api/run/last-completed?dn=modelName&lang=en
// GET /api/run/last-completed/:dn
// GET /api/run/last-completed/:dn/:lang
// Run completed if run status one of: s=success, x=exit, e=error
// If multiple models or runs with same name exist only one is returned.
// If no run exist in database then empty result returned.
// If optional lang specified then result in that language else in browser language.
func lastCompletedRunHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	rp, _ := theCatalog.LastCompletedRunText(dn, rqLangTags)
	jsonResponse(w, r, rp)
}

// runListHandler return list of run_lst and run_txt db rows by model digest-or-name in json:
// GET /api/run-list?dn=a1b2c3d
// GET /api/run-list?dn=modelName&lang=en
// GET /api/run-list/:dn
// GET /api/run-list/:dn/:lang
// If multiple models with same name exist only one is returned.
// If optional lang specified then result in that language else in browser language.
func runListHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	rpl, _ := theCatalog.RunListText(dn, rqLangTags)
	jsonResponse(w, r, rpl)
}

// runTextHandler return full run metadata: run_lst, run_txt, parameter sub-value counts and text db rows
// by model digest-or-name and digest-or-name in json:
// GET /api/run/text?dn=a1b2c3d&rdn=1f2e3d4
// GET /api/run/text?dn=modelName&rdn=runName&lang=en
// GET /api/run/text/:dn/:rdn
// GET /api/run/text/:dn/:rdn/:lang
// If multiple models with same name exist only one is returned.
// It does not return non-completed runs (run in progress).
// Run completed if run status one of: s=success, x=exit, e=error.
// If optional lang specified then result in that language else in browser language.
func runTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rdn := getRequestParam(r, "rdn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	rp, _ := theCatalog.RunTextFull(dn, rdn, rqLangTags)
	jsonResponse(w, r, rp)
}

// worksetStatusHandler return workset_lst db row by model digest-or-name and workset name in json:
// GET /api/workset/status?dn=a1b2c3d&name=mySet
// GET /api/workset/status?dn=modelName&name=mySet
// GET /api/workset/status/:dn/:name
// If multiple models with same name exist only one is returned.
// If no such workset exist in database then empty result returned.
func worksetStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	name := getRequestParam(r, "name")

	wst, _ := theCatalog.WorksetStatus(dn, name)
	jsonResponse(w, r, wst)
}

// worksetDefaultStatusHandler return workset_lst db row of default workset by model digest-or-name in json:
// GET /api/workset/default-status?dn=a1b2c3d
// GET /api/workset/default-status?dn=modelName
// GET /api/workset/default-status/:dn
// If multiple models with same name exist only one is returned.
// If no default workset exist in database then empty result returned.
func worksetDefaultStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	wst, _ := theCatalog.WorksetDefaultStatus(dn)
	jsonResponse(w, r, wst)
}

// worksetHandler return workset_lst and workset_txt db rows by model digest-or-name and workset name in json:
// GET /api/workset?dn=a1b2c3d&name=mySet
// GET /api/workset?dn=modelName&name=mySet&lang=en
// GET /api/workset/:dn/:name
// GET /api/workset-list/:dn/:name/:lang
// If multiple models with same name exist only one is returned.
// If no such workset exist in database then empty result returned.
// If optional lang specified then result in that language else in browser language.
func worksetHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	name := getRequestParam(r, "name")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	wp, _ := theCatalog.WorksetText(dn, name, rqLangTags)
	jsonResponse(w, r, wp)
}

// worksetListHandler return list of workset_lst and workset_txt db rows by model digest-or-name in json:
// GET /api/workset-list?dn=a1b2c3d
// GET /api/workset-list?dn=modelName&lang=en
// GET /api/workset-list/:dn
// GET /api/workset-list/:dn/:lang
// If multiple models with same name exist only one is returned.
// If optional lang specified then result in that language else in browser language.
func worksetListHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	wpl, _ := theCatalog.WorksetListText(dn, rqLangTags)
	jsonResponse(w, r, wpl)
}

// worksetTextHandler return full workset metadata by model digest-or-name and workset name in json:
// GET /api/workset/text?dn=a1b2c3d&name=mySet
// GET /api/workset/text?dn=modelName&name=mySet
// GET /api/workset/text/:dn/:name
// If multiple models with same name exist only one is returned.
// If no such workset exist in database then empty result returned.
// If optional lang specified then result in that language else in browser language.
func worksetTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	name := getRequestParam(r, "name")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	wp, _ := theCatalog.WorksetTextFull(dn, name, rqLangTags)
	jsonResponse(w, r, wp)
}
