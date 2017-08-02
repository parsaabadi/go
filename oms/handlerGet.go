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

// modelListHandler return list of model_dic rows:
// GET /api/model-list
// GET /api/model-list/
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

// modelTextListHandler return list of model_dic and model_dic_txt rows:
// GET /api/model-list-text?lang=en
// GET /api/model-list-text/
// GET /api/model-list/text
// GET /api/model-list/text/lang/:lang
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

// modelMetaHandler return language-indepedent model metadata:
// GET /api/model?dn=a1b2c3d
// GET /api/model?dn=modelName
// GET /api/model/:dn
// If multiple models with same name exist only one is returned.
func modelMetaHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	m, _ := theCatalog.ModelMetaByDigestOrName(dn)
	jsonResponse(w, r, m)
}

// modelTextHandler return language-specific model metadata:
// GET /api/model-text?dn=a1b2c3d
// GET /api/model-text?dn=modelName&lang=en
// GET /api/model/:dn/text
// GET /api/model/:dn/text/lang/:lang
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
// If optional lang specified then result in that language else in browser language or model default.
func modelTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	mt, _ := theCatalog.ModelMetaTextByDigestOrName(dn, rqLangTags)
	jsonResponse(w, r, mt)
}

// modelAllTextHandler return language-specific model metadata:
// GET /api/model-text-all?dn=a1b2c3d
// GET /api/model-text-all?dn=modelName
// GET /api/model/:dn/text/all
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
// Text rows returned in all languages.
func modelAllTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	// find model language-neutral metadata by digest or name
	mf := &ModelMetaFull{}

	m, ok := theCatalog.ModelMetaByDigestOrName(dn)
	if !ok {
		jsonResponse(w, r, mf)
		return // empty result: digest not found
	}
	mf.ModelMeta = *m

	// find model language-specific metadata by digest
	if t, ok := theCatalog.ModelMetaAllTextByDigest(mf.ModelMeta.Model.Digest); ok {
		mf.ModelTxtMeta = *t
	}

	// write json response
	jsonResponse(w, r, mf)
}

// langListHandler return list of model langauages:
// GET /api/lang-list?dn=a1b2c3d
// GET /api/lang-list?dn=modelName
// GET /api/model/:dn/lang-list
// If multiple models with same name exist only one is returned.
func langListHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	m, _ := theCatalog.LangListByDigestOrName(dn)
	jsonResponse(w, r, m)
}

// modelGroupHandler return parameter and output table groups (language-neutral part):
// GET /api/model-group?dn=a1b2c3d
// GET /api/model-group?dn=modelName
// GET /api/model/:dn/group
// If multiple models with same name exist only one is returned.
func modelGroupHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	mt, _ := theCatalog.GroupsByDigestOrName(dn)
	jsonResponse(w, r, mt)
}

// modelGroupTextHandler return parameter and output table groups with text (description and notes):
// GET /api/model-group-text?dn=a1b2c3d
// GET /api/model-group-text?dn=modelName&lang=en
// GET /api/model/:dn/group/text
// GET /api/model/:dn/group/text/lang/:lang
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
// If optional lang specified then result in that language else in browser language or model default.
func modelGroupTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	mt, _ := theCatalog.GroupsTextByDigestOrName(dn, rqLangTags)
	jsonResponse(w, r, mt)
}

// modelGroupTextHandler return parameter and output table groups with text (description and notes):
// GET /api/model-group-text-all?dn=a1b2c3d
// GET /api/model-group-text-all?dn=modelName
// GET /api/model/:dn/group/text/all
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
// Text rows returned in all languages.
func modelGroupAllTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	mt, _ := theCatalog.GroupsAllTextByDigestOrName(dn)
	jsonResponse(w, r, mt)
}

// modelProfileHandler return profile db rows by model digest and profile name:
// GET /api/model-profile?digest=a1b2c3d&name=profileName
// GET /api/model/:digest/profile/:name
// If no such profile exist in database then empty result returned.
func modelProfileHandler(w http.ResponseWriter, r *http.Request) {

	digest := getRequestParam(r, "digest")
	profile := getRequestParam(r, "name")

	mt, _ := theCatalog.ModelProfileByName(digest, profile)
	jsonResponse(w, r, mt)
}

// runListHandler return list of run_lst and run_txt db rows by model digest-or-name:
// GET /api/run-list?dn=a1b2c3d
// GET /api/run-list?dn=modelName
// GET /api/model/:dn/run-list
// If multiple models with same name exist only one is returned.
func runListHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	rpl, _ := theCatalog.RunList(dn)
	jsonResponse(w, r, rpl)
}

// runListHandler return list of run_lst and run_txt db rows by model digest-or-name:
// GET /api/run-list-text?dn=a1b2c3d
// GET /api/run-list-text?dn=modelName&lang=en
// GET /api/model/:dn/run-list/text
// GET /api/model/:dn/run-list/text/lang/:lang
// If multiple models with same name exist only one is returned.
// If optional lang specified then result in that language else in browser language.
func runListTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	rpl, _ := theCatalog.RunListText(dn, rqLangTags)
	jsonResponse(w, r, rpl)
}

// runStatusHandler return run_lst db row by model digest-or-name and run digest-or-name:
// GET /api/run-status?dn=a1b2c3d&rdn=1f2e3d4
// GET /api/run-status?dn=modelName&rdn=runName
// GET /api/model/:dn/run/:rdn/status
// If multiple models or runs with same name exist only one is returned.
// If no such run exist in database then empty result returned.
func runStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rdn := getRequestParam(r, "rdn")

	rst, _ := theCatalog.RunStatus(dn, rdn)
	jsonResponse(w, r, rst)
}

// firstRunStatusHandler return first run_lst db row by model digest-or-name:
// GET /api/run-first-status?dn=a1b2c3d
// GET /api/run-first-status?dn=modelName
// GET /api/model/:dn/run/status/first
// If multiple models or runs with same name exist only one is returned.
// If no run exist in database then empty result returned.
func firstRunStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	rst, _ := theCatalog.FirstOrLastRunStatus(dn, true)
	jsonResponse(w, r, rst)
}

// lastRunStatusHandler return last run_lst db row by model digest-or-name:
// GET /api/run-last-status?dn=a1b2c3d
// GET /api/run-last-status?dn=modelName
// GET /api/model/:dn/run/status/last
// If multiple models or runs with same name exist only one is returned.
// If no run exist in database then empty result returned.
func lastRunStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	rst, _ := theCatalog.FirstOrLastRunStatus(dn, false)
	jsonResponse(w, r, rst)
}

// lastCompletedRunHandler return last compeleted run_lst db row by model digest-or-name:
// GET /api/run-last-completed-status?dn=a1b2c3d
// GET /api/run-last-completed-status?dn=modelName
// GET /api/model/:dn/run/status/last/completed
// Run completed if run status one of: s=success, x=exit, e=error
// If multiple models or runs with same name exist only one is returned.
// If no run exist in database then empty result returned.
func lastCompletedRunStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	rp, _ := theCatalog.LastCompletedRunStatus(dn)
	jsonResponse(w, r, rp)
}

// lastCompletedRunHandler return last compeleted run_lst and run_txt db rows by model digest-or-name:
// GET /api/run-last-completed-text?dn=a1b2c3d
// GET /api/run-last-completed-text?dn=modelName&lang=en
// GET /api/model/:dn/run/last/completed/text
// GET /api/model/:dn/run/last/completed/text/lang/:lang
// Run completed if run status one of: s=success, x=exit, e=error
// If multiple models or runs with same name exist only one is returned.
// If no run exist in database then empty result returned.
// If optional lang specified then result in that language else in browser language.
func lastCompletedRunTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	rp, _ := theCatalog.LastCompletedRunText(dn, false, rqLangTags)
	jsonResponse(w, r, rp)
}

// lastCompletedRulastCompletedRunAllTextHandlernHandler return last compeleted run_lst and run_txt db rows by model digest-or-name:
// GET /api/run-last-completed-text-all?dn=a1b2c3d
// GET /api/run-last-completed-text-all?dn=modelName
// GET /api/model/:dn/run/last/completed/text/all
// Run completed if run status one of: s=success, x=exit, e=error
// If multiple models or runs with same name exist only one is returned.
// If no run exist in database then empty result returned.
// Text rows returned in all languages.
func lastCompletedRunAllTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	rp, _ := theCatalog.LastCompletedRunText(dn, true, nil)
	jsonResponse(w, r, rp)
}

// runTextHandler return full run metadata: run_lst, run_txt, parameter sub-value counts and text db rows
// by model digest-or-name and digest-or-name:
// GET /api/run-text?dn=a1b2c3d&rdn=1f2e3d4
// GET /api/run-text?dn=modelName&rdn=runName&lang=en
// GET /api/model/:dn/run/:rdn/text
// GET /api/model/:dn/run/:rdn/text/
// GET /api/model/:dn/run/:rdn/text/lang/
// GET /api/model/:dn/run/:rdn/text/lang/:lang
// If multiple models with same name exist only one is returned.
// It does not return non-completed runs (run in progress).
// Run completed if run status one of: s=success, x=exit, e=error.
// If optional lang specified then result in that language else in browser language.
func runTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rdn := getRequestParam(r, "rdn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	rp, _ := theCatalog.RunTextFull(dn, rdn, false, rqLangTags)
	jsonResponse(w, r, rp)
}

// runAllTextHandler return full run metadata: run_lst, run_txt, parameter sub-value counts and text db rows
// by model digest-or-name and digest-or-name:
// GET /api/run-text-all?dn=a1b2c3d&rdn=1f2e3d4
// GET /api/run-text-all?dn=modelName&rdn=runName
// GET /api/model/:dn/run/:rdn/text/all
// If multiple models with same name exist only one is returned.
// It does not return non-completed runs (run in progress).
// Run completed if run status one of: s=success, x=exit, e=error.
// Text rows returned in all languages.
func runAllTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rdn := getRequestParam(r, "rdn")

	rp, _ := theCatalog.RunTextFull(dn, rdn, true, nil)
	jsonResponse(w, r, rp)
}

// worksetListHandler return list of workset_lst db rows by model digest-or-name:
// GET /api/workset-list?dn=a1b2c3d
// GET /api/workset-list?dn=modelName
// GET /api/model/:dn/workset-list
// If multiple models with same name exist only one is returned.
func worksetListHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	wpl, _ := theCatalog.WorksetList(dn)
	jsonResponse(w, r, wpl)
}

// worksetListHandler return list of workset_lst and workset_txt db rows by model digest-or-name:
// GET /api/workset-list-text?dn=a1b2c3d
// GET /api/workset-list-text?dn=modelName&lang=en
// GET /api/model/:dn/workset-list/text
// GET /api/model/:dn/workset-list/text/lang/:lang
// If multiple models with same name exist only one is returned.
// If optional lang specified then result in that language else in browser language.
func worksetListTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	wpl, _ := theCatalog.WorksetListText(dn, rqLangTags)
	jsonResponse(w, r, wpl)
}

// worksetStatusHandler return workset_lst db row by model digest-or-name and workset name:
// GET /api/workset-status?dn=a1b2c3d&wsn=mySet
// GET /api/workset-status?dn=modelName&wsn=mySet
// GET /api/model/:dn/workset/:wsn/status
// If multiple models with same name exist only one is returned.
// If no such workset exist in database then empty result returned.
func worksetStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	wsn := getRequestParam(r, "wsn")

	wst, _ := theCatalog.WorksetStatus(dn, wsn)
	jsonResponse(w, r, wst)
}

// worksetDefaultStatusHandler return workset_lst db row of default workset by model digest-or-name:
// GET /api/workset-default-status?dn=a1b2c3d
// GET /api/workset-default-status?dn=modelName
// GET /api/model/:dn/workset/status/default
// If multiple models with same name exist only one is returned.
// If no default workset exist in database then empty result returned.
func worksetDefaultStatusHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")

	wst, _ := theCatalog.WorksetDefaultStatus(dn)
	jsonResponse(w, r, wst)
}

// worksetTextHandler return full workset metadata by model digest-or-name and workset name:
// GET /api/workset-text?dn=a1b2c3d&wsn=mySet
// GET /api/workset-text?dn=modelName&wsn=mySet&lang=en
// GET /api/model/:dn/workset/:wsn/text
// GET /api/model/:dn/workset/:wsn/text/
// GET /api/model/:dn/workset/:wsn/text/lang/
// GET /api/model/:dn/workset/:wsn/text/lang/:lang
// If multiple models with same name exist only one is returned.
// If no such workset exist in database then empty result returned.
// If optional lang specified then result in that language else in browser language.
func worksetTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	wsn := getRequestParam(r, "wsn")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	wp, _ := theCatalog.WorksetText(dn, wsn, false, rqLangTags)
	jsonResponse(w, r, wp)
}

// worksetAllTextHandler return full workset metadata by model digest-or-name and workset name:
// GET /api/workset-text-all?dn=a1b2c3d&wsn=mySet
// GET /api/workset-text-all?dn=modelName&wsn=mySet
// GET /api/model/:dn/workset/:wsn/text/all
// If multiple models with same name exist only one is returned.
// If no such workset exist in database then empty result returned.
// Text rows returned in all languages.
func worksetAllTextHandler(w http.ResponseWriter, r *http.Request) {

	dn := getRequestParam(r, "dn")
	wsn := getRequestParam(r, "wsn")

	wp, _ := theCatalog.WorksetText(dn, wsn, true, nil)
	jsonResponse(w, r, wp)
}

