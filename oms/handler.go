// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"github.com/husobee/vestigo"
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

	// get optional language argument and languages accepted by browser
	rqLangTags := getRequestLang(r)

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
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
func modelMetaHandler(w http.ResponseWriter, r *http.Request) {

	// get ?dn parameter from url or get router parameter /:dn
	dn := r.URL.Query().Get("dn")
	if dn == "" {
		dn = vestigo.Param(r, "dn")
	}

	// find metadata in catalog and write json response
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

	// get ?dn parameter from url or get router parameter /:dn
	dn := r.URL.Query().Get("dn")
	if dn == "" {
		dn = vestigo.Param(r, "dn")
	}

	// get optional language argument and languages accepted by browser
	rqLangTags := getRequestLang(r)

	// find metadata in catalog and write json response
	mt, _ := theCatalog.ModelMetaTextByDigestOrName(dn, rqLangTags)
	jsonResponse(w, r, mt)
}

// langListHandler return list of model langauages in json:
// GET /api/model-lang?dn=a1b2c3d
// GET /api/model-lang?dn=modelName
// GET /api/model-lang/:dn
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
func langListHandler(w http.ResponseWriter, r *http.Request) {

	// get ?dn parameter from url or get router parameter /:dn
	dn := r.URL.Query().Get("dn")
	if dn == "" {
		dn = vestigo.Param(r, "dn")
	}

	// find model languages in catalog and write json response
	m, _ := theCatalog.LangListByDigestOrName(dn)
	jsonResponse(w, r, m)
}

// modelGroupHandler return parameter and output table groups (language-neutral part) in json:
// GET /api/model-group?dn=a1b2c3d
// GET /api/model-group?dn=modelName
// GET /api/model-group/:dn
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
func modelGroupHandler(w http.ResponseWriter, r *http.Request) {

	// get ?dn parameter from url or get router parameter /:dn
	dn := r.URL.Query().Get("dn")
	if dn == "" {
		dn = vestigo.Param(r, "dn")
	}

	// find group info in catalog and write json response
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

	// get ?dn parameter from url or get router parameter /:dn
	dn := r.URL.Query().Get("dn")
	if dn == "" {
		dn = vestigo.Param(r, "dn")
	}

	// get optional language argument and languages accepted by browser
	rqLangTags := getRequestLang(r)

	// find group info in catalog and write json response
	mt, _ := theCatalog.GroupsTextByDigestOrName(dn, rqLangTags)
	jsonResponse(w, r, mt)
}
