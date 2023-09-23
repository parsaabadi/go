// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"github.com/husobee/vestigo"
)

// homeHandler is static pages handler for front-end UI served on web / root.
// Only GET requests expected.
func homeHandler(w http.ResponseWriter, r *http.Request) {
	setContentType(http.FileServer(http.Dir(theCfg.htmlDir))).ServeHTTP(w, r)
}

// downloadHandler is static file download handler from user home/io/download and home/io/upload folders.
// Files served from home/io directory URLs are:
//
//	https://domain.name/download/file.name
//	https://domain.name/upload/file.name
//
// Only GET requests expected.
func downloadHandler(w http.ResponseWriter, r *http.Request) {
	setContentType(http.FileServer(http.Dir(theCfg.inOutDir))).ServeHTTP(w, r)
}

// add http GET web-service /api routes to get metadata
func apiGetRoutes(router *vestigo.Router) {

	//
	// GET model definition
	//

	// GET /api/model-list
	router.Get("/api/model-list", modelListHandler, logRequest)

	// GET /api/model-list/text
	// GET /api/model-list/text/lang/:lang
	router.Get("/api/model-list/text", modelTextListHandler, logRequest)
	router.Get("/api/model-list/text/lang/:lang", modelTextListHandler, logRequest)
	router.Get("/api/model-list/text/lang/", http.NotFound)

	// GET /api/model/:model
	router.Get("/api/model/:model", modelMetaHandler, logRequest)
	router.Get("/api/model/", http.NotFound)

	// GET /api/model/:model/text
	// GET /api/model/:model/text/lang/:lang
	router.Get("/api/model/:model/text", modelTextHandler, logRequest)
	router.Get("/api/model/:model/text/lang/:lang", modelTextHandler, logRequest)
	router.Get("/api/model/:model/text/lang/", http.NotFound)

	// GET /api/model/:model/text/all
	router.Get("/api/model/:model/text/all", modelAllTextHandler, logRequest)

	//
	// GET model extra: languages, profile(s)
	//

	// GET /api/model/:model/lang-list
	router.Get("/api/model/:model/lang-list", langListHandler, logRequest)

	// GET /api/model/:model/word-list
	// GET /api/model/:model/word-list/lang/:lang
	router.Get("/api/model/:model/word-list", wordListHandler, logRequest)
	router.Get("/api/model/:model/word-list/lang/:lang", wordListHandler, logRequest)
	router.Get("/api/model/:model/word-list/lang/", http.NotFound)

	// GET /api/model/:model/profile/:profile
	router.Get("/api/model/:model/profile/:profile", modelProfileHandler, logRequest)
	router.Get("/api/model/:model/profile/", http.NotFound)

	// GET /api/model/:model/profile-list
	router.Get("/api/model/:model/profile-list", modelProfileListHandler, logRequest)

	//
	// GET model run results
	//

	// GET /api/model/:model/run-list
	router.Get("/api/model/:model/run-list", runListHandler, logRequest)

	// GET /api/model/:model/run-list/text
	// GET /api/model/:model/run-list/text/lang/:lang
	router.Get("/api/model/:model/run-list/text", runListTextHandler, logRequest)
	router.Get("/api/model/:model/run-list/text/lang/:lang", runListTextHandler, logRequest)
	router.Get("/api/model/:model/run-list/text/lang/", http.NotFound)

	// GET /api/model/:model/run/:run/status
	router.Get("/api/model/:model/run/:run/status", runStatusHandler, logRequest)

	// GET /api/model/:model/run/:run/status/list
	router.Get("/api/model/:model/run/:run/status/list", runStatusListHandler, logRequest)

	// GET /api/model/:model/run/status/first
	router.Get("/api/model/:model/run/status/first", firstRunStatusHandler, logRequest)

	// GET /api/model/:model/run/status/last
	router.Get("/api/model/:model/run/status/last", lastRunStatusHandler, logRequest)

	// GET /api/model/:model/run/status/last-completed
	router.Get("/api/model/:model/run/status/last-completed", lastCompletedRunStatusHandler, logRequest)

	// GET /api/model/:model/run/:run
	router.Get("/api/model/:model/run/:run", runFullHandler, logRequest)
	router.Get("/api/model/:model/run/", http.NotFound)
	router.Get("/api/model/:model/run/:run/", http.NotFound)

	// GET /api/model/:model/run/:run/text
	// GET /api/model/:model/run/:run/text/lang/:lang
	router.Get("/api/model/:model/run/:run/text", runTextHandler, logRequest)
	router.Get("/api/model/:model/run/:run/text/lang/:lang", runTextHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/text/", http.NotFound)
	router.Get("/api/model/:model/run/:run/text/lang/", http.NotFound)

	// GET /api/model/:model/run/:run/text/all
	router.Get("/api/model/:model/run/:run/text/all", runAllTextHandler, logRequest)

	//
	// GET model set of input parameters (workset)
	//

	// GET /api/model/:model/workset-list
	router.Get("/api/model/:model/workset-list", worksetListHandler, logRequest)

	// GET /api/model/:model/workset-list/text
	// GET /api/model/:model/workset-list/text/lang/:lang
	router.Get("/api/model/:model/workset-list/text", worksetListTextHandler, logRequest)
	router.Get("/api/model/:model/workset-list/text/lang/:lang", worksetListTextHandler, logRequest)
	router.Get("/api/model/:model/workset-list/text/lang/", http.NotFound)

	// GET /api/model/:model/workset/:set/status
	router.Get("/api/model/:model/workset/:set/status", worksetStatusHandler, logRequest)

	// GET /api/model/:model/workset/status/default
	router.Get("/api/model/:model/workset/status/default", worksetDefaultStatusHandler, logRequest)

	// GET /api/model/:model/workset/:set/text
	// GET /api/model/:model/workset/:set/text/lang/:lang
	router.Get("/api/model/:model/workset/:set/text", worksetTextHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/text/lang/:lang", worksetTextHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/text/lang/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/text/", http.NotFound)

	// GET /api/model/:model/workset/:set/text/all
	router.Get("/api/model/:model/workset/:set/text/all", worksetAllTextHandler, logRequest)

	//
	// GET modeling tasks and task run history
	//

	// GET /api/model/:model/task-list
	router.Get("/api/model/:model/task-list", taskListHandler, logRequest)

	// GET /api/model/:model/task-list/text
	// GET /api/model/:model/task-list/text/lang/:lang
	router.Get("/api/model/:model/task-list/text", taskListTextHandler, logRequest)
	router.Get("/api/model/:model/task-list/text/lang/:lang", taskListTextHandler, logRequest)
	router.Get("/api/model/:model/task-list/text/lang/", http.NotFound)

	// GET /api/model/:model/task/:task/sets
	router.Get("/api/model/:model/task/:task/sets", taskSetsHandler, logRequest)

	// GET /api/model/:model/task/:task/runs
	router.Get("/api/model/:model/task/:task/runs", taskRunsHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/run/:run
	router.Get("/api/model/:model/task/:task/run-status/run/:run", taskRunStatusHandler, logRequest)
	router.Get("/api/model/:model/task/:task/run-status/run/", http.NotFound)

	// GET /api/model/:model/task/:task/run-status/list/:run
	router.Get("/api/model/:model/task/:task/run-status/list/:run", taskRunStatusListHandler, logRequest)
	router.Get("/api/model/:model/task/:task/run-status/list/", http.NotFound)

	// GET /api/model/:model/task/:task/run-status/first
	router.Get("/api/model/:model/task/:task/run-status/first", firstTaskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/last
	router.Get("/api/model/:model/task/:task/run-status/last", lastTaskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/run-status/last-completed
	router.Get("/api/model/:model/task/:task/run-status/last-completed", lastCompletedTaskRunStatusHandler, logRequest)

	// GET /api/model/:model/task/:task/text
	// GET /api/model/:model/task/:task/text/lang/:lang
	router.Get("/api/model/:model/task/:task/text", taskTextHandler, logRequest)
	router.Get("/api/model/:model/task/:task/text/lang/:lang", taskTextHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/task/:task/", http.NotFound)
	router.Get("/api/model/:model/task/:task/text/", http.NotFound)
	router.Get("/api/model/:model/task/:task/text/lang/", http.NotFound)

	// GET /api/model/:model/task/:task/text/all
	router.Get("/api/model/:model/task/:task/text/all", taskAllTextHandler, logRequest)
}

// add http GET or POST web-service /api routes to read parameters or output tables
func apiReadRoutes(router *vestigo.Router) {

	// POST /api/model/:model/workset/:set/parameter/value
	// POST /api/model/:model/workset/:set/parameter/value-id
	router.Post("/api/model/:model/workset/:set/parameter/value", worksetParameterPageReadHandler, logRequest)
	router.Post("/api/model/:model/workset/:set/parameter/value-id", worksetParameterIdPageReadHandler, logRequest)

	// POST /api/model/:model/run/:run/parameter/value
	// POST /api/model/:model/run/:run/parameter/value-id
	router.Post("/api/model/:model/run/:run/parameter/value", runParameterPageReadHandler, logRequest)
	router.Post("/api/model/:model/run/:run/parameter/value-id", runParameterIdPageReadHandler, logRequest)

	// POST /api/model/:model/run/:run/table/value
	// POST /api/model/:model/run/:run/table/value-id
	router.Post("/api/model/:model/run/:run/table/value", runTablePageReadHandler, logRequest)
	router.Post("/api/model/:model/run/:run/table/value-id", runTableIdPageReadHandler, logRequest)

	// POST /api/model/:model/run/:run/table/calc
	// POST /api/model/:model/run/:run/table/calc-id
	router.Post("/api/model/:model/run/:run/table/calc", runTableCalcPageReadHandler, logRequest)
	router.Post("/api/model/:model/run/:run/table/calc-id", runTableCalcIdPageReadHandler, logRequest)

	// POST /api/model/:model/run/:run/table/compare
	// POST /api/model/:model/run/:run/table/compare-id
	router.Post("/api/model/:model/run/:run/table/compare", runTableComparePageReadHandler, logRequest)
	router.Post("/api/model/:model/run/:run/table/compare-id", runTableCompareIdPageReadHandler, logRequest)

	if theCfg.isMicrodata {

		// POST /api/model/:model/run/:run/microdata/value
		// POST /api/model/:model/run/:run/microdata/value-id
		router.Post("/api/model/:model/run/:run/microdata/value", runMicrodataPageReadHandler, logRequest)
		router.Post("/api/model/:model/run/:run/microdata/value-id", runMicrodataIdPageReadHandler, logRequest)
	}

	// GET /api/model/:model/workset/:set/parameter/:name/value
	// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start
	// GET /api/model/:model/workset/:set/parameter/:name/value/start/:start/count/:count
	router.Get("/api/model/:model/workset/:set/parameter/:name/value", worksetParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start", worksetParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start/count/:count", worksetParameterPageGetHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/workset/:set/parameter/:name/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/", http.NotFound)
	router.Get("/api/model/:model/workset/:set/parameter/:name/value/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/parameter/:name/value
	// GET /api/model/:model/run/:run/parameter/:name/value/start/:start
	// GET /api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count
	router.Get("/api/model/:model/run/:run/parameter/:name/value", runParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start", runParameterPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start/count/:count", runParameterPageGetHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/parameter/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/parameter/:name/value/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/expr
	// GET /api/model/:model/run/:run/table/:name/expr/start/:start
	// GET /api/model/:model/run/:run/table/:name/expr/start/:start/count/:count
	router.Get("/api/model/:model/run/:run/table/:name/expr", runTableExprPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start", runTableExprPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start/count/:count", runTableExprPageGetHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/expr/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/acc/start/:start
	// GET /api/model/:model/run/:run/table/:name/acc/start/:start/count/:count
	router.Get("/api/model/:model/run/:run/table/:name/acc", runTableAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start", runTableAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start/count/:count", runTableAccPageGetHandler, logRequest)
	// reject if request ill-formed
	// router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/acc/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/all-acc
	// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start
	// GET /api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count
	router.Get("/api/model/:model/run/:run/table/:name/all-acc", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start", runTableAllAccPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start/count/:count", runTableAllAccPageGetHandler, logRequest)
	// reject if request ill-formed
	// router.Get("/api/model/:model/run/:run/table/:name/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/start/:start/count/", http.NotFound)

	// GET /api/model/:model/run/:run/table/:name/calc/:calc
	// GET /api/model/:model/run/:run/table/:name/calc/:calc/start/:start
	// GET /api/model/:model/run/:run/table/:name/calc/:calc/start/:start/count/:count
	router.Get("/api/model/:model/run/:run/table/:name/calc/:calc", runTableCalcPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/calc/:calc/start/:start", runTableCalcPageGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/calc/:calc/start/:start/count/:count", runTableCalcPageGetHandler, logRequest)
	// reject if request ill-formed
	router.Get("/api/model/:model/run/:run/table/:name/calc/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/calc/:calc/start/", http.NotFound)
	router.Get("/api/model/:model/run/:run/table/:name/calc/:calc/start/:start/count/", http.NotFound)

	if theCfg.isMicrodata {

		// GET /api/model/:model/run/:run/microdata/:name/value
		// GET /api/model/:model/run/:run/microdata/:name/value/start/:start
		// GET /api/model/:model/run/:run/microdata/:name/value/start/:start/count/:count
		router.Get("/api/model/:model/run/:run/microdata/:name/value", runMicrodatarPageGetHandler, logRequest)
		router.Get("/api/model/:model/run/:run/microdata/:name/value/start/:start", runMicrodatarPageGetHandler, logRequest)
		router.Get("/api/model/:model/run/:run/microdata/:name/value/start/:start/count/:count", runMicrodatarPageGetHandler, logRequest)
		// reject if request ill-formed
		router.Get("/api/model/:model/run/:run/microdata/:name/", http.NotFound)
		router.Get("/api/model/:model/run/:run/microdata/:name/value/", http.NotFound)
		router.Get("/api/model/:model/run/:run/microdata/:name/value/start/", http.NotFound)
		router.Get("/api/model/:model/run/:run/microdata/:name/value/start/:start/count/", http.NotFound)
	}
}

// add http GET web-service /api routes to read parameters or output tables as csv stream
func apiReadCsvRoutes(router *vestigo.Router) {

	// GET /api/model/:model/workset/:set/parameter/:name/csv
	// GET /api/model/:model/workset/:set/parameter/:name/csv-bom
	// GET /api/model/:model/workset/:set/parameter/:name/csv-id
	// GET /api/model/:model/workset/:set/parameter/:name/csv-id-bom
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv", worksetParameterCsvGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv-bom", worksetParameterCsvBomGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv-id", worksetParameterIdCsvGetHandler, logRequest)
	router.Get("/api/model/:model/workset/:set/parameter/:name/csv-id-bom", worksetParameterIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/parameter/:name/csv
	// GET /api/model/:model/run/:run/parameter/:name/csv-bom
	// GET /api/model/:model/run/:run/parameter/:name/csv-id
	// GET /api/model/:model/run/:run/parameter/:name/csv-id-bom
	router.Get("/api/model/:model/run/:run/parameter/:name/csv", runParameterCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-bom", runParameterCsvBomGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-id", runParameterIdCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/parameter/:name/csv-id-bom", runParameterIdCsvBomGetHandler, logRequest)

	if theCfg.isMicrodata {

		// GET /api/model/:model/run/:run/microdata/:name/csv
		// GET /api/model/:model/run/:run/microdata/:name/csv-bom
		// GET /api/model/:model/run/:run/microdata/:name/csv-id
		// GET /api/model/:model/run/:run/microdata/:name/csv-id-bom
		router.Get("/api/model/:model/run/:run/microdata/:name/csv", runMicrodataCsvGetHandler, logRequest)
		router.Get("/api/model/:model/run/:run/microdata/:name/csv-bom", runMicrodataCsvBomGetHandler, logRequest)
		router.Get("/api/model/:model/run/:run/microdata/:name/csv-id", runMicrodataIdCsvGetHandler, logRequest)
		router.Get("/api/model/:model/run/:run/microdata/:name/csv-id-bom", runMicrodataIdCsvBomGetHandler, logRequest)
	}

	// GET /api/model/:model/run/:run/table/:name/expr/csv
	// GET /api/model/:model/run/:run/table/:name/expr/csv-bom
	// GET /api/model/:model/run/:run/table/:name/expr/csv-id
	// GET /api/model/:model/run/:run/table/:name/expr/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv", runTableExprCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-bom", runTableExprCsvBomGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-id", runTableExprIdCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/expr/csv-id-bom", runTableExprIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/acc/csv
	// GET /api/model/:model/run/:run/table/:name/acc/csv-bom
	// GET /api/model/:model/run/:run/table/:name/acc/csv-id
	// GET /api/model/:model/run/:run/table/:name/acc/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv", runTableAccCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-bom", runTableAccCsvBomGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-id", runTableAccIdCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/acc/csv-id-bom", runTableAccIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/all-acc/csv
	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-bom
	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id
	// GET /api/model/:model/run/:run/table/:name/all-acc/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv", runTableAllAccCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-bom", runTableAllAccCsvBomGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-id", runTableAllAccIdCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/all-acc/csv-id-bom", runTableAllAccIdCsvBomGetHandler, logRequest)

	// GET /api/model/:model/run/:run/table/:name/calc/:calc/csv
	// GET /api/model/:model/run/:run/table/:name/calc/:calc/csv-bom
	// GET /api/model/:model/run/:run/table/:name/calc/:calc/csv-id
	// GET /api/model/:model/run/:run/table/:name/calc/:calc/csv-id-bom
	router.Get("/api/model/:model/run/:run/table/:name/calc/:calc/csv", runTableCalcCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/calc/:calc/csv-bom", runTableCalcCsvBomGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/calc/:calc/csv-id", runTableCalcIdCsvGetHandler, logRequest)
	router.Get("/api/model/:model/run/:run/table/:name/calc/:calc/csv-id-bom", runTableCalcIdCsvBomGetHandler, logRequest)
}

// add web-service /api routes to update metadata
func apiUpdateRoutes(router *vestigo.Router) {

	//
	// update profile
	//

	// PATCH /api/model/:model/profile
	router.Patch("/api/model/:model/profile", profileReplaceHandler, logRequest)
	router.Patch("/api/model/:model/profile/", http.NotFound)

	// DELETE /api/model/:model/profile/:profile
	router.Delete("/api/model/:model/profile/:profile", profileDeleteHandler, logRequest)
	router.Delete("/api/model/:model/profile/", http.NotFound)

	// POST /api/model/:model/profile/:profile/key/:key/value/:value
	router.Post("/api/model/:model/profile/:profile/key/:key/value/:value", profileOptionReplaceHandler, logRequest)
	router.Post("/api/model/:model/profile/:profile/key/:key/value/", http.NotFound)

	// DELETE /api/model/:model/profile/:profile/key/:key
	router.Delete("/api/model/:model/profile/:profile/key/:key", profileOptionDeleteHandler, logRequest)
	router.Delete("/api/model/:model/profile/:profile/key/", http.NotFound)

	//
	// update model set of input parameters (workset)
	//

	// POST /api/model/:model/workset/:set/readonly/:readonly
	router.Post("/api/model/:model/workset/:set/readonly/:readonly", worksetReadonlyUpdateHandler, logRequest)
	router.Post("/api/model/:model/workset/:set/readonly/", http.NotFound)

	// PUT  /api/workset-create
	router.Put("/api/workset-create", worksetCreateHandler, logRequest)

	// PUT  /api/workset-replace
	router.Put("/api/workset-replace", worksetReplaceHandler, logRequest)

	// PATCH /api/workset-merge
	router.Patch("/api/workset-merge", worksetMergeHandler, logRequest)

	// DELETE /api/model/:model/workset/:set
	router.Delete("/api/model/:model/workset/:set", worksetDeleteHandler, logRequest)
	router.Delete("/api/model/:model/workset/", http.NotFound)

	// PATCH /api/model/:model/workset/:set/parameter/:name/new/value
	router.Patch("/api/model/:model/workset/:set/parameter/:name/new/value", parameterPageUpdateHandler, logRequest)

	// PATCH /api/model/:model/workset/:set/parameter/:name/new/value-id
	router.Patch("/api/model/:model/workset/:set/parameter/:name/new/value-id", parameterIdPageUpdateHandler, logRequest)

	// DELETE /api/model/:model/workset/:set/parameter/:name
	router.Delete("/api/model/:model/workset/:set/parameter/:name", worksetParameterDeleteHandler, logRequest)
	router.Delete("/api/model/:model/workset/:set/parameter/", http.NotFound)

	// PUT  /api/model/:model/workset/:set/copy/parameter/:name/from-run/:run
	router.Put("/api/model/:model/workset/:set/copy/parameter/:name/from-run/:run", worksetParameterRunCopyHandler, logRequest)
	router.Put("/api/model/:model/workset/:set/copy/parameter/:name/from-run/", http.NotFound)

	// PATCH  /api/model/:model/workset/:set/merge/parameter/:name/from-run/:run
	router.Patch("/api/model/:model/workset/:set/merge/parameter/:name/from-run/:run", worksetParameterRunMergeHandler, logRequest)
	router.Patch("/api/model/:model/workset/:set/merge/parameter/:name/from-run/", http.NotFound)

	// PUT /api/model/:model/workset/:set/copy/parameter/:name/from-workset/:from-set
	router.Put("/api/model/:model/workset/:set/copy/parameter/:name/from-workset/:from-set", worksetParameterCopyFromWsHandler, logRequest)
	router.Put("/api/model/:model/workset/:set/copy/parameter/:name/from-workset/", http.NotFound)

	// PATCH /api/model/:model/workset/:set/merge/parameter/:name/from-workset/:from-set
	router.Patch("/api/model/:model/workset/:set/merge/parameter/:name/from-workset/:from-set", worksetParameterMergeFromWsHandler, logRequest)
	router.Patch("/api/model/:model/workset/:set/merge/parameter/:name/from-workset/", http.NotFound)

	// PATCH /api/model/:model/workset/:set/parameter-text
	router.Patch("/api/model/:model/workset/:set/parameter-text", worksetParameterTextMergeHandler, logRequest)

	//
	// update model run
	//

	// PATCH /api/run/text
	router.Patch("/api/run/text", runTextMergeHandler, logRequest)

	// DELETE /api/model/:model/run/:run
	router.Delete("/api/model/:model/run/:run", runDeleteHandler, logRequest)
	router.Delete("/api/model/:model/run/", http.NotFound)

	// DELETE /api/model/:model/unlink/run/:run
	router.Delete("/api/model/:model/unlink/run/:run", runUnlinkStartHandler, logRequest)
	router.Delete("/api/model/:model/unlink/run/", http.NotFound)

	// PATCH /api/model/:model/run/:run/parameter-text
	router.Patch("/api/model/:model/run/:run/parameter-text", runParameterTextMergeHandler, logRequest)

	//
	// update modeling task and task run history
	//

	// PUT  /api/task-new
	router.Put("/api/task-new", taskDefReplaceHandler, logRequest)

	// PATCH /api/task
	router.Patch("/api/task", taskDefMergeHandler, logRequest)

	// DELETE /api/model/:model/task/:task
	router.Delete("/api/model/:model/task/:task", taskDeleteHandler, logRequest)
	router.Delete("/api/model/:model/task/", http.NotFound)
}

// add web-service /api routes to run the model and monitor progress
func apiRunModelRoutes(router *vestigo.Router) {

	// POST /api/run
	router.Post("/api/run", runModelHandler, logRequest)

	// GET /api/run/log/model/:model/stamp/:stamp
	// GET /api/run/log/model/:model/stamp/:stamp/start/:start/count/:count
	router.Get("/api/run/log/model/:model/stamp/:stamp", runLogPageHandler, logRequest)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start", runLogPageHandler, logRequest)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start/count/:count", runLogPageHandler, logRequest)
	router.Get("/api/run/log/model/:model/stamp/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/", http.NotFound)
	router.Get("/api/run/log/model/:model/stamp/:stamp/start/:start/count/", http.NotFound)

	// PUT /api/run/stop/model/:model/stamp/:stamp
	router.Put("/api/run/stop/model/:model/stamp/:stamp", stopModelHandler, logRequest)
	router.Put("/api/run/stop/model/:model/stamp/", http.NotFound)

	// reject run log if request ill-formed
	router.Get("/api/run/log/model/", http.NotFound)
}

// add http web-service /api routes to download and manage files at home/io/download folder
func apiDownloadRoutes(router *vestigo.Router) {

	// GET /api/download/log/all
	router.Get("/api/download/log/all", allLogDownloadGetHandler, logRequest)

	// GET /api/download/log/model/:model
	router.Get("/api/download/log/model/:model", modelLogDownloadGetHandler, logRequest)
	router.Get("/api/download/log/model/", http.NotFound)

	// GET /api/download/log/file/:name
	router.Get("/api/download/log/file/:name", fileLogDownloadGetHandler, logRequest)
	router.Get("/api/download/log/file/", http.NotFound)

	// GET /api/download/file-tree/:folder
	router.Get("/api/download/file-tree/:folder", fileTreeDownloadGetHandler, logRequest)
	router.Get("/api/download/file-tree/", http.NotFound)

	// POST /api/download/model/:model
	router.Post("/api/download/model/:model", modelDownloadPostHandler, logRequest)
	router.Post("/api/download/model/", http.NotFound)

	// POST /api/download/model/:model/run/:run
	router.Post("/api/download/model/:model/run/:run", runDownloadPostHandler, logRequest)
	router.Post("/api/download/model/:model/run/", http.NotFound)
	router.Post("/api/download/model/run/", http.NotFound)

	// POST /api/download/model/:model/workset/:set
	router.Post("/api/download/model/:model/workset/:set", worksetDownloadPostHandler, logRequest)
	router.Post("/api/download/model/:model/workset/", http.NotFound)
	router.Post("/api/download/model/workset/", http.NotFound)

	// DELETE /api/download/delete/:folder
	router.Delete("/api/download/delete/:folder", downloadDeleteHandler, logRequest)
	router.Delete("/api/download/delete/", http.NotFound)

	// DELETE /api/download/start/delete/:folder
	router.Delete("/api/download/start/delete/:folder", downloadAsyncDeleteHandler, logRequest)
	router.Delete("/api/download/start/delete/", http.NotFound)
}

// add http web-service /api routes to upload and manage files at home/io/upload folder
func apiUploadRoutes(router *vestigo.Router) {

	// GET /api/upload/log/all
	router.Get("/api/upload/log/all", allLogUploadGetHandler, logRequest)

	// GET /api/upload/log/model/:model
	router.Get("/api/upload/log/model/:model", modelLogUploadGetHandler, logRequest)
	router.Get("/api/upload/log/model/", http.NotFound)

	// GET /api/upload/log/file/:name
	router.Get("/api/upload/log/file/:name", fileLogUploadGetHandler, logRequest)
	router.Get("/api/upload/log/file/", http.NotFound)

	// GET /api/upload/file-tree/:folder
	router.Get("/api/upload/file-tree/:folder", fileTreeUploadGetHandler, logRequest)
	router.Get("/api/upload/file-tree/", http.NotFound)

	// POST /api/upload/model/:model/workset
	// POST /api/upload/model/:model/workset/:set
	router.Post("/api/upload/model/:model/workset", worksetUploadPostHandler, logRequest)
	router.Post("/api/upload/model/:model/workset/:set", worksetUploadPostHandler, logRequest)
	router.Post("/api/upload/model/:model/workset/", http.NotFound)

	// POST /api/upload/model/:model/run
	// POST /api/upload/model/:model/run/:run
	router.Post("/api/upload/model/:model/run", runUploadPostHandler, logRequest)
	router.Post("/api/upload/model/:model/run/:run", runUploadPostHandler, logRequest)
	router.Post("/api/upload/model/:model/run/", http.NotFound)
	router.Post("/api/upload/model/", http.NotFound)

	// DELETE /api/upload/delete/:folder
	router.Delete("/api/upload/delete/:folder", uploadDeleteHandler, logRequest)
	router.Delete("/api/upload/delete/", http.NotFound)

	// DELETE /api/upload/start/delete/:folder
	router.Delete("/api/upload/start/delete/:folder", uploadAsyncDeleteHandler, logRequest)
	router.Delete("/api/upload/start/delete/", http.NotFound)
}

// add http web-service /api routes to download and manage archive files
func apiArchiveRoutes(router *vestigo.Router) {
}

// add http web-service /api routes to upload, download and manage files at home/io/files folder
func apiFilesRoutes(router *vestigo.Router) {

	// GET /api/files/list/:folder
	// router.Get("/api/files/list/:path", fileListGetHandler, logRequest)

	// GET /api/files/file/:path
	// router.Get("/api/files/file/:path", fileDownloadGetHandler, logRequest)

	// POST /api/files/file/:path
	// router.Post("/api/files/file/:path", fileUploadPostHandler, logRequest)

	// DELETE /api/files/file/:path
	// router.Delete("/api/files/file/:path", fileDeleteHandler, logRequest)

	// POST /api/files/folder/:path
	// router.Post("/api/files/folder/:path", folderCreatePostHandler, logRequest)

	// DELETE /api/files/folder/:path
	// router.Delete("/api/files/folder/:path", folderDeleteHandler, logRequest)
}

// add web-service /api routes for user-specific request
func apiUserRoutes(router *vestigo.Router) {

	// GET /api/user/view/model/:model
	router.Get("/api/user/view/model/:model", userViewGetHandler, logRequest)
	router.Get("/api/user/view/model/", http.NotFound)

	// PUT  /api/user/view/model/:model
	router.Put("/api/user/view/model/:model", userViewPutHandler, logRequest)
	router.Put("/api/user/view/model/", http.NotFound)

	// DELETE /api/user/view/model/:model
	router.Delete("/api/user/view/model/:model", userViewDeleteHandler, logRequest)
	router.Delete("/api/user/view/model/", http.NotFound)
}

// add web-service /api routes service state
func apiServiceRoutes(router *vestigo.Router) {

	// GET /api/service/config
	router.Get("/api/service/config", serviceConfigHandler, logRequest)

	// GET /api/service/state
	router.Get("/api/service/state", serviceStateHandler, logRequest)

	// GET /api/service/job/active/:job
	// GET /api/service/job/queue/:job
	// GET /api/service/job/history/:job
	router.Get("/api/service/job/active/:job", jobActiveHandler, logRequest)
	router.Get("/api/service/job/queue/:job", jobQueueHandler, logRequest)
	router.Get("/api/service/job/history/:job", jobHistoryHandler, logRequest)
	router.Get("/api/service/job/active/", http.NotFound)
	router.Get("/api/service/job/queue/", http.NotFound)
	router.Get("/api/service/job/history/", http.NotFound)

	// PUT /api/service/job/move/:pos/:job
	router.Put("/api/service/job/move/:pos/:job", jobMoveHandler, logRequest)
	router.Put("/api/service/job/move/:pos/", http.NotFound)
	router.Put("/api/service/job/move/", http.NotFound)

	// DELETE /api/service/job/delete/history/:job
	router.Delete("/api/service/job/delete/history/:job", jobHistoryDeleteHandler, logRequest)
	router.Delete("/api/service/job/delete/history/", http.NotFound)

	// GET /api/archive/state
	router.Get("/api/archive/state", archiveStateHandler, logRequest)
}

// add web-service /api routes for administrative tasks
func apiAdminRoutes(router *vestigo.Router) {

	// POST /api/admin/all-models/refresh
	router.Post("/api/admin/all-models/refresh", allModelsRefreshHandler, logRequest)

	// POST /api/admin/all-models/close
	router.Post("/api/admin/all-models/close", allModelsCloseHandler, logRequest)

	// POST /api/admin/jobs-pause/:pause
	router.Post("/api/admin/jobs-pause/:pause", jobsPauseHandler, logRequest)
	router.Post("/api/admin/jobs-pause/", http.NotFound)

	if theCfg.isAdminAll {

		// POST /api/admin-all/jobs-pause/:pause
		router.Post("/api/admin-all/jobs-pause/:pause", jobsAllPauseHandler, logRequest)
		router.Post("/api/admin-all/jobs-pause/", http.NotFound)
	}

	// DO NOT USE in production, development only
	//
	// POST /api/admin/run-test/:exe/:arg
	// router.Post("/api/admin/run-test/:exe/:arg", runTestHandler, logRequest)
}
