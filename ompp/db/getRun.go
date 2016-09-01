// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"strconv"
)

// GetRun return model run row by id: run_lst table row.
func GetRun(dbConn *sql.DB, runId int) (*RunRow, error) {
	return getRunRow(dbConn,
		"SELECT"+
			" H.run_id, H.model_id, H.run_name, H.sub_count,"+
			" H.sub_started, H.sub_completed, H.create_dt, H.status,"+
			" H.update_dt"+
			" FROM run_lst H"+
			" WHERE H.run_id ="+strconv.Itoa(runId))
}

// GetFirstRun return first run of the model: run_lst table row.
func GetFirstRun(dbConn *sql.DB, modelId int) (*RunRow, error) {

	return getRunRow(dbConn,
		"SELECT"+
			" H.run_id, H.model_id, H.run_name, H.sub_count,"+
			" H.sub_started, H.sub_completed, H.create_dt, H.status,"+
			" H.update_dt"+
			" FROM run_lst H"+
			" WHERE H.run_id ="+
			" (SELECT MIN(M.run_id) FROM run_lst M WHERE M.model_id = "+strconv.Itoa(modelId)+")")
}

// GetLastRun return last run of the model: run_lst table row.
func GetLastRun(dbConn *sql.DB, modelId int) (*RunRow, error) {

	return getRunRow(dbConn,
		"SELECT"+
			" H.run_id, H.model_id, H.run_name, H.sub_count,"+
			" H.sub_started, H.sub_completed, H.create_dt, H.status,"+
			" H.update_dt"+
			" FROM run_lst H"+
			" WHERE H.run_id ="+
			" (SELECT MAX(M.run_id) FROM run_lst M WHERE M.model_id = "+strconv.Itoa(modelId)+")")
}

// GetLastCompletedRun return last completed run of the model: run_lst table row.
// Run completed if run status one of: s=success, x=exit, e=error
func GetLastCompletedRun(dbConn *sql.DB, modelId int) (*RunRow, error) {

	return getRunRow(dbConn,
		"SELECT"+
			" H.run_id, H.model_id, H.run_name, H.sub_count,"+
			" H.sub_started, H.sub_completed, H.create_dt, H.status,"+
			" H.update_dt"+
			" FROM run_lst H"+
			" WHERE H.run_id ="+
			" ("+
			" SELECT MAX(M.run_id) FROM run_lst M"+
			" WHERE M.model_id = "+strconv.Itoa(modelId)+" AND M.status IN ('s', 'x', 'e')"+
			" )")
}

// getRunRow return run_lst table row.
func getRunRow(dbConn *sql.DB, query string) (*RunRow, error) {

	var runRow RunRow

	err := SelectFirst(dbConn, query,
		func(row *sql.Row) error {
			if err := row.Scan(
				&runRow.RunId, &runRow.ModelId, &runRow.Name, &runRow.SubCount,
				&runRow.SubStarted, &runRow.SubCompleted, &runRow.CreateDateTime, &runRow.Status,
				&runRow.UpdateDateTime); err != nil {
				return err
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return &runRow, nil
}

// getRunLst return run_lst table rows.
func getRunLst(dbConn *sql.DB, query string) ([]RunRow, error) {

	var runRs []RunRow

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r RunRow
			if err := rows.Scan(
				&r.RunId, &r.ModelId, &r.Name, &r.SubCount,
				&r.SubStarted, &r.SubCompleted, &r.CreateDateTime, &r.Status,
				&r.UpdateDateTime); err != nil {
				return err
			}
			runRs = append(runRs, r)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return runRs, nil
}

// GetRunByModelId return list of model runs with description and notes: run_lst and run_txt rows.
// If langCode not empty then only specified language selected else all languages
func GetRunByModelId(dbConn *sql.DB, modelId int, langCode string) ([]RunRow, []RunTxtRow, error) {

	// model not found: model id must be positive
	if modelId <= 0 {
		return nil, nil, nil
	}

	// get list of runs for that model id
	q := "SELECT" +
		" H.run_id, H.model_id, H.run_name, H.sub_count," +
		" H.sub_started, H.sub_completed, H.create_dt, H.status," +
		" H.update_dt" +
		" FROM run_lst H" +
		" WHERE H.model_id = " + strconv.Itoa(modelId) +
		" ORDER BY 1"

	runRs, err := getRunLst(dbConn, q)
	if err != nil {
		return nil, nil, err
	}

	// get run description and notes by model id and language
	q = "SELECT M.run_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM run_txt M" +
		" INNER JOIN run_lst H ON (H.run_id = M.run_id)" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE H.model_id = " + strconv.Itoa(modelId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2"

	txtRs, err := getRunText(dbConn, q)
	if err != nil {
		return nil, nil, err
	}
	return runRs, txtRs, nil
}

// GetRunText return model run description and notes: run_txt table rows.
// If langCode not empty then only specified language selected else all languages
func GetRunText(dbConn *sql.DB, runId int, langCode string) ([]RunTxtRow, error) {

	q := "SELECT M.run_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM run_txt M" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE M.run_id = " + strconv.Itoa(runId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2"

	return getRunText(dbConn, q)
}

// getRunText return model run description and notes: run_txt table rows.
func getRunText(dbConn *sql.DB, query string) ([]RunTxtRow, error) {

	// select db rows from workset_parameter_txt
	var txtLst []RunTxtRow

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r RunTxtRow
			var note sql.NullString
			if err := rows.Scan(
				&r.RunId, &r.LangId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			txtLst = append(txtLst, r)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return txtLst, nil
}

// GetRunParamText return run parameter value notes: run_parameter_txt table rows.
// If langCode not empty then only specified language selected else all languages
func GetRunParamText(dbConn *sql.DB, modelDef *ModelMeta, runId int, paramId int, langCode string) ([]RunParamTxtRow, error) {

	// find parameter Hid
	hId := modelDef.ParamHidById(paramId)
	if hId <= 0 {
		return []RunParamTxtRow{}, nil // parameter not found, return empty results
	}

	// make select using Hid
	q := "SELECT M.run_id, M.parameter_hid, M.lang_id, L.lang_code, M.note" +
		" FROM run_parameter_txt M" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE M.run_id = " + strconv.Itoa(runId) +
		" AND M.parameter_hid = " + strconv.Itoa(hId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2, 3"

	// do select and set parameter id in output results
	return getRunParamText(dbConn, modelDef, q)
}

// GetRunParamTextByRunId return all run parameters value notes: run_parameter_txt table rows.
// If langCode not empty then only specified language selected else all languages
func GetRunParamTextByRunId(dbConn *sql.DB, modelDef *ModelMeta, runId int, langCode string) ([]RunParamTxtRow, error) {

	// make select using Hid
	q := "SELECT M.run_id, M.parameter_hid, M.lang_id, L.lang_code, M.note" +
		" FROM run_parameter_txt M" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE M.run_id = " + strconv.Itoa(runId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2, 3"

	// do select and set parameter id in output results
	return getRunParamText(dbConn, modelDef, q)
}

// getRunParamText return run parameter value notes: run_parameter_txt table rows.
func getRunParamText(dbConn *sql.DB, modelDef *ModelMeta, query string) ([]RunParamTxtRow, error) {

	var txtLst []RunParamTxtRow
	hId := 0

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r RunParamTxtRow
			var note sql.NullString
			if err := rows.Scan(
				&r.RunId, &hId, &r.LangId, &r.LangCode, &note); err != nil {
				return err
			}

			if note.Valid {
				r.Note = note.String
			}
			r.ParamId = modelDef.ParamIdByHid(hId) // set parameter id in output results

			txtLst = append(txtLst, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return txtLst, nil
}

// GetRunList return list of completed model runs: run_lst, run_txt, run_option run_parameter_txt rows.
// If isSuccess true then return only successfully completed runs else all completed runs.
// It does not return non-completed runs (run in progress).
// If langCode not empty then only specified language selected else all languages
func GetRunList(dbConn *sql.DB, modelDef *ModelMeta, isSuccess bool, langCode string) (*RunList, error) {

	// where filters
	var statusFilter string
	if isSuccess {
		statusFilter = " AND H.status = " + toQuoted(DoneRunStatus)
	} else {
		statusFilter = " AND H.status IN (" +
			toQuoted(DoneRunStatus) + ", " + toQuoted(ErrorRunStatus) + ", " + toQuoted(ExitRunStatus) + ", " + ")"
	}

	var langFilter string
	if langCode != "" {
		langFilter = " AND L.lang_code = " + toQuoted(langCode)
	}

	// get list of runs for that model id
	q := "SELECT" +
		" H.run_id, H.model_id, H.run_name, H.sub_count," +
		" H.sub_started, H.sub_completed, H.create_dt, H.status," +
		" H.update_dt" +
		" FROM run_lst H" +
		" WHERE H.model_id = " + strconv.Itoa(modelDef.Model.ModelId) +
		statusFilter +
		" ORDER BY 1"

	runRs, err := getRunLst(dbConn, q)
	if err != nil {
		return nil, err
	}

	// get run description and notes by model id and language
	q = "SELECT M.run_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM run_txt M" +
		" INNER JOIN run_lst H ON (H.run_id = M.run_id)" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE H.model_id = " + strconv.Itoa(modelDef.Model.ModelId) +
		statusFilter +
		langFilter +
		" ORDER BY 1, 2"

	runTxtRs, err := getRunText(dbConn, q)
	if err != nil {
		return nil, err
	}

	// get run options by model id
	q = "SELECT" +
		" M.run_id, M.option_key, M.option_value" +
		" FROM run_option M" +
		" INNER JOIN run_lst H ON (H.run_id = M.run_id)" +
		" WHERE H.model_id = " + strconv.Itoa(modelDef.Model.ModelId) +
		statusFilter +
		" ORDER BY 1, 2"

	optRs, err := getRunOpts(dbConn, q)
	if err != nil {
		return nil, err
	}

	// run_parameter_txt: select using Hid
	q = "SELECT M.run_id, M.parameter_hid, M.lang_id, L.lang_code, M.note" +
		" FROM run_parameter_txt M" +
		" INNER JOIN run_lst H ON (H.run_id = M.run_id)" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE H.model_id = " + strconv.Itoa(modelDef.Model.ModelId) +
		statusFilter +
		langFilter +
		" ORDER BY 1, 2, 3"

	// do select and set parameter id in output results
	paramTxtRs, err := getRunParamText(dbConn, modelDef, q)
	if err != nil {
		return nil, err
	}

	// convert to output result: join run pieces in struct by run id
	rl := RunList{
		ModelName:   modelDef.Model.Name,
		ModelDigest: modelDef.Model.Digest,
		Lst:         make([]RunMeta, len(runRs))}
	m := make(map[int]int)

	for k := range runRs {
		runId := runRs[k].RunId
		rl.Lst[k].Run = runRs[k]
		rl.Lst[k].Opts = optRs[runId]
		m[runId] = k
	}
	for k := range runTxtRs {
		i := m[runTxtRs[k].RunId]
		rl.Lst[i].Txt = append(rl.Lst[i].Txt, runTxtRs[k])
	}
	for k := range paramTxtRs {
		i := m[paramTxtRs[k].RunId]
		rl.Lst[i].ParamTxt = append(rl.Lst[i].ParamTxt, paramTxtRs[k])
	}

	return &rl, nil
}
