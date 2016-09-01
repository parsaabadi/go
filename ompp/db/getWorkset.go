// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"strconv"
)

// GetWorkset return working set by id: workset_lst table row.
func GetWorkset(dbConn *sql.DB, setId int) (*WorksetRow, error) {
	return getWsRow(dbConn,
		"SELECT"+
			" W.set_id, W.base_run_id, W.model_id, W.set_name, W.is_readonly, W.update_dt"+
			" FROM workset_lst W"+
			" WHERE W.set_id = "+strconv.Itoa(setId))
}

// GetDefaultWorkset return default working set for the model.
// Default workset is a first workset for the model, each model must have default workset.
func GetDefaultWorkset(dbConn *sql.DB, modelId int) (*WorksetRow, error) {

	return getWsRow(dbConn,
		"SELECT"+
			" W.set_id, W.base_run_id, W.model_id, W.set_name, W.is_readonly, W.update_dt"+
			" FROM workset_lst W"+
			" WHERE W.set_id ="+
			" (SELECT MIN(M.set_id) FROM workset_lst M WHERE M.model_id = "+strconv.Itoa(modelId)+")")
}

// GetWorksetByName return working set by name.
// If model has multiple worksets with that name then return first set.
func GetWorksetByName(dbConn *sql.DB, modelId int, name string) (*WorksetRow, error) {

	return getWsRow(dbConn,
		"SELECT"+
			" W.set_id, W.base_run_id, W.model_id, W.set_name, W.is_readonly, W.update_dt"+
			" FROM workset_lst W"+
			" WHERE W.set_id ="+
			" ("+
			" SELECT MIN(M.set_id) FROM workset_lst M"+
			" WHERE M.model_id = "+strconv.Itoa(modelId)+
			" AND M.set_name = "+toQuoted(name)+
			" )")
}

// GetWorksetByModelId return list of model worksets, description and notes: workset_lst and workset_txt rows.
// If langCode not empty then only specified language selected else all languages
func GetWorksetByModelId(dbConn *sql.DB, modelId int, langCode string) ([]WorksetRow, []WorksetTxtRow, error) {

	// model not found: model id must be positive
	if modelId <= 0 {
		return nil, nil, nil
	}

	// select worksets by model id
	q := "SELECT" +
		" W.set_id, W.base_run_id, W.model_id, W.set_name, W.is_readonly, W.update_dt" +
		" FROM workset_lst W" +
		" WHERE W.model_id = " + strconv.Itoa(modelId) +
		" ORDER BY 1"

	setRs, err := getWsLst(dbConn, q)
	if err != nil {
		return nil, nil, err
	}

	// select worksets description and notes by model id and language
	q = "SELECT M.set_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM workset_txt M" +
		" INNER JOIN workset_lst H ON (H.set_id = M.set_id)" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE H.model_id = " + strconv.Itoa(modelId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2"

	txtRs, err := getWsText(dbConn, q)
	if err != nil {
		return nil, nil, err
	}
	return setRs, txtRs, nil
}

// getWsRow return workset_lst table row.
func getWsRow(dbConn *sql.DB, query string) (*WorksetRow, error) {

	var setRow WorksetRow

	err := SelectFirst(dbConn, query,
		func(row *sql.Row) error {
			var baseId sql.NullInt64
			if err := row.Scan(
				&setRow.SetId, &baseId, &setRow.ModelId, &setRow.Name, &setRow.IsReadonly, &setRow.UpdateDateTime); err != nil {
				return err
			}
			if baseId.Valid {
				setRow.BaseRunId = int(baseId.Int64)
			}
			return nil
		})
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return &setRow, nil
}

// getWsLst return workset_lst table rows.
func getWsLst(dbConn *sql.DB, query string) ([]WorksetRow, error) {

	// get list of workset for that model id
	var setRs []WorksetRow

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r WorksetRow
			var rId sql.NullInt64
			if err := rows.Scan(
				&r.SetId, &rId, &r.ModelId, &r.Name, &r.IsReadonly, &r.UpdateDateTime); err != nil {
				return err
			}
			if rId.Valid {
				r.BaseRunId = int(rId.Int64)
			}
			setRs = append(setRs, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return setRs, nil
}

// GetWorksetText return workset description and notes: workset_txt table rows.
// If langCode not empty then only specified language selected else all languages
func GetWorksetText(dbConn *sql.DB, setId int, langCode string) ([]WorksetTxtRow, error) {

	q := "SELECT M.set_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM workset_txt M" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE M.set_id = " + strconv.Itoa(setId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2"

	return getWsText(dbConn, q)
}

// getWsText return workset description and notes: workset_txt table rows.
func getWsText(dbConn *sql.DB, query string) ([]WorksetTxtRow, error) {

	var txtLst []WorksetTxtRow

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r WorksetTxtRow
			var note sql.NullString
			if err := rows.Scan(
				&r.SetId, &r.LangId, &r.LangCode, &r.Descr, &note); err != nil {
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

// GetWorksetRunIds return ids of model run results (run_id) where input parameters are from specified working set.
// Only successfully completed run ids returned, not failed, not "in progress".
// This method is "local" to database and if data transfered between databases it very likely return wrong results.
// This method is not recommended, use modeling task to establish relationship between input set and model run.
func GetWorksetRunIds(dbConn *sql.DB, setId int) ([]int, error) {

	var idRs []int

	err := SelectRows(dbConn,
		"SELECT RL.run_id"+
			" FROM run_lst RL"+
			" INNER JOIN run_option RO ON (RO.run_id = RL.run_id)"+
			" WHERE RL.status = 's'"+
			" AND RO.option_key = 'OpenM.SetId'"+
			" AND RO.option_value = '"+strconv.Itoa(setId)+"'"+
			" ORDER BY 1",
		func(rows *sql.Rows) error {
			var rId int
			if err := rows.Scan(&rId); err != nil {
				return err
			}
			idRs = append(idRs, rId)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return idRs, nil
}

// GetWorksetParamIds return id of parameters (parameter_id) included in that workset
func GetWorksetParamIds(dbConn *sql.DB, modelDef *ModelMeta, setId int) ([]int, error) {

	var idRs []int

	err := SelectRows(dbConn,
		"SELECT parameter_hid FROM workset_parameter WHERE set_id = "+strconv.Itoa(setId)+" ORDER BY 1",
		func(rows *sql.Rows) error {
			var hId int
			if err := rows.Scan(&hId); err != nil {
				return err
			}
			idRs = append(idRs, modelDef.ParamIdByHid(hId))
			return nil
		})
	if err != nil {
		return nil, err
	}
	return idRs, nil
}

// GetWorksetParamText return parameter value notes: workset_parameter_txt table rows.
// If langCode not empty then only specified language selected else all languages
func GetWorksetParamText(dbConn *sql.DB, modelDef *ModelMeta, setId int, paramId int, langCode string) ([]WorksetParamTxtRow, error) {

	// find parameter Hid
	hId := modelDef.ParamHidById(paramId)
	if hId <= 0 {
		return []WorksetParamTxtRow{}, nil // parameter not found, return empty results
	}

	// make select using Hid
	q := "SELECT M.set_id, M.parameter_hid, M.lang_id, L.lang_code, M.note" +
		" FROM workset_parameter_txt M" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE M.set_id = " + strconv.Itoa(setId) +
		" AND M.parameter_hid = " + strconv.Itoa(hId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2, 3"

	// do select and set parameter id in output results
	return getWsParamText(dbConn, modelDef, q)
}

// GetWorksetParamTextBySetId return all workset parameters value notes: workset_parameter_txt table rows.
// If langCode not empty then only specified language selected else all languages
func GetWorksetParamTextBySetId(dbConn *sql.DB, modelDef *ModelMeta, setId int, langCode string) ([]WorksetParamTxtRow, error) {

	// make select using Hid
	q := "SELECT M.set_id, M.parameter_hid, M.lang_id, L.lang_code, M.note" +
		" FROM workset_parameter_txt M" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE M.set_id = " + strconv.Itoa(setId)
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2, 3"

	// do select and set parameter id in output results
	return getWsParamText(dbConn, modelDef, q)
}

// getWsParamText return parameter value notes: workset_parameter_txt table rows.
func getWsParamText(dbConn *sql.DB, modelDef *ModelMeta, query string) ([]WorksetParamTxtRow, error) {

	var txtLst []WorksetParamTxtRow
	hId := 0

	err := SelectRows(dbConn, query,
		func(rows *sql.Rows) error {
			var r WorksetParamTxtRow
			var note sql.NullString
			if err := rows.Scan(
				&r.SetId, &hId, &r.LangId, &r.LangCode, &note); err != nil {
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

// GetWorksetList return list of model worksets: workset_lst, workset_txt, workset_parameter, workset_parameter_txt table rows.
// If isReadonly true then return only readonly worksets else all worsets.
// If langCode not empty then only specified language selected else all languages
func GetWorksetList(dbConn *sql.DB, modelDef *ModelMeta, isReadonly bool, langCode string) (*WorksetList, error) {

	// where filters
	var roFilter string
	if isReadonly {
		roFilter = " AND H.is_readonly <> 0"
	}

	var langFilter string
	if langCode != "" {
		langFilter = " AND L.lang_code = " + toQuoted(langCode)
	}

	// workset_lst rows
	q := "SELECT" +
		" H.set_id, H.base_run_id, H.model_id, H.set_name, H.is_readonly, H.update_dt" +
		" FROM workset_lst H" +
		" WHERE H.model_id = " + strconv.Itoa(modelDef.Model.ModelId) +
		roFilter +
		" ORDER BY 1"

	setRs, err := getWsLst(dbConn, q)
	if err != nil {
		return nil, err
	}

	// workset_txt rows
	q = "SELECT M.set_id, M.lang_id, L.lang_code, M.descr, M.note" +
		" FROM workset_txt M" +
		" INNER JOIN workset_lst H ON (H.set_id = M.set_id)" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE H.model_id = " + strconv.Itoa(modelDef.Model.ModelId) +
		roFilter +
		langFilter +
		" ORDER BY 1, 2"

	setTxtRs, err := getWsText(dbConn, q)
	if err != nil {
		return nil, err
	}

	// workset_parameter: select using Hid, translate to parameter id in result
	q = "SELECT H.set_id, M.parameter_hid" +
		" FROM workset_parameter M" +
		" INNER JOIN workset_lst H ON (H.set_id = M.set_id)" +
		" WHERE H.model_id = " + strconv.Itoa(modelDef.Model.ModelId) +
		roFilter +
		" ORDER BY 1, 2"

	var ps [][2]int

	err = SelectRows(dbConn, q,
		func(rows *sql.Rows) error {
			var setId, hId int
			if err := rows.Scan(&setId, &hId); err != nil {
				return err
			}
			ps = append(ps, [2]int{setId, hId})
			return nil
		})
	if err != nil {
		return nil, err
	}

	// workset_parameter_txt: select using Hid
	q = "SELECT M.set_id, M.parameter_hid, M.lang_id, L.lang_code, M.note" +
		" FROM workset_parameter_txt M" +
		" INNER JOIN workset_lst H ON (H.set_id = M.set_id)" +
		" INNER JOIN lang_lst L ON (L.lang_id = M.lang_id)" +
		" WHERE H.model_id = " + strconv.Itoa(modelDef.Model.ModelId) +
		roFilter +
		langFilter +
		" ORDER BY 1, 2, 3"

	// do select and set parameter id in output results
	paramTxtRs, err := getWsParamText(dbConn, modelDef, q)
	if err != nil {
		return nil, err
	}

	// convert to output result: join workset pieces in struct by set id
	wl := WorksetList{
		ModelName:   modelDef.Model.Name,
		ModelDigest: modelDef.Model.Digest,
		Lst:         make([]WorksetMeta, len(setRs))}
	m := make(map[int]int)

	for k := range setRs {
		setId := setRs[k].SetId
		wl.Lst[k].Set = setRs[k]
		m[setId] = k
	}
	for k := range ps {
		i := m[ps[k][0]]
		if j, ok := modelDef.ParamByHid(ps[k][1]); ok {
			wl.Lst[i].Param = append(wl.Lst[i].Param, modelDef.Param[j].ParamDicRow)
		}
	}
	for k := range setTxtRs {
		i := m[setTxtRs[k].SetId]
		wl.Lst[i].Txt = append(wl.Lst[i].Txt, setTxtRs[k])
	}
	for k := range paramTxtRs {
		i := m[paramTxtRs[k].SetId]
		wl.Lst[i].ParamTxt = append(wl.Lst[i].ParamTxt, paramTxtRs[k])
	}

	return &wl, nil
}
