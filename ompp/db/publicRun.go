// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// ToPublic convert model run db rows into "public" model run format for json import-export.
func (meta *RunMeta) ToPublic(dbConn *sql.DB, modelDef *ModelMeta) (*RunPub, error) {

	// validate run model id: run must belong to the model
	if meta.Run.ModelId != modelDef.Model.ModelId {
		return nil, errors.New("model run: " + strconv.Itoa(meta.Run.RunId) + " " + meta.Run.Name + ", invalid model id " + strconv.Itoa(meta.Run.ModelId) + " expected: " + strconv.Itoa(modelDef.Model.ModelId))
	}

	// run header
	pub := RunPub{
		ModelName:      modelDef.Model.Name,
		ModelDigest:    modelDef.Model.Digest,
		Name:           meta.Run.Name,
		SubCount:       meta.Run.SubCount,
		SubStarted:     meta.Run.SubStarted,
		SubCompleted:   meta.Run.SubCompleted,
		CreateDateTime: meta.Run.CreateDateTime,
		Status:         meta.Run.Status,
		UpdateDateTime: meta.Run.UpdateDateTime,
		Digest:         meta.Run.Digest,
		Opts:           make(map[string]string, len(meta.Opts)),
		Txt:            make([]DescrNote, len(meta.Txt)),
		Param:          make([]ParamRunSetPub, len(meta.Param)),
		Progress:       make([]RunProgress, len(meta.Progress)),
	}

	// copy run_option rows
	for k, v := range meta.Opts {
		pub.Opts[k] = v
	}

	// run description and notes by language
	for k := range meta.Txt {
		pub.Txt[k] = DescrNote{
			LangCode: meta.Txt[k].LangCode,
			Descr:    meta.Txt[k].Descr,
			Note:     meta.Txt[k].Note}
	}

	// run parameters value notes
	for k := range meta.Param {

		// find model parameter index by name
		idx, ok := modelDef.ParamByHid(meta.Param[k].ParamHid)
		if !ok {
			return nil, errors.New("model run: " + strconv.Itoa(meta.Run.RunId) + " " + meta.Run.Name + ", parameter " + strconv.Itoa(meta.Param[k].ParamHid) + " not found")
		}

		pub.Param[k] = ParamRunSetPub{
			Name:     modelDef.Param[idx].Name,
			SubCount: meta.Param[k].SubCount,
			Txt:      make([]LangNote, len(meta.Param[k].Txt)),
		}
		for j := range meta.Param[k].Txt {
			pub.Param[k].Txt[j] = LangNote{
				LangCode: meta.Param[k].Txt[j].LangCode,
				Note:     meta.Param[k].Txt[j].Note,
			}
		}
	}

	// copy run_progress rows
	copy(pub.Progress, meta.Progress)

	return &pub, nil
}

// FromPublic convert model run metadata from "public" format (coming from json import-export) into db rows.
func (pub *RunPub) FromPublic(dbConn *sql.DB, modelDef *ModelMeta) (*RunMeta, error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata")
	}
	if pub.ModelName == "" && pub.ModelDigest == "" {
		return nil, errors.New("invalid (empty) model name and digest, model run: " + pub.Name + " " + pub.CreateDateTime)
	}

	// validate run model name and/or digest: run must belong to the model
	if (pub.ModelName != "" && pub.ModelName != modelDef.Model.Name) ||
		(pub.ModelDigest != "" && pub.ModelDigest != modelDef.Model.Digest) {
		return nil, errors.New("invalid model name " + pub.ModelName + " or digest " + pub.ModelDigest + " expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// run header: run_lst row with zero default run id
	meta := RunMeta{
		Run: RunRow{
			RunId:          0, // run id is undefined
			ModelId:        modelDef.Model.ModelId,
			Name:           pub.Name,
			SubCount:       pub.SubCount,
			SubStarted:     pub.SubStarted,
			SubCompleted:   pub.SubCompleted,
			CreateDateTime: pub.CreateDateTime,
			Status:         pub.Status,
			UpdateDateTime: pub.UpdateDateTime,
			Digest:         pub.Digest,
		},
		Txt:      make([]RunTxtRow, len(pub.Txt)),
		Opts:     make(map[string]string, len(pub.Opts)),
		Param:    make([]runParam, len(pub.Param)),
		Progress: make([]RunProgress, len(pub.Progress)),
	}

	// model run description and notes: run_txt rows
	// use run id default zero
	for k := range pub.Txt {
		meta.Txt[k].LangCode = pub.Txt[k].LangCode
		meta.Txt[k].Descr = pub.Txt[k].Descr
		meta.Txt[k].Note = pub.Txt[k].Note
	}

	// run options
	for k, v := range pub.Opts {
		meta.Opts[k] = v
	}

	// run parameters value notes: run_parameter_txt rows
	// use set id default zero
	for k := range pub.Param {

		// find model parameter index by name
		idx, ok := modelDef.ParamByName(pub.Param[k].Name)
		if !ok {
			return nil, errors.New("model run: " + pub.Name + " parameter " + pub.Param[k].Name + " not found")
		}
		meta.Param[k].ParamHid = modelDef.Param[idx].ParamHid
		meta.Param[k].SubCount = pub.Param[k].SubCount

		// workset parameter value notes, use set id default zero
		if len(pub.Param[k].Txt) > 0 {
			meta.Param[k].Txt = make([]RunParamTxtRow, len(pub.Param[k].Txt))

			for j := range pub.Param[k].Txt {
				meta.Param[k].Txt[j].ParamHid = meta.Param[k].ParamHid
				meta.Param[k].Txt[j].LangCode = pub.Param[k].Txt[j].LangCode
				meta.Param[k].Txt[j].Note = pub.Param[k].Txt[j].Note
			}
		}
	}

	// copy run_progress rows
	copy(meta.Progress, pub.Progress)

	return &meta, nil
}
