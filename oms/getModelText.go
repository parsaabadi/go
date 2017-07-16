// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"golang.org/x/text/language"

	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
)

// ModelTextByDigest return model_dic_txt db row by model digest and prefered language tags.
// It can be in prefered language, default model language or empty if no model model_dic_txt rows exist.
func (mc *ModelCatalog) ModelTextByDigest(digest string, preferedLang []language.Tag) (*ModelDicDescrNote, bool) {

	// if model_dic_txt rows not loaded then read it from database
	idx := mc.loadModelText(digest)
	if idx < 0 {
		return &ModelDicDescrNote{}, false // return empty result: model not found or error
	}

	// lock model catalog
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// match prefered languages and model languages
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langLst[np].LangCode
	lcd := mc.modelLst[idx].meta.Model.DefaultLangCode

	mt := ModelDicDescrNote{Model: mc.modelLst[idx].meta.Model}

	// if model_dic_txt rows not empty then find row by matched language or by default language
	if len(mc.modelLst[idx].txtMeta.ModelTxt) > 0 {

		var nd, i int
		for ; i < len(mc.modelLst[idx].txtMeta.ModelTxt); i++ {
			if mc.modelLst[idx].txtMeta.ModelTxt[i].LangCode == lc {
				break // language match
			}
			if mc.modelLst[idx].txtMeta.ModelTxt[i].LangCode == lcd {
				nd = i // index of default language
			}
		}
		if i >= len(mc.modelLst[idx].txtMeta.ModelTxt) {
			i = nd // use default language or zero index row
		}

		mt.DescrNote = db.DescrNote{
			LangCode: mc.modelLst[idx].txtMeta.ModelTxt[i].LangCode,
			Descr:    mc.modelLst[idx].txtMeta.ModelTxt[i].Descr,
			Note:     mc.modelLst[idx].txtMeta.ModelTxt[i].Note}
	}
	return &mt, true
}

// loadModelText partially init model text metadata by reading model_dic_txt db rows.
// If metadata already loaded then skip db reading and return index in model list.
// Return index in model list or < 0 on error or if model digest not found.
func (mc *ModelCatalog) loadModelText(digest string) int {

	// if model digest is empty then return empty results
	if digest == "" {
		omppLog.Log("Warning: invalid (empty) model digest")
		return -1
	}

	// find model index by digest
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigest(digest)
	if idx < 0 {
		omppLog.Log("Warning: model digest not found: ", digest)
		return idx // model not found, index is negative
	}
	if mc.modelLst[idx].txtMeta != nil { // exit if model text already loaded
		return idx
	}

	// read model_dic_txt rows from database
	txt, err := db.GetModelTextById(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, "")
	if err != nil {
		omppLog.Log("Error at get model_dic_txt: ", digest, ": ", err.Error())
		return -1
	}

	// partial initialization of model text metadata: only model_dic_txt rows
	mc.modelLst[idx].isTxtMetaFull = false
	mc.modelLst[idx].txtMeta =
		&db.ModelTxtMeta{
			ModelName:   mc.modelLst[idx].meta.Model.Name,
			ModelDigest: mc.modelLst[idx].meta.Model.Digest,
			ModelTxt:    txt}

	return idx
}

// ModelMetaTextByDigestOrName return language-specific model metadata
// by model digest or name and prefered language tags.
// It can be in default model language or empty if no model text db rows exist.
func (mc *ModelCatalog) ModelMetaTextByDigestOrName(dn string, preferedLang []language.Tag) (*ModelMetaDescrNote, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &ModelMetaDescrNote{}, false
	}

	// before text metadata we must load language-neutral model metadata
	idx := mc.loadModelMeta(dn)
	if idx < 0 {
		return &ModelMetaDescrNote{}, false // return empty result: model not found or error
	}

	// if language-specific model metadata not loaded then read it from database
	idx = mc.loadModelMetaText(dn)
	if idx < 0 {
		return &ModelMetaDescrNote{}, false // return empty result: model not found or error
	}

	// lock model catalog
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// match prefered languages and model languages
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langLst[np].LangCode
	lcd := mc.modelLst[idx].meta.Model.DefaultLangCode

	// initialaze text metadata with copy of language-neutral metadata
	mt := ModelMetaDescrNote{
		ModelTxt: ModelDicDescrNote{Model: mc.modelLst[idx].meta.Model},
		TypeTxt:  make([]TypeDescrNote, len(mc.modelLst[idx].meta.Type)),
		ParamTxt: make([]ParamDescrNote, len(mc.modelLst[idx].meta.Param)),
		TableTxt: make([]TableDescrNote, len(mc.modelLst[idx].meta.Table))}

	// model types
	for k := range mt.TypeTxt {
		mt.TypeTxt[k].Type = mc.modelLst[idx].meta.Type[k].TypeDicRow
		mt.TypeTxt[k].TypeEnumTxt = make([]TypeEnumDescrNote, len(mc.modelLst[idx].meta.Type[k].Enum))

		for j := range mt.TypeTxt[k].TypeEnumTxt {
			mt.TypeTxt[k].TypeEnumTxt[j].Enum = mc.modelLst[idx].meta.Type[k].Enum[j]
		}
	}

	// model parameters
	for k := range mt.ParamTxt {
		mt.ParamTxt[k].Param = mc.modelLst[idx].meta.Param[k].ParamDicRow
		mt.ParamTxt[k].ParamDimsTxt = make([]ParamDimsDescrNote, len(mc.modelLst[idx].meta.Param[k].Dim))

		for j := range mt.ParamTxt[k].ParamDimsTxt {
			mt.ParamTxt[k].ParamDimsTxt[j].Dim = mc.modelLst[idx].meta.Param[k].Dim[j]
		}
	}

	// model output tables
	for k := range mt.TableTxt {
		mt.TableTxt[k].Table = mc.modelLst[idx].meta.Table[k].TableDicRow
		mt.TableTxt[k].TableDimsTxt = make([]TableDimsDescrNote, len(mc.modelLst[idx].meta.Table[k].Dim))
		mt.TableTxt[k].TableAccTxt = make([]TableAccDescrNote, len(mc.modelLst[idx].meta.Table[k].Acc))
		mt.TableTxt[k].TableExprTxt = make([]TableExprDescrNote, len(mc.modelLst[idx].meta.Table[k].Expr))

		for j := range mt.TableTxt[k].TableDimsTxt {
			mt.TableTxt[k].TableDimsTxt[j].Dim = mc.modelLst[idx].meta.Table[k].Dim[j]
		}
		for j := range mt.TableTxt[k].TableAccTxt {
			mt.TableTxt[k].TableAccTxt[j].Acc = mc.modelLst[idx].meta.Table[k].Acc[j]
		}
		for j := range mt.TableTxt[k].TableExprTxt {
			mt.TableTxt[k].TableExprTxt[j].Expr = mc.modelLst[idx].meta.Table[k].Expr[j]
		}
	}

	// set language-specific rows by matched language or by default language or by zero index language

	// set model description and notes
	if len(mc.modelLst[idx].txtMeta.ModelTxt) > 0 {

		var nf, i int
		for ; i < len(mc.modelLst[idx].txtMeta.ModelTxt); i++ {
			if mc.modelLst[idx].txtMeta.ModelTxt[i].LangCode == lc {
				break // language match
			}
			if mc.modelLst[idx].txtMeta.ModelTxt[i].LangCode == lcd {
				nf = i // index of default language
			}
		}
		if i >= len(mc.modelLst[idx].txtMeta.ModelTxt) {
			i = nf // use default language or zero index row
		}
		mt.ModelTxt.DescrNote = db.DescrNote{
			LangCode: mc.modelLst[idx].txtMeta.ModelTxt[i].LangCode,
			Descr:    mc.modelLst[idx].txtMeta.ModelTxt[i].Descr,
			Note:     mc.modelLst[idx].txtMeta.ModelTxt[i].Note}
	}

	// set model types description and notes
	if len(mt.TypeTxt) > 0 && len(mc.modelLst[idx].txtMeta.TypeTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, di int

		for ; si < len(mc.modelLst[idx].txtMeta.TypeTxt); si++ {

			// destination rows must be defined by [di] index
			if di >= len(mt.TypeTxt) {
				break // done with all destination text
			}

			// check if source and destination keys equal
			mId := mt.TypeTxt[di].Type.ModelId
			tId := mt.TypeTxt[di].Type.TypeId

			isKey = mc.modelLst[idx].txtMeta.TypeTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.TypeTxt[si].TypeId == tId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.TypeTxt[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TypeTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TypeTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TypeTxt[ni].Note}

				// reset to start next search
				isFound = false
				isMatch = false
				di++ // move to next type
				si-- // repeat current source row
				continue
			}

			// inside of key
			if isKey {

				if !isFound {
					isFound = true // first key found
					nf = si
				}
				// match the language
				isMatch = mc.modelLst[idx].txtMeta.TypeTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.TypeTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.TypeTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.TypeTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TypeTxt[si].TypeId > tId) {

				di++ // move to next type
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && di < len(mt.TypeTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.TypeTxt) {
				mt.TypeTxt[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TypeTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TypeTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TypeTxt[ni].Note}
			}
		}
	}

	// set model enums description and notes
	if len(mt.TypeTxt) > 0 && len(mc.modelLst[idx].txtMeta.TypeEnumTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, pi, ci int

		for ; si < len(mc.modelLst[idx].txtMeta.TypeEnumTxt); si++ {

			// destination rows: parent and child must be defined by valid (pi, ci) indexes
			if pi >= len(mt.TypeTxt) {
				break // done with all destination text
			}
			if pi < len(mt.TypeTxt) &&
				ci >= len(mt.TypeTxt[pi].TypeEnumTxt) {

				if pi++; pi >= len(mt.TypeTxt) {
					break // done with all destination text
				}

				ci = 0 // move to next type
				si--   // repeat current source row
				continue
			}

			// check if source and destination keys equal
			mId := mt.TypeTxt[pi].Type.ModelId
			tId := mt.TypeTxt[pi].Type.TypeId
			eId := mt.TypeTxt[pi].TypeEnumTxt[ci].Enum.EnumId

			isKey = mc.modelLst[idx].txtMeta.TypeEnumTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.TypeEnumTxt[si].TypeId == tId &&
				mc.modelLst[idx].txtMeta.TypeEnumTxt[si].EnumId == eId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.TypeTxt[pi].TypeEnumTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].Note}

				// reset to start next search
				isFound = false
				isMatch = false
				ci++ // move to next type
				si-- // repeat current source row
				continue
			}

			// inside of key
			if isKey {

				if !isFound {
					isFound = true // first key found
					nf = si
				}
				// match the language
				isMatch = mc.modelLst[idx].txtMeta.TypeEnumTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.TypeEnumTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.TypeEnumTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.TypeEnumTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TypeEnumTxt[si].TypeId > tId ||
					mc.modelLst[idx].txtMeta.TypeEnumTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TypeEnumTxt[si].TypeId == tId &&
						mc.modelLst[idx].txtMeta.TypeEnumTxt[si].EnumId > eId) {

				ci++ // move to next type
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && pi < len(mt.TypeTxt) && ci < len(mt.TypeTxt[pi].TypeEnumTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.TypeEnumTxt) {
				mt.TypeTxt[pi].TypeEnumTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].Note}
			}
		}
	}

	// set parameter description and notes
	if len(mt.ParamTxt) > 0 && len(mc.modelLst[idx].txtMeta.ParamTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, di int

		for ; si < len(mc.modelLst[idx].txtMeta.ParamTxt); si++ {

			// destination rows must be defined by [di] index
			if di >= len(mt.ParamTxt) {
				break // done with all destination text
			}

			// check if source and destination keys equal
			mId := mt.ParamTxt[di].Param.ModelId
			tId := mt.ParamTxt[di].Param.ParamId

			isKey = mc.modelLst[idx].txtMeta.ParamTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.ParamTxt[si].ParamId == tId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.ParamTxt[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.ParamTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.ParamTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.ParamTxt[ni].Note}

				// reset to start next search
				isFound = false
				isMatch = false
				di++ // move to next parameter
				si-- // repeat current source row
				continue
			}

			// inside of key
			if isKey {

				if !isFound {
					isFound = true // first key found
					nf = si
				}
				// match the language
				isMatch = mc.modelLst[idx].txtMeta.ParamTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.ParamTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.ParamTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.ParamTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.ParamTxt[si].ParamId > tId) {

				di++ // move to next parameter
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && di < len(mt.ParamTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.ParamTxt) {
				mt.ParamTxt[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.ParamTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.ParamTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.ParamTxt[ni].Note}
			}
		}
	}

	// set parameter dimensions description and notes
	if len(mt.ParamTxt) > 0 && len(mc.modelLst[idx].txtMeta.ParamDimsTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, pi, ci int

		for ; si < len(mc.modelLst[idx].txtMeta.ParamDimsTxt); si++ {

			// destination rows: parent and child must be defined by valid (pi, ci) indexes
			if pi >= len(mt.ParamTxt) {
				break // done with all destination text
			}
			if pi < len(mt.ParamTxt) &&
				ci >= len(mt.ParamTxt[pi].ParamDimsTxt) {

				if pi++; pi >= len(mt.ParamTxt) {
					break // done with all destination text
				}

				ci = 0 // move to next type
				si--   // repeat current source row
				continue
			}

			// check if source and destination keys equal
			mId := mt.ParamTxt[pi].Param.ModelId
			pId := mt.ParamTxt[pi].Param.ParamId
			dimId := mt.ParamTxt[pi].ParamDimsTxt[ci].Dim.DimId

			isKey = mc.modelLst[idx].txtMeta.ParamDimsTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.ParamDimsTxt[si].ParamId == pId &&
				mc.modelLst[idx].txtMeta.ParamDimsTxt[si].DimId == dimId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.ParamTxt[pi].ParamDimsTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].Note}

				// reset to start next search
				isFound = false
				isMatch = false
				ci++ // move to next type
				si-- // repeat current source row
				continue
			}

			// inside of key
			if isKey {

				if !isFound {
					isFound = true // first key found
					nf = si
				}
				// match the language
				isMatch = mc.modelLst[idx].txtMeta.ParamDimsTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.ParamDimsTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.ParamDimsTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.ParamDimsTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.ParamDimsTxt[si].ParamId > pId ||
					mc.modelLst[idx].txtMeta.ParamDimsTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.ParamDimsTxt[si].ParamId == pId &&
						mc.modelLst[idx].txtMeta.ParamDimsTxt[si].DimId > dimId) {

				ci++ // move to next type
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && pi < len(mt.ParamTxt) && ci < len(mt.ParamTxt[pi].ParamDimsTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.ParamDimsTxt) {
				mt.ParamTxt[pi].ParamDimsTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].Note}
			}
		}
	}

	// set output table description and notes
	if len(mt.TableTxt) > 0 && len(mc.modelLst[idx].txtMeta.TableTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, di int

		for ; si < len(mc.modelLst[idx].txtMeta.TableTxt); si++ {

			// destination rows must be defined by [di] index
			if di >= len(mt.TableTxt) {
				break // done with all destination text
			}

			// check if source and destination keys equal
			mId := mt.TableTxt[di].Table.ModelId
			tId := mt.TableTxt[di].Table.TableId

			isKey = mc.modelLst[idx].txtMeta.TableTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.TableTxt[si].TableId == tId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.TableTxt[di].LangCode = mc.modelLst[idx].txtMeta.TableTxt[ni].LangCode
				mt.TableTxt[di].TableDescr = mc.modelLst[idx].txtMeta.TableTxt[ni].Descr
				mt.TableTxt[di].TableNote = mc.modelLst[idx].txtMeta.TableTxt[ni].Note
				mt.TableTxt[di].ExprDescr = mc.modelLst[idx].txtMeta.TableTxt[ni].ExprDescr
				mt.TableTxt[di].ExprNote = mc.modelLst[idx].txtMeta.TableTxt[ni].ExprNote

				// reset to start next search
				isFound = false
				isMatch = false
				di++ // move to next output table
				si-- // repeat current source row
				continue
			}

			// inside of key
			if isKey {

				if !isFound {
					isFound = true // first key found
					nf = si
				}
				// match the language
				isMatch = mc.modelLst[idx].txtMeta.TableTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.TableTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.TableTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.TableTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TableTxt[si].TableId > tId) {

				di++ // move to next output table
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && di < len(mt.TableTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.TableTxt) {
				mt.TableTxt[di].LangCode = mc.modelLst[idx].txtMeta.TableTxt[ni].LangCode
				mt.TableTxt[di].TableDescr = mc.modelLst[idx].txtMeta.TableTxt[ni].Descr
				mt.TableTxt[di].TableNote = mc.modelLst[idx].txtMeta.TableTxt[ni].Note
				mt.TableTxt[di].ExprDescr = mc.modelLst[idx].txtMeta.TableTxt[ni].ExprDescr
				mt.TableTxt[di].ExprNote = mc.modelLst[idx].txtMeta.TableTxt[ni].ExprNote
			}
		}
	}

	// set output table dimensions description and notes
	if len(mt.TableTxt) > 0 && len(mc.modelLst[idx].txtMeta.TableDimsTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, pi, ci int

		for ; si < len(mc.modelLst[idx].txtMeta.TableDimsTxt); si++ {

			// destination rows: parent and child must be defined by valid (pi, ci) indexes
			if pi >= len(mt.TableTxt) {
				break // done with all destination text
			}
			if pi < len(mt.TableTxt) &&
				ci >= len(mt.TableTxt[pi].TableDimsTxt) {

				if pi++; pi >= len(mt.TableTxt) {
					break // done with all destination text
				}

				ci = 0 // move to next type
				si--   // repeat current source row
				continue
			}

			// check if source and destination keys equal
			mId := mt.TableTxt[pi].Table.ModelId
			tId := mt.TableTxt[pi].Table.TableId
			dimId := mt.TableTxt[pi].TableDimsTxt[ci].Dim.DimId

			isKey = mc.modelLst[idx].txtMeta.TableDimsTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.TableDimsTxt[si].TableId == tId &&
				mc.modelLst[idx].txtMeta.TableDimsTxt[si].DimId == dimId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.TableTxt[pi].TableDimsTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TableDimsTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TableDimsTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TableDimsTxt[ni].Note}

				// reset to start next search
				isFound = false
				isMatch = false
				ci++ // move to next type
				si-- // repeat current source row
				continue
			}

			// inside of key
			if isKey {

				if !isFound {
					isFound = true // first key found
					nf = si
				}
				// match the language
				isMatch = mc.modelLst[idx].txtMeta.TableDimsTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.TableDimsTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.TableDimsTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.TableDimsTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TableDimsTxt[si].TableId > tId ||
					mc.modelLst[idx].txtMeta.TableDimsTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TableDimsTxt[si].TableId == tId &&
						mc.modelLst[idx].txtMeta.TableDimsTxt[si].DimId > dimId) {

				ci++ // move to next type
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && pi < len(mt.TableTxt) && ci < len(mt.TableTxt[pi].TableDimsTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.TableDimsTxt) {
				mt.TableTxt[pi].TableDimsTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TableDimsTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TableDimsTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TableDimsTxt[ni].Note}
			}
		}
	}

	// set output table accumulators description and notes
	if len(mt.TableTxt) > 0 && len(mc.modelLst[idx].txtMeta.TableAccTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, pi, ci int

		for ; si < len(mc.modelLst[idx].txtMeta.TableAccTxt); si++ {

			// destination rows: parent and child must be defined by valid (pi, ci) indexes
			if pi >= len(mt.TableTxt) {
				break // done with all destination text
			}
			if pi < len(mt.TableTxt) &&
				ci >= len(mt.TableTxt[pi].TableAccTxt) {

				if pi++; pi >= len(mt.TableTxt) {
					break // done with all destination text
				}

				ci = 0 // move to next type
				si--   // repeat current source row
				continue
			}

			// check if source and destination keys equal
			mId := mt.TableTxt[pi].Table.ModelId
			tId := mt.TableTxt[pi].Table.TableId
			accId := mt.TableTxt[pi].TableAccTxt[ci].Acc.AccId

			isKey = mc.modelLst[idx].txtMeta.TableAccTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.TableAccTxt[si].TableId == tId &&
				mc.modelLst[idx].txtMeta.TableAccTxt[si].AccId == accId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.TableTxt[pi].TableAccTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TableAccTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TableAccTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TableAccTxt[ni].Note}

				// reset to start next search
				isFound = false
				isMatch = false
				ci++ // move to next type
				si-- // repeat current source row
				continue
			}

			// inside of key
			if isKey {

				if !isFound {
					isFound = true // first key found
					nf = si
				}
				// match the language
				isMatch = mc.modelLst[idx].txtMeta.TableAccTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.TableAccTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.TableAccTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.TableAccTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TableAccTxt[si].TableId > tId ||
					mc.modelLst[idx].txtMeta.TableAccTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TableAccTxt[si].TableId == tId &&
						mc.modelLst[idx].txtMeta.TableAccTxt[si].AccId > accId) {

				ci++ // move to next type
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && pi < len(mt.TableTxt) && ci < len(mt.TableTxt[pi].TableAccTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.TableAccTxt) {
				mt.TableTxt[pi].TableAccTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TableAccTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TableAccTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TableAccTxt[ni].Note}
			}
		}
	}

	// set output table expressions description and notes
	if len(mt.TableTxt) > 0 && len(mc.modelLst[idx].txtMeta.TableExprTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, pi, ci int

		for ; si < len(mc.modelLst[idx].txtMeta.TableExprTxt); si++ {

			// destination rows: parent and child must be defined by valid (pi, ci) indexes
			if pi >= len(mt.TableTxt) {
				break // done with all destination text
			}
			if pi < len(mt.TableTxt) &&
				ci >= len(mt.TableTxt[pi].TableExprTxt) {

				if pi++; pi >= len(mt.TableTxt) {
					break // done with all destination text
				}

				ci = 0 // move to next type
				si--   // repeat current source row
				continue
			}

			// check if source and destination keys equal
			mId := mt.TableTxt[pi].Table.ModelId
			tId := mt.TableTxt[pi].Table.TableId
			exprId := mt.TableTxt[pi].TableExprTxt[ci].Expr.ExprId

			isKey = mc.modelLst[idx].txtMeta.TableExprTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.TableExprTxt[si].TableId == tId &&
				mc.modelLst[idx].txtMeta.TableExprTxt[si].ExprId == exprId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.TableTxt[pi].TableExprTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TableExprTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TableExprTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TableExprTxt[ni].Note}

				// reset to start next search
				isFound = false
				isMatch = false
				ci++ // move to next type
				si-- // repeat current source row
				continue
			}

			// inside of key
			if isKey {

				if !isFound {
					isFound = true // first key found
					nf = si
				}
				// match the language
				isMatch = mc.modelLst[idx].txtMeta.TableExprTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.TableExprTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.TableExprTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.TableExprTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TableExprTxt[si].TableId > tId ||
					mc.modelLst[idx].txtMeta.TableExprTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.TableExprTxt[si].TableId == tId &&
						mc.modelLst[idx].txtMeta.TableExprTxt[si].ExprId > exprId) {

				ci++ // move to next type
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && pi < len(mt.TableTxt) && ci < len(mt.TableTxt[pi].TableExprTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.TableExprTxt) {
				mt.TableTxt[pi].TableExprTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.TableExprTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.TableExprTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.TableExprTxt[ni].Note}
			}
		}
	}

	return &mt, false
}

// loadModelMetaText reads language-specific model metadata from db by digest or name.
// If metadata already loaded then skip db reading and return index in model list.
// Return index in model list or < 0 on error or if model digest not found.
func (mc *ModelCatalog) loadModelMetaText(dn string) int {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return -1
	}

	// find model index by digest-or-name
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	idx := mc.indexByDigestOrName(dn)
	if idx < 0 {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return idx // model not found, index is negative
	}
	if mc.modelLst[idx].txtMeta != nil && mc.modelLst[idx].isTxtMetaFull { // exit if model metadata already loaded
		return idx
	}

	// read metadata from database
	m, err := db.GetModelText(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, "")
	if err != nil {
		omppLog.Log("Error at get model text metadata: ", dn, ": ", err.Error())
		return -1
	}

	// store model text metadata
	mc.modelLst[idx].isTxtMetaFull = true
	mc.modelLst[idx].txtMeta = m

	return idx
}
