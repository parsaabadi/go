// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"golang.org/x/text/language"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// ModelTextByDigest return model_dic_txt db row by model digest and preferred language tags.
// It can be in preferred language, default model language or empty if no model model_dic_txt rows exist.
func (mc *ModelCatalog) ModelTextByDigest(digest string, preferredLang []language.Tag) (*ModelDicDescrNote, bool) {

	// if model digest is empty then return empty results
	if digest == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &ModelDicDescrNote{}, false
	}

	// get model_dic row
	mdRow, ok := mc.ModelDicByDigest(digest)
	if !ok {
		omppLog.Log("Warning: model digest not found: ", digest)
		return &ModelDicDescrNote{}, false // return empty result: model not found or error
	}

	// get model_dic_txt rows from catalog: it is loaded at catalog initialization
	_, txt := mc.modelTextMeta(digest)
	if txt == nil {
		return &ModelDicDescrNote{}, false // return empty result: model not found or error
	}

	// match preferred languages and model languages
	lc := mc.languageTagMatch(digest, preferredLang)
	lcd, _, _ := mc.modelLangs(digest)
	if lc == "" && lcd == "" {
		omppLog.Log("Error: invalid (empty) model default language: ", digest)
		return &ModelDicDescrNote{}, false
	}

	// if model_dic_txt rows not empty then find row by matched language or by default language
	t := ModelDicDescrNote{Model: mdRow}

	if len(txt.ModelTxt) > 0 {

		var nd, i int
		for ; i < len(txt.ModelTxt); i++ {
			if txt.ModelTxt[i].LangCode == lc {
				break // language match
			}
			if txt.ModelTxt[i].LangCode == lcd {
				nd = i // index of default language
			}
		}
		if i >= len(txt.ModelTxt) {
			i = nd // use default language or zero index row
		}

		t.DescrNote = db.DescrNote{
			LangCode: txt.ModelTxt[i].LangCode,
			Descr:    txt.ModelTxt[i].Descr,
			Note:     txt.ModelTxt[i].Note}
	}
	return &t, true
}

// ModelMetaAllTextByDigest return language-specific model metadata by model digest or name in all languages.
func (mc *ModelCatalog) ModelMetaAllTextByDigest(dn string) (*db.ModelTxtMeta, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.ModelTxtMeta{}, false
	}

	// if language-specific model metadata not loaded then read it from database
	if ok := mc.loadModelText(dn); !ok {
		return &db.ModelTxtMeta{}, false // return empty result: model not found or error
	}

	// return a copy of model text metadata from catalog
	return mc.ModelTextByDigestOrName(dn)
}

// loadModelText reads language-specific model metadata from db by digest or name.
// If metadata already loaded then skip db reading and return success.
func (mc *ModelCatalog) loadModelText(dn string) bool {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return false
	}

	// get model_dic row
	mdRow, ok := mc.ModelDicByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false // model not found or error
	}

	// check if model text metadata already fully loaded from database
	if isFull, _ := mc.modelTextMeta(mdRow.Digest); isFull {
		return true
	}
	// else: no model text in catalog: read from database and update catalog

	// get database connection
	_, dbConn, ok := mc.modelMeta(mdRow.Digest)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return false // model not found or error
	}

	// read model text metadata from database and update catalog
	txt, err := db.GetModelText(dbConn, mdRow.ModelId, "")
	if err != nil {
		omppLog.Log("Error at get model text metadata: ", dn, ": ", err.Error())
		return false
	}

	ok = mc.setModelTextMeta(mdRow.Digest, true, txt)
	if !ok {
		omppLog.Log("Error: model digest not found: ", mdRow.Digest)
		return false // model not found or error
	}
	return true
}

// ModelMetaTextByDigestOrName return language-specific model metadata
// by model digest or name and preferred language tags.
// It can be in default model language or empty if no model text db rows exist.
func (mc *ModelCatalog) ModelMetaTextByDigestOrName(dn string, preferredLang []language.Tag) (*ModelMetaDescrNote, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &ModelMetaDescrNote{}, false
	}

	// find model in catalog
	mdRow, ok := mc.ModelDicByDigestOrName(dn)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &ModelMetaDescrNote{}, false // return empty result: model not found or error
	}

	// if language-specific model metadata not loaded then read it from database
	if ok := mc.loadModelText(mdRow.Digest); !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &ModelMetaDescrNote{}, false // return empty result: model not found or error
	}

	// match preferred languages and model languages
	lc := mc.languageTagMatch(mdRow.Digest, preferredLang)
	lcd, _, _ := mc.modelLangs(mdRow.Digest)
	if lc == "" && lcd == "" {
		omppLog.Log("Error: invalid (empty) model default language: ", dn)
		return &ModelMetaDescrNote{}, false // return empty result: model not found or error
	}

	// lock model catalog and copy text metadata for perfered language or default model language
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// initialaze text metadata with copy of language-neutral metadata
	idx, ok := mc.indexByDigest(mdRow.Digest)
	if !ok {
		omppLog.Log("Warning: model digest or name not found: ", dn)
		return &ModelMetaDescrNote{}, false // return empty result: model not found or error
	}

	mt := ModelMetaDescrNote{
		ModelDicDescrNote: ModelDicDescrNote{Model: mc.modelLst[idx].meta.Model},
		TypeTxt:           make([]TypeDescrNote, len(mc.modelLst[idx].meta.Type)),
		ParamTxt:          make([]ParamDescrNote, len(mc.modelLst[idx].meta.Param)),
		TableTxt:          make([]TableDescrNote, len(mc.modelLst[idx].meta.Table)),
		EntityTxt:         make([]EntityDescrNote, len(mc.modelLst[idx].meta.Entity)),
		GroupTxt:          make([]GroupDescrNote, len(mc.modelLst[idx].meta.Group))}

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

	// model output tables, remove sql for accumulators and expressions
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
			mt.TableTxt[k].TableAccTxt[j].Acc.AccSql = "" // remove sql of accumulator
		}
		for j := range mt.TableTxt[k].TableExprTxt {
			mt.TableTxt[k].TableExprTxt[j].Expr = mc.modelLst[idx].meta.Table[k].Expr[j]
			mt.TableTxt[k].TableExprTxt[j].Expr.ExprSql = "" // remove sql of expression
		}
	}

	// model entities
	for k := range mt.EntityTxt {
		mt.EntityTxt[k].Entity = mc.modelLst[idx].meta.Entity[k].EntityDicRow
		mt.EntityTxt[k].EntityAttrTxt = make([]EntityAttrDescrNote, len(mc.modelLst[idx].meta.Entity[k].Attr))

		for j := range mt.EntityTxt[k].EntityAttrTxt {
			mt.EntityTxt[k].EntityAttrTxt[j].Attr = mc.modelLst[idx].meta.Entity[k].Attr[j]
		}
	}

	// model groups
	for k := range mt.GroupTxt {
		mt.GroupTxt[k].Group = db.GroupMeta{
			GroupLstRow: mc.modelLst[idx].meta.Group[k].GroupLstRow,
			GroupPc:     make([]db.GroupPcRow, len(mc.modelLst[idx].meta.Group[k].GroupPc))}

		for j := range mt.GroupTxt[k].Group.GroupPc {
			mt.GroupTxt[k].Group.GroupPc[j] = mc.modelLst[idx].meta.Group[k].GroupPc[j]
		}
	}

	//
	// set language-specific rows by matched language or by default language or by zero index language
	//

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
		mt.DescrNote = db.DescrNote{
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

	// set entity description and notes
	if len(mt.EntityTxt) > 0 && len(mc.modelLst[idx].txtMeta.EntityTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, di int

		for ; si < len(mc.modelLst[idx].txtMeta.EntityTxt); si++ {

			// destination rows must be defined by [di] index
			if di >= len(mt.EntityTxt) {
				break // done with all destination text
			}

			// check if source and destination keys equal
			mId := mt.EntityTxt[di].Entity.ModelId
			tId := mt.EntityTxt[di].Entity.EntityId

			isKey = mc.modelLst[idx].txtMeta.EntityTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.EntityTxt[si].EntityId == tId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.EntityTxt[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.EntityTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.EntityTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.EntityTxt[ni].Note}

				// reset to start next search
				isFound = false
				isMatch = false
				di++ // move to next entity
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
				isMatch = mc.modelLst[idx].txtMeta.EntityTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.EntityTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.EntityTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.EntityTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.EntityTxt[si].EntityId > tId) {

				di++ // move to next entity
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && di < len(mt.EntityTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.EntityTxt) {
				mt.EntityTxt[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.EntityTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.EntityTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.EntityTxt[ni].Note}
			}
		}
	}

	// set entity attributes description and notes
	if len(mt.EntityTxt) > 0 && len(mc.modelLst[idx].txtMeta.EntityAttrTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, pi, ci int

		for ; si < len(mc.modelLst[idx].txtMeta.EntityAttrTxt); si++ {

			// destination rows: parent and child must be defined by valid (pi, ci) indexes
			if pi >= len(mt.EntityTxt) {
				break // done with all destination text
			}
			if pi < len(mt.EntityTxt) &&
				ci >= len(mt.EntityTxt[pi].EntityAttrTxt) {

				if pi++; pi >= len(mt.EntityTxt) {
					break // done with all destination text
				}

				ci = 0 // move to next type
				si--   // repeat current source row
				continue
			}

			// check if source and destination keys equal
			mId := mt.EntityTxt[pi].Entity.ModelId
			eId := mt.EntityTxt[pi].Entity.EntityId
			aId := mt.EntityTxt[pi].EntityAttrTxt[ci].Attr.AttrId

			isKey = mc.modelLst[idx].txtMeta.EntityAttrTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.EntityAttrTxt[si].EntityId == eId &&
				mc.modelLst[idx].txtMeta.EntityAttrTxt[si].AttrId == aId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.EntityTxt[pi].EntityAttrTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].Note}

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
				isMatch = mc.modelLst[idx].txtMeta.EntityAttrTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.EntityAttrTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.EntityAttrTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.EntityAttrTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.EntityAttrTxt[si].EntityId > eId ||
					mc.modelLst[idx].txtMeta.EntityAttrTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.EntityAttrTxt[si].EntityId == eId &&
						mc.modelLst[idx].txtMeta.EntityAttrTxt[si].AttrId > aId) {

				ci++ // move to next type
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && pi < len(mt.EntityTxt) && ci < len(mt.EntityTxt[pi].EntityAttrTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.EntityAttrTxt) {
				mt.EntityTxt[pi].EntityAttrTxt[ci].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].Note}
			}
		}
	}

	// set group description and notes
	if len(mt.GroupTxt) > 0 && len(mc.modelLst[idx].txtMeta.GroupTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, di int

		for ; si < len(mc.modelLst[idx].txtMeta.GroupTxt); si++ {

			// destination rows must be defined by [di] index
			if di >= len(mt.GroupTxt) {
				break // done with all destination text
			}

			// check if source and destination keys equal
			mId := mt.GroupTxt[di].Group.ModelId
			gId := mt.GroupTxt[di].Group.GroupId

			isKey = mc.modelLst[idx].txtMeta.GroupTxt[si].ModelId == mId &&
				mc.modelLst[idx].txtMeta.GroupTxt[si].GroupId == gId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				mt.GroupTxt[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.GroupTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.GroupTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.GroupTxt[ni].Note}

				// reset to start next search
				isFound = false
				isMatch = false
				di++ // move to next group
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
				isMatch = mc.modelLst[idx].txtMeta.GroupTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].txtMeta.GroupTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].txtMeta.GroupTxt[si].ModelId > mId ||
					mc.modelLst[idx].txtMeta.GroupTxt[si].ModelId == mId &&
						mc.modelLst[idx].txtMeta.GroupTxt[si].GroupId > gId) {

				di++ // move to next group
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && di < len(mt.GroupTxt) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].txtMeta.GroupTxt) {
				mt.GroupTxt[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].txtMeta.GroupTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].txtMeta.GroupTxt[ni].Descr,
					Note:     mc.modelLst[idx].txtMeta.GroupTxt[ni].Note}
			}
		}
	}

	return &mt, false
}
