// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"net/http"

	"golang.org/x/text/language"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// modelTextHandler return language-specific model metadata:
// GET /api/model/:model/text
// GET /api/model/:model/text/lang/:lang
// Model digest-or-name must specified, if multiple models with same name exist only one is returned.
// If optional lang specified then result in that language else in browser language or model default.
func modelTextHandler(w http.ResponseWriter, r *http.Request) {

	type aDescrNote struct {
		LangCode *string // lang_code VARCHAR(32)  NOT NULL
		Descr    *string // descr     VARCHAR(255) NOT NULL
		Note     *string // note      VARCHAR(32000)
	}

	// TypeEnumDescrNote is join of type_enum_lst, model_type_dic, type_enum_txt
	type TypeEnumDescrNote struct {
		Enum      *db.TypeEnumRow // type enum row: type_enum_lst join to model_type_dic
		DescrNote aDescrNote      // from type_enum_txt
	}
	// TypeDescrNote is join of type_dic_txt, model_type_dic, type_dic_txt
	type TypeDescrNote struct {
		Type        *db.TypeDicRow      // model type row: type_dic join to model_type_dic
		DescrNote   aDescrNote          // from type_dic_txt
		TypeEnumTxt []TypeEnumDescrNote // type enum text rows: type_enum_txt join to model_type_dic
	}

	// ParamDimsDescrNote is join of parameter_dims, model_parameter_dic, parameter_dims_txt
	type ParamDimsDescrNote struct {
		Dim       *db.ParamDimsRow // parameter dimension row: parameter_dims join to model_parameter_dic table
		DescrNote aDescrNote       // from parameter_dims_txt
	}
	// ParamDescrNote is join of parameter_dic, model_parameter_dic, parameter_dic_txt, parameter_dims_txt
	type ParamDescrNote struct {
		Param        *db.ParamDicRow      // parameter row: parameter_dic join to model_parameter_dic table
		DescrNote    aDescrNote           // from parameter_dic_txt
		ParamDimsTxt []ParamDimsDescrNote // parameter dimension text rows: parameter_dims_txt join to model_parameter_dic
	}

	// TableDimsDescrNote is join of table_dims, model_table_dic, table_dims_txt
	type TableDimsDescrNote struct {
		Dim       *db.TableDimsRow // parameter dimension row: table_dims join to model_table_dic table
		DescrNote aDescrNote       // from table_dims_txt
	}

	// TableAccDescrNote is join of table_acc, model_table_dic, table_acc_txt
	type TableAccDescrNote struct {
		Acc       *db.TableAccRow // output table accumulator row: table_acc join to model_table_dic
		DescrNote aDescrNote      // from table_acc_txt
	}

	// TableExprDescrNote is join of table_expr, model_table_dic, table_expr_txt
	type TableExprDescrNote struct {
		Expr      *db.TableExprRow // output table expression row: table_expr join to model_table_dic
		DescrNote aDescrNote       // from table_expr_txt
	}

	// TableDescrNote is join of table_dic, model_table_dic, table_dic_txt, table_dims_txt, table_acc_txt, table_expr_txt
	type TableDescrNote struct {
		Table        *db.TableDicRow      // output table row: table_dic join to model_table_dic
		LangCode     *string              // table_dic_txt.lang_code
		TableDescr   *string              // table_dic_txt.descr
		TableNote    *string              // table_dic_txt.note
		ExprDescr    *string              // table_dic_txt.expr_descr
		ExprNote     *string              // table_dic_txt.expr_note
		TableDimsTxt []TableDimsDescrNote // output table dimension text rows: table_dims_txt join to model_table_dic
		TableAccTxt  []TableAccDescrNote  // output table accumulator text rows: table_acc_txt join to model_table_dic
		TableExprTxt []TableExprDescrNote // output table expression text rows: table_expr_txt join to model_table_dic
	}

	// EntityAttrDescrNote is join of entity_attr, model_entity_dic, entity_attr_txt
	type EntityAttrDescrNote struct {
		Attr      *db.EntityAttrRow // entity attribute row: entity_attr join to model_entity_dic table
		DescrNote aDescrNote        // from entity_attr_txt
	}

	// EntityDescrNote is join of entity_dic, model_entity_dic, entity_dic_txt, entity_attr_txt
	type EntityDescrNote struct {
		Entity        *db.EntityDicRow      // entity row: entity_dic join to model_entity_dic
		DescrNote     aDescrNote            // from entity_dic_txt
		EntityAttrTxt []EntityAttrDescrNote // entity attribute text rows: entity_attr, model_entity_dic, entity_attr_txt
	}

	// GroupDescrNote is join of group_lst, group_pc and group_txt
	type GroupDescrNote struct {
		Group     *db.GroupMeta // parameters or output tables group rows: group_lst join to group_pc
		DescrNote aDescrNote    // from group_txt
	}
	// language-specific model metadata db rows.
	// It is sliced by one single language, but it can be different single language for each row.
	// It is either user preferred language, model default language, first of the row or empty "" language.
	type modelMetaDescrNote struct {
		ModelDicDescrNote                   // model text rows: model_dic_txt
		TypeTxt           []TypeDescrNote   // model type text rows: type_dic_txt join to model_type_dic
		ParamTxt          []ParamDescrNote  // model parameter text rows: parameter_dic, model_parameter_dic, parameter_dic_txt, parameter_dims_txt
		TableTxt          []TableDescrNote  // model output table text rows: table_dic, model_table_dic, table_dic_txt, table_dims_txt, table_acc_txt, table_expr_txt
		EntityTxt         []EntityDescrNote // model entity text rows: join of entity_dic, model_entity_dic, entity_dic_txt, entity_attr_txt
		GroupTxt          []GroupDescrNote  // model group text rows: group_txt join to group_lst
	}

	// ModelMetaTextByDigestOrName return language-specific model metadata
	// by model digest or name and preferred language tags.
	// It can be in default model language or empty if no model text db rows exist.
	getText := func(mc *ModelCatalog, dn string, preferredLang []language.Tag) (*modelMetaDescrNote, bool) {

		// if model digest-or-name is empty then return empty results
		if dn == "" {
			omppLog.Log("Warning: invalid (empty) model digest and name")
			return &modelMetaDescrNote{}, false
		}

		// find model in catalog
		mdRow, ok := theCatalog.ModelDicByDigestOrName(dn)
		if !ok {
			omppLog.Log("Warning: model digest or name not found: ", dn)
			return &modelMetaDescrNote{}, false // return empty result: model not found or error
		}

		// if language-specific model metadata not loaded then read it from database
		if ok := mc.loadModelText(mdRow.Digest); !ok {
			omppLog.Log("Warning: model digest or name not found: ", dn)
			return &modelMetaDescrNote{}, false // return empty result: model not found or error
		}

		// match preferred languages and model languages
		lc := mc.languageTagMatch(mdRow.Digest, preferredLang)
		lcd, _, _ := mc.modelLangs(mdRow.Digest)
		if lc == "" && lcd == "" {
			omppLog.Log("Error: invalid (empty) model default language: ", dn)
			return &modelMetaDescrNote{}, false // return empty result: model not found or error
		}

		// lock model catalog and copy text metadata for perfered language or default model language
		mc.theLock.Lock()
		defer mc.theLock.Unlock()

		// initialaze text metadata with copy of language-neutral metadata
		idx, ok := mc.indexByDigest(mdRow.Digest)
		if !ok {
			omppLog.Log("Warning: model digest or name not found: ", dn)
			return &modelMetaDescrNote{}, false // return empty result: model not found or error
		}

		mt := modelMetaDescrNote{
			ModelDicDescrNote: ModelDicDescrNote{Model: mc.modelLst[idx].meta.Model},
			TypeTxt:           make([]TypeDescrNote, len(mc.modelLst[idx].meta.Type)),
			ParamTxt:          make([]ParamDescrNote, len(mc.modelLst[idx].meta.Param)),
			TableTxt:          make([]TableDescrNote, len(mc.modelLst[idx].meta.Table)),
			EntityTxt:         make([]EntityDescrNote, len(mc.modelLst[idx].meta.Entity)),
			GroupTxt:          make([]GroupDescrNote, len(mc.modelLst[idx].meta.Group)),
		}
		emptyStr := ""

		// model types
		for k := range mt.TypeTxt {
			mt.TypeTxt[k].Type = &mc.modelLst[idx].meta.Type[k].TypeDicRow
			mt.TypeTxt[k].TypeEnumTxt = make([]TypeEnumDescrNote, len(mc.modelLst[idx].meta.Type[k].Enum))
			mt.TypeTxt[k].DescrNote.LangCode = &emptyStr
			mt.TypeTxt[k].DescrNote.Descr = &emptyStr
			mt.TypeTxt[k].DescrNote.Note = &emptyStr

			for j := range mt.TypeTxt[k].TypeEnumTxt {
				mt.TypeTxt[k].TypeEnumTxt[j].Enum = &mc.modelLst[idx].meta.Type[k].Enum[j]
				mt.TypeTxt[k].TypeEnumTxt[j].DescrNote.LangCode = &emptyStr
				mt.TypeTxt[k].TypeEnumTxt[j].DescrNote.Descr = &emptyStr
				mt.TypeTxt[k].TypeEnumTxt[j].DescrNote.Note = &emptyStr
			}
		}

		// model parameters
		for k := range mt.ParamTxt {
			mt.ParamTxt[k].Param = &mc.modelLst[idx].meta.Param[k].ParamDicRow
			mt.ParamTxt[k].ParamDimsTxt = make([]ParamDimsDescrNote, len(mc.modelLst[idx].meta.Param[k].Dim))
			mt.ParamTxt[k].DescrNote.LangCode = &emptyStr
			mt.ParamTxt[k].DescrNote.Descr = &emptyStr
			mt.ParamTxt[k].DescrNote.Note = &emptyStr

			for j := range mt.ParamTxt[k].ParamDimsTxt {
				mt.ParamTxt[k].ParamDimsTxt[j].Dim = &mc.modelLst[idx].meta.Param[k].Dim[j]
				mt.ParamTxt[k].ParamDimsTxt[j].DescrNote.LangCode = &emptyStr
				mt.ParamTxt[k].ParamDimsTxt[j].DescrNote.Descr = &emptyStr
				mt.ParamTxt[k].ParamDimsTxt[j].DescrNote.Note = &emptyStr
			}
		}

		// model output tables, remove sql for accumulators and expressions
		for k := range mt.TableTxt {
			mt.TableTxt[k].Table = &mc.modelLst[idx].meta.Table[k].TableDicRow
			mt.TableTxt[k].TableDimsTxt = make([]TableDimsDescrNote, len(mc.modelLst[idx].meta.Table[k].Dim))
			mt.TableTxt[k].TableAccTxt = make([]TableAccDescrNote, len(mc.modelLst[idx].meta.Table[k].Acc))
			mt.TableTxt[k].TableExprTxt = make([]TableExprDescrNote, len(mc.modelLst[idx].meta.Table[k].Expr))
			mt.TableTxt[k].LangCode = &emptyStr
			mt.TableTxt[k].TableDescr = &emptyStr
			mt.TableTxt[k].TableNote = &emptyStr
			mt.TableTxt[k].ExprDescr = &emptyStr
			mt.TableTxt[k].ExprNote = &emptyStr

			for j := range mt.TableTxt[k].TableDimsTxt {
				mt.TableTxt[k].TableDimsTxt[j].Dim = &mc.modelLst[idx].meta.Table[k].Dim[j]
				mt.TableTxt[k].TableDimsTxt[j].DescrNote.LangCode = &emptyStr
				mt.TableTxt[k].TableDimsTxt[j].DescrNote.Descr = &emptyStr
				mt.TableTxt[k].TableDimsTxt[j].DescrNote.Note = &emptyStr
			}
			for j := range mt.TableTxt[k].TableAccTxt {
				mt.TableTxt[k].TableAccTxt[j].Acc = &mc.modelLst[idx].meta.Table[k].Acc[j]
				// mt.TableTxt[k].TableAccTxt[j].Acc.AccSql = "" // remove sql of accumulator
				mt.TableTxt[k].TableAccTxt[j].DescrNote.LangCode = &emptyStr
				mt.TableTxt[k].TableAccTxt[j].DescrNote.Descr = &emptyStr
				mt.TableTxt[k].TableAccTxt[j].DescrNote.Note = &emptyStr
			}
			for j := range mt.TableTxt[k].TableExprTxt {
				mt.TableTxt[k].TableExprTxt[j].Expr = &mc.modelLst[idx].meta.Table[k].Expr[j]
				// mt.TableTxt[k].TableExprTxt[j].Expr.ExprSql = "" // remove sql of expression
				mt.TableTxt[k].TableExprTxt[j].DescrNote.LangCode = &emptyStr
				mt.TableTxt[k].TableExprTxt[j].DescrNote.Descr = &emptyStr
				mt.TableTxt[k].TableExprTxt[j].DescrNote.Note = &emptyStr
			}
		}

		// model entities
		for k := range mt.EntityTxt {
			mt.EntityTxt[k].Entity = &mc.modelLst[idx].meta.Entity[k].EntityDicRow
			mt.EntityTxt[k].EntityAttrTxt = make([]EntityAttrDescrNote, len(mc.modelLst[idx].meta.Entity[k].Attr))
			mt.EntityTxt[k].DescrNote.LangCode = &emptyStr
			mt.EntityTxt[k].DescrNote.Descr = &emptyStr
			mt.EntityTxt[k].DescrNote.Note = &emptyStr

			for j := range mt.EntityTxt[k].EntityAttrTxt {
				mt.EntityTxt[k].EntityAttrTxt[j].Attr = &mc.modelLst[idx].meta.Entity[k].Attr[j]
				mt.EntityTxt[k].EntityAttrTxt[j].DescrNote.LangCode = &emptyStr
				mt.EntityTxt[k].EntityAttrTxt[j].DescrNote.Descr = &emptyStr
				mt.EntityTxt[k].EntityAttrTxt[j].DescrNote.Note = &emptyStr
			}
		}

		// model groups
		for k := range mt.GroupTxt {
			mt.GroupTxt[k].DescrNote.LangCode = &emptyStr
			mt.GroupTxt[k].DescrNote.Descr = &emptyStr
			mt.GroupTxt[k].DescrNote.Note = &emptyStr

			mt.GroupTxt[k].Group = &db.GroupMeta{
				GroupLstRow: mc.modelLst[idx].meta.Group[k].GroupLstRow,
				GroupPc:     mc.modelLst[idx].meta.Group[k].GroupPc,
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
					mt.TypeTxt[di].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TypeTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TypeTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TypeTxt[ni].Note}

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
					mt.TypeTxt[di].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TypeTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TypeTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TypeTxt[ni].Note}
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
					mt.TypeTxt[pi].TypeEnumTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].Note}

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
					mt.TypeTxt[pi].TypeEnumTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TypeEnumTxt[ni].Note}
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
					mt.ParamTxt[di].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.ParamTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.ParamTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.ParamTxt[ni].Note}

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
					mt.ParamTxt[di].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.ParamTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.ParamTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.ParamTxt[ni].Note}
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
					mt.ParamTxt[pi].ParamDimsTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].Note}

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
					mt.ParamTxt[pi].ParamDimsTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.ParamDimsTxt[ni].Note}
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
					mt.TableTxt[di].LangCode = &mc.modelLst[idx].txtMeta.TableTxt[ni].LangCode
					mt.TableTxt[di].TableDescr = &mc.modelLst[idx].txtMeta.TableTxt[ni].Descr
					mt.TableTxt[di].TableNote = &mc.modelLst[idx].txtMeta.TableTxt[ni].Note
					mt.TableTxt[di].ExprDescr = &mc.modelLst[idx].txtMeta.TableTxt[ni].ExprDescr
					mt.TableTxt[di].ExprNote = &mc.modelLst[idx].txtMeta.TableTxt[ni].ExprNote

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
					mt.TableTxt[di].LangCode = &mc.modelLst[idx].txtMeta.TableTxt[ni].LangCode
					mt.TableTxt[di].TableDescr = &mc.modelLst[idx].txtMeta.TableTxt[ni].Descr
					mt.TableTxt[di].TableNote = &mc.modelLst[idx].txtMeta.TableTxt[ni].Note
					mt.TableTxt[di].ExprDescr = &mc.modelLst[idx].txtMeta.TableTxt[ni].ExprDescr
					mt.TableTxt[di].ExprNote = &mc.modelLst[idx].txtMeta.TableTxt[ni].ExprNote
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
					mt.TableTxt[pi].TableDimsTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TableDimsTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TableDimsTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TableDimsTxt[ni].Note}

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
					mt.TableTxt[pi].TableDimsTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TableDimsTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TableDimsTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TableDimsTxt[ni].Note}
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
					mt.TableTxt[pi].TableAccTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TableAccTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TableAccTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TableAccTxt[ni].Note}

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
					mt.TableTxt[pi].TableAccTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TableAccTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TableAccTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TableAccTxt[ni].Note}
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
					mt.TableTxt[pi].TableExprTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TableExprTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TableExprTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TableExprTxt[ni].Note}

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
					mt.TableTxt[pi].TableExprTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.TableExprTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.TableExprTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.TableExprTxt[ni].Note}
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
					mt.EntityTxt[di].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.EntityTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.EntityTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.EntityTxt[ni].Note}

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
					mt.EntityTxt[di].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.EntityTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.EntityTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.EntityTxt[ni].Note}
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
					mt.EntityTxt[pi].EntityAttrTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].Note}

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
					mt.EntityTxt[pi].EntityAttrTxt[ci].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.EntityAttrTxt[ni].Note}
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
					mt.GroupTxt[di].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.GroupTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.GroupTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.GroupTxt[ni].Note}

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
					mt.GroupTxt[di].DescrNote = aDescrNote{
						LangCode: &mc.modelLst[idx].txtMeta.GroupTxt[ni].LangCode,
						Descr:    &mc.modelLst[idx].txtMeta.GroupTxt[ni].Descr,
						Note:     &mc.modelLst[idx].txtMeta.GroupTxt[ni].Note}
				}
			}
		}

		return &mt, false
	}

	//
	// actual hhtp handler
	//

	dn := getRequestParam(r, "model")
	rqLangTags := getRequestLang(r, "lang") // get optional language argument and languages accepted by browser

	mt, _ := getText(&theCatalog, dn, rqLangTags)

	jsonResponse(w, r, mt)
}
