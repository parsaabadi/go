// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
	"golang.org/x/text/language"
)

// GroupsByDigestOrName return parameter and output table groups,
// language-neutral part of metadata by model digest or name.
func (mc *ModelCatalog) GroupsByDigestOrName(dn string) (*db.GroupLstPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.GroupLstPub{}, false
	}

	// if groups not loaded then read it from database
	idx := mc.loadModelGroups(dn)
	if idx < 0 {
		return &db.GroupLstPub{}, false // return empty result: model not found or error
	}

	// lock model catalog and copy of language-neutral metadata
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	gp := db.GroupLstPub{
		ModelName:   mc.modelLst[idx].groupLst.ModelName,
		ModelDigest: mc.modelLst[idx].groupLst.ModelDigest,
		Group:       make([]db.GroupPub, len(mc.modelLst[idx].groupLst.Group)),
		Pc:          append([]db.GroupPcPub{}, mc.modelLst[idx].groupLst.Pc...)}

	// copy language-neutral part of group rows: group_lst only
	for k := range mc.modelLst[idx].groupLst.Group {
		gp.Group[k] = db.GroupPub{
			GroupId:  mc.modelLst[idx].groupLst.Group[k].GroupId,
			IsParam:  mc.modelLst[idx].groupLst.Group[k].IsParam,
			Name:     mc.modelLst[idx].groupLst.Group[k].Name,
			IsHidden: mc.modelLst[idx].groupLst.Group[k].IsHidden,
			Txt:      []db.DescrNote{},
		}
	}

	return &gp, true
}

// loadModelGroups reads parameter and output table groups from db by digest or name.
// If groups already loaded then skip db reading and return index in model list.
// Return index in model list or < 0 on error or if model digest not found.
func (mc *ModelCatalog) loadModelGroups(dn string) int {

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
	if mc.modelLst[idx].groupLst != nil { // exit if groups already loaded
		return idx
	}

	// read groups from database and convert to "public" model groups format
	g, err := db.GetModelGroup(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, "")
	if err != nil {
		omppLog.Log("Error at get parameter and output table groups: ", dn, ": ", err.Error())
		return -1
	}
	gp, err := g.ToPublic(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta)
	if err != nil {
		omppLog.Log("Error at parameter and output table groups conversion: ", dn, ": ", err.Error())
		return -1
	}

	// store model groups
	mc.modelLst[idx].groupLst = gp
	return idx
}

// GroupsAllTextByDigestOrName return parameter and output table groups with text (description and notes)
// by model digest or name in all languages.
func (mc *ModelCatalog) GroupsAllTextByDigestOrName(dn string) (*db.GroupLstPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.GroupLstPub{}, false
	}

	// if groups not loaded then read it from database
	idx := mc.loadModelGroups(dn)
	if idx < 0 {
		return &db.GroupLstPub{}, false // return empty result: model not found or error
	}

	// lock model catalog and return copy of model metadata
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	g := &db.GroupLstPub{}
	if err := helper.DeepCopy(mc.modelLst[idx].groupLst, g); err != nil {
		omppLog.Log("Error at model groups metadata clone: ", dn, ": ", err.Error())
		return &db.GroupLstPub{}, false
	}

	return g, true
}

// GroupsTextByDigestOrName return parameter and output table groups with text (description and notes)
// by model digest or name and prefered language tags.
// Language-specifc description and notes can be in default model language or empty if no rows db rows exist.
func (mc *ModelCatalog) GroupsTextByDigestOrName(dn string, preferedLang []language.Tag) (*db.GroupLstPub, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &db.GroupLstPub{}, false
	}

	// if groups not loaded then read it from database
	idx := mc.loadModelGroups(dn)
	if idx < 0 {
		return &db.GroupLstPub{}, false // return empty result: model not found or error
	}

	// lock model catalog
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// match prefered languages and model languages
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langCodes[np]
	lcd := mc.modelLst[idx].meta.Model.DefaultLangCode

	// initialaze with copy of language-neutral metadata
	gp := db.GroupLstPub{
		ModelName:   mc.modelLst[idx].groupLst.ModelName,
		ModelDigest: mc.modelLst[idx].groupLst.ModelDigest,
		Group:       make([]db.GroupPub, len(mc.modelLst[idx].groupLst.Group)),
		Pc:          append([]db.GroupPcPub{}, mc.modelLst[idx].groupLst.Pc...)}

	// copy group rows, text only in prefered language or model default language
	for k := range mc.modelLst[idx].groupLst.Group {

		gp.Group[k] = db.GroupPub{
			GroupId:  mc.modelLst[idx].groupLst.Group[k].GroupId,
			IsParam:  mc.modelLst[idx].groupLst.Group[k].IsParam,
			Name:     mc.modelLst[idx].groupLst.Group[k].Name,
			IsHidden: mc.modelLst[idx].groupLst.Group[k].IsHidden,
			Txt:      []db.DescrNote{},
		}

		// find group text in prefered language, model default or zero index language
		nd := 0
		isMatch := false

		for j := range mc.modelLst[idx].groupLst.Group[k].Txt {

			if isMatch = mc.modelLst[idx].groupLst.Group[k].Txt[j].LangCode == lc; isMatch {
				gp.Group[k].Txt = append(gp.Group[k].Txt, mc.modelLst[idx].groupLst.Group[k].Txt[j])
				break // prefered language found
			}
			if mc.modelLst[idx].groupLst.Group[k].Txt[j].LangCode == lcd {
				nd = j // model default language found
			}
		}
		// if prefereed language not found then use model default or zero index language
		if !isMatch && nd < len(mc.modelLst[idx].groupLst.Group[k].Txt) {
			gp.Group[k].Txt = append(gp.Group[k].Txt, mc.modelLst[idx].groupLst.Group[k].Txt[nd])
		}
	}

	return &gp, true
}
