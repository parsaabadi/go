// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"go.openmpp.org/ompp/db"
	"go.openmpp.org/ompp/omppLog"
	"golang.org/x/text/language"
)

// GroupsByDigestOrName return parameter and output table groups,
// language-neutral part of metadata by model digest or name.
func (mc *ModelCatalog) GroupsByDigestOrName(dn string) (*GroupMeta, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &GroupMeta{}, false
	}

	// if groups not loaded then read it from database
	idx := mc.loadModelGroups(dn)
	if idx < 0 {
		return &GroupMeta{}, false // return empty result: model not found or error
	}

	// lock model catalog and copy of language-neutral metadata
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	gm := GroupMeta{
		Group:   append([]db.GroupLstRow{}, mc.modelLst[idx].groupLst.GroupLst...),
		GroupPc: append([]db.GroupPcRow{}, mc.modelLst[idx].groupLst.GroupPc...)}

	return &gm, true
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

	// read groups from database
	g, err := db.GetModelGroup(mc.modelLst[idx].dbConn, mc.modelLst[idx].meta.Model.ModelId, "")
	if err != nil {
		omppLog.Log("Error at get parameter and output table groups: ", dn, ": ", err.Error())
		return -1
	}

	// store model groups
	mc.modelLst[idx].groupLst = g
	return idx
}

// GroupsTextByDigestOrName return parameter and output table groups with text (description and notes)
// by model digest or name and prefered language tags.
// Language-specifc description and notes can be in default model language or empty if no rows db rows exist.
func (mc *ModelCatalog) GroupsTextByDigestOrName(dn string, preferedLang []language.Tag) (*GroupMetaDescrNote, bool) {

	// if model digest-or-name is empty then return empty results
	if dn == "" {
		omppLog.Log("Warning: invalid (empty) model digest and name")
		return &GroupMetaDescrNote{}, false
	}

	// if groups not loaded then read it from database
	idx := mc.loadModelGroups(dn)
	if idx < 0 {
		return &GroupMetaDescrNote{}, false // return empty result: model not found or error
	}

	// lock model catalog
	mc.theLock.Lock()
	defer mc.theLock.Unlock()

	// match prefered languages and model languages
	_, np, _ := mc.modelLst[idx].matcher.Match(preferedLang...)
	lc := mc.modelLst[idx].langLst[np].LangCode
	lcd := mc.modelLst[idx].meta.Model.DefaultLangCode

	// initialaze with copy of language-neutral metadata
	gm := GroupMetaDescrNote{
		GroupLst: make([]GroupDescrNote, len(mc.modelLst[idx].groupLst.GroupLst)),
		GroupPc:  append([]db.GroupPcRow{}, mc.modelLst[idx].groupLst.GroupPc...)}

	for k := range mc.modelLst[idx].groupLst.GroupLst {
		gm.GroupLst[k].Group = mc.modelLst[idx].groupLst.GroupLst[k]
	}

	// set language-specific rows by matched language or by default language or by zero index language

	// set group description and notes
	if len(gm.GroupLst) > 0 && len(mc.modelLst[idx].groupLst.GroupTxt) > 0 {

		var isKey, isFound, isMatch bool
		var nf, ni, si, di int

		for ; si < len(mc.modelLst[idx].groupLst.GroupTxt); si++ {

			// destination rows must be defined by [di] index
			if di >= len(gm.GroupLst) {
				break // done with all destination text
			}

			// check if source and destination keys equal
			mId := gm.GroupLst[di].Group.ModelId
			gId := gm.GroupLst[di].Group.GroupId

			isKey = mc.modelLst[idx].groupLst.GroupTxt[si].ModelId == mId &&
				mc.modelLst[idx].groupLst.GroupTxt[si].GroupId == gId

			// start of next key: set value
			if !isKey && isFound {

				if !isMatch { // if no match then use default
					ni = nf
				}
				gm.GroupLst[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].groupLst.GroupTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].groupLst.GroupTxt[ni].Descr,
					Note:     mc.modelLst[idx].groupLst.GroupTxt[ni].Note}

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
				isMatch = mc.modelLst[idx].groupLst.GroupTxt[si].LangCode == lc
				if isMatch {
					ni = si // perefred language match
				}
				if mc.modelLst[idx].groupLst.GroupTxt[si].LangCode == lcd {
					nf = si // index of default language
				}
			}

			// if keys not equal and destination key behind source
			// then move to next destination row and repeat current source row
			if !isKey &&
				(mc.modelLst[idx].groupLst.GroupTxt[si].ModelId > mId ||
					mc.modelLst[idx].groupLst.GroupTxt[si].ModelId == mId &&
						mc.modelLst[idx].groupLst.GroupTxt[si].GroupId > gId) {

				di++ // move to next type
				si-- // repeat current source row
				continue
			}
		} // for

		// last row
		if isFound && di < len(gm.GroupLst) {

			if !isMatch { // if no match then use default
				ni = nf
			}
			if ni < len(mc.modelLst[idx].groupLst.GroupTxt) {
				gm.GroupLst[di].DescrNote = db.DescrNote{
					LangCode: mc.modelLst[idx].groupLst.GroupTxt[ni].LangCode,
					Descr:    mc.modelLst[idx].groupLst.GroupTxt[ni].Descr,
					Note:     mc.modelLst[idx].groupLst.GroupTxt[ni].Note}
			}
		}
	}

	return &gm, true
}
