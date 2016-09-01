// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"strconv"
)

// Setup update model metadata internal members, it must be called after restoring from json.
func (meta *ModelMeta) Setup() error {

	// update parameter type and size: row count for all dimensions
	for idx := range meta.Param {

		k, ok := meta.TypeByKey(meta.Param[idx].TypeId)
		if !ok {
			return errors.New("type " + strconv.Itoa(meta.Param[idx].TypeId) + " not found for " + meta.Param[idx].Name)
		}
		meta.Param[idx].typeOf = &meta.Type[k]

		if meta.Param[idx].Rank != len(meta.Param[idx].Dim) {
			return errors.New("incorrect rank of parameter " + meta.Param[idx].Name)
		}

		meta.Param[idx].sizeOf = 1
		for i := range meta.Param[idx].Dim {

			j, ok := meta.TypeByKey(meta.Param[idx].Dim[i].TypeId)
			if !ok {
				return errors.New("type " + strconv.Itoa(meta.Param[idx].Dim[i].TypeId) + " not found for " + meta.Param[idx].Name)
			}
			meta.Param[idx].Dim[i].typeOf = &meta.Type[j]
			meta.Param[idx].Dim[i].sizeOf = len(meta.Param[idx].Dim[i].typeOf.Enum)

			if meta.Param[idx].Dim[i].sizeOf > 0 {
				meta.Param[idx].sizeOf *= meta.Param[idx].Dim[i].sizeOf
			}
		}
	}

	// update output table: size, dimensions type
	for idx := range meta.Table {

		if meta.Table[idx].Rank != len(meta.Table[idx].Dim) {
			return errors.New("incorrect rank of output table " + meta.Table[idx].Name)
		}

		meta.Table[idx].sizeOf = 1
		for i := range meta.Table[idx].Dim {

			j, ok := meta.TypeByKey(meta.Table[idx].Dim[i].TypeId)
			if !ok {
				return errors.New("type " + strconv.Itoa(meta.Table[idx].Dim[i].TypeId) + " not found for " + meta.Table[idx].Name)
			}
			meta.Table[idx].Dim[i].typeOf = &meta.Type[j]

			if meta.Table[idx].Dim[i].DimSize > 0 {
				meta.Table[idx].sizeOf *= meta.Table[idx].Dim[i].DimSize
			}
		}
	}

	return nil
}

// Setup update language metadata internal members, it must be called after restoring from json.
func (meta *LangList) Setup() {

	meta.idIndex = make(map[int]int, len(meta.LangWord))
	meta.codeIndex = make(map[string]int, len(meta.LangWord))

	for k := range meta.LangWord {
		meta.idIndex[meta.LangWord[k].LangId] = k     // index of lang_id in result []language slice
		meta.codeIndex[meta.LangWord[k].LangCode] = k // index of lang_code in result []language slice
	}
}
