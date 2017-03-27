// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"crypto/md5"
	"errors"
	"fmt"
	"strconv"
)

// updateInternals language metadata internal members.
// It must be called after restoring from json.
func (meta *LangMeta) updateInternals() {

	meta.idIndex = make(map[int]int, len(meta.Lang))
	meta.codeIndex = make(map[string]int, len(meta.Lang))

	for k := range meta.Lang {
		meta.idIndex[meta.Lang[k].langId] = k     // index of lang_id in result []language slice
		meta.codeIndex[meta.Lang[k].LangCode] = k // index of lang_code in result []language slice
	}
}

// updateInternals model metadata internal members.
// It must be called after restoring from json.
// It does recalculate digest of type, parameter, output table, model if digest is "" empty.
func (meta *ModelMeta) updateInternals() error {

	hMd5 := md5.New()

	// update type digest, if it is empty
	for idx := range meta.Type {

		if meta.Type[idx].Digest != "" { // digest already defined, skip
			continue
		}

		// for built-in types use _name_ as digest, ie: _int_ or _Time_
		if meta.Type[idx].IsBuiltIn() {
			meta.Type[idx].Digest = "_" + meta.Type[idx].Name + "_"
			continue
		}
		// else: model-specific type with empty "" digest

		// digest type header
		hMd5.Reset()
		_, err := hMd5.Write([]byte("type_name,dic_id\n"))
		if err != nil {
			return err
		}
		_, err = hMd5.Write([]byte(
			meta.Type[idx].Name + "," + strconv.Itoa(meta.Type[idx].DicId) + "\n"))
		if err != nil {
			return err
		}

		// digest type enums
		_, err = hMd5.Write([]byte("enum_id,enum_name\n"))
		if err != nil {
			return err
		}
		for k := range meta.Type[idx].Enum {
			_, err := hMd5.Write([]byte(
				strconv.Itoa(meta.Type[idx].Enum[k].EnumId) + "," + meta.Type[idx].Enum[k].Name + "\n"))
			if err != nil {
				return err
			}
		}

		meta.Type[idx].Digest = fmt.Sprintf("%x", hMd5.Sum(nil)) // set type digest string
	}

	// update parameter type and size (row count for all dimensions)
	// update parameter dimensions: type and size (count of all enums)
	// update parameter digest, if it is empty
	for idx := range meta.Param {

		// update parameter type
		k, ok := meta.TypeByKey(meta.Param[idx].TypeId)
		if !ok {
			return errors.New("type " + strconv.Itoa(meta.Param[idx].TypeId) + " not found for " + meta.Param[idx].Name)
		}
		meta.Param[idx].typeOf = &meta.Type[k]

		if meta.Param[idx].Rank != len(meta.Param[idx].Dim) {
			return errors.New("incorrect rank of parameter " + meta.Param[idx].Name)
		}

		// update parameter size: row count for all dimensions
		// update parameter dimensions: type and size (count of all enums)
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

		// update parameter digest, if it is empty
		if meta.Param[idx].Digest == "" {

			// digest parameter header: name, rank, value type digest
			hMd5.Reset()
			_, err := hMd5.Write([]byte("parameter_name,parameter_rank,type_digest\n"))
			if err != nil {
				return err
			}
			_, err = hMd5.Write([]byte(
				meta.Param[idx].Name + "," + strconv.Itoa(meta.Param[idx].Rank) + "," + meta.Param[idx].typeOf.Digest + "\n"))
			if err != nil {
				return err
			}

			// digest parameter dimensions: id, name, dimension type digest
			_, err = hMd5.Write([]byte("dim_id,dim_name,type_digest\n"))
			if err != nil {
				return err
			}
			for k := range meta.Param[idx].Dim {
				_, err := hMd5.Write([]byte(
					strconv.Itoa(meta.Param[idx].Dim[k].DimId) + "," + meta.Param[idx].Dim[k].Name + "," + meta.Param[idx].Dim[k].typeOf.Digest + "\n"))
				if err != nil {
					return err
				}
			}

			meta.Param[idx].Digest = fmt.Sprintf("%x", hMd5.Sum(nil)) // set parameter digest string
		}
	}

	// update output table size (row count for all dimensions)
	// update output table dimensions: type and size (count of all enums)
	// update output table digest, if it is empty
	for idx := range meta.Table {

		if meta.Table[idx].Rank != len(meta.Table[idx].Dim) {
			return errors.New("incorrect rank of output table " + meta.Table[idx].Name)
		}

		// update output table size (row count for all dimensions)
		// update output table dimensions: type and size (count of all enums)
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

		// update output table digest, if it is empty
		if meta.Table[idx].Digest == "" {

			// digest output table header: name, rank
			hMd5.Reset()
			_, err := hMd5.Write([]byte("table_name,table_rank\n"))
			if err != nil {
				return err
			}
			_, err = hMd5.Write([]byte(
				meta.Table[idx].Name + "," + strconv.Itoa(meta.Table[idx].Rank) + "\n"))
			if err != nil {
				return err
			}

			// digest output table dimensions: id, name, dimension type digest
			_, err = hMd5.Write([]byte("dim_id,dim_name,type_digest\n"))
			if err != nil {
				return err
			}
			for k := range meta.Table[idx].Dim {
				_, err := hMd5.Write([]byte(
					strconv.Itoa(meta.Table[idx].Dim[k].DimId) + "," + meta.Table[idx].Dim[k].Name + "," + meta.Table[idx].Dim[k].typeOf.Digest + "\n"))
				if err != nil {
					return err
				}
			}

			// digest output table accumulators: id, name, expression
			_, err = hMd5.Write([]byte("acc_id,acc_name,acc_src\n"))
			if err != nil {
				return err
			}
			for k := range meta.Table[idx].Acc {
				_, err := hMd5.Write([]byte(
					strconv.Itoa(meta.Table[idx].Acc[k].AccId) + "," + meta.Table[idx].Acc[k].Name + "," + meta.Table[idx].Acc[k].SrcAcc + "\n"))
				if err != nil {
					return err
				}
			}

			// digest output table expressions: id, name, source expression
			_, err = hMd5.Write([]byte("expr_id,expr_name,expr_src\n"))
			if err != nil {
				return err
			}
			for k := range meta.Table[idx].Expr {
				_, err := hMd5.Write([]byte(
					strconv.Itoa(meta.Table[idx].Expr[k].ExprId) + "," + meta.Table[idx].Expr[k].Name + "," + meta.Table[idx].Expr[k].SrcExpr + "\n"))
				if err != nil {
					return err
				}
			}

			meta.Table[idx].Digest = fmt.Sprintf("%x", hMd5.Sum(nil)) // set output table digest string
		}
	}

	// update model digest if it is "" empty
	if meta.Model.Digest == "" {

		// digest model header: name and model type
		hMd5.Reset()
		_, err := hMd5.Write([]byte("model_name,model_type\n"))
		if err != nil {
			return err
		}
		_, err = hMd5.Write([]byte(
			meta.Model.Name + "," + strconv.Itoa(meta.Model.Type) + "\n"))
		if err != nil {
			return err
		}

		// add digests of all model types
		_, err = hMd5.Write([]byte("type_digest\n"))
		if err != nil {
			return err
		}
		for k := range meta.Type {
			_, err := hMd5.Write([]byte(meta.Type[k].Digest + "\n"))
			if err != nil {
				return err
			}
		}

		// add digests of all model parameters
		_, err = hMd5.Write([]byte("parameter_digest\n"))
		if err != nil {
			return err
		}
		for k := range meta.Param {
			_, err := hMd5.Write([]byte(meta.Param[k].Digest + "\n"))
			if err != nil {
				return err
			}
		}

		// add digests of all model output tables
		_, err = hMd5.Write([]byte("table_digest\n"))
		if err != nil {
			return err
		}
		for k := range meta.Table {
			_, err := hMd5.Write([]byte(meta.Table[k].Digest + "\n"))
			if err != nil {
				return err
			}
		}

		meta.Model.Digest = fmt.Sprintf("%x", hMd5.Sum(nil)) // set model digest string
	}

	return nil
}
