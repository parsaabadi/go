// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// ToPublic convert model groups metadata into "public" format for json import-export.
func (meta *GroupMeta) ToPublic(dbConn *sql.DB, modelDef *ModelMeta) (*GroupLstPub, error) {

	// validate run model id: run must belong to the model
	if meta.ModelName != modelDef.Model.Name || meta.ModelDigest != modelDef.Model.Digest {
		return nil, errors.New("invalid model groups name or digest: " + meta.ModelName + " " + meta.ModelDigest + ", expected: " + modelDef.Model.Name + " " + modelDef.Model.Digest)
	}

	// groups header
	pub := GroupLstPub{
		ModelName:   modelDef.Model.Name,
		ModelDigest: modelDef.Model.Digest,
		Group:       make([]GroupPub, len(meta.GroupLst)),
		Pc:          make([]GroupPcPub, len(meta.GroupPc)),
	}

	// groups and group text (language, description, notes)
	nt := 0
	for k := range meta.GroupLst {

		pub.Group[k] = GroupPub{
			GroupId:  meta.GroupLst[k].GroupId,
			IsParam:  meta.GroupLst[k].IsParam,
			Name:     meta.GroupLst[k].Name,
			IsHidden: meta.GroupLst[k].IsHidden,
			Txt:      []DescrNote{},
		}

		for ; nt < len(meta.GroupTxt); nt++ {
			if meta.GroupTxt[nt].GroupId < meta.GroupLst[k].GroupId {
				continue // text row is before current group row
			}
			if meta.GroupTxt[nt].GroupId > meta.GroupLst[k].GroupId {
				break // done with current group row
			}
			pub.Group[k].Txt = append(pub.Group[k].Txt, meta.GroupTxt[nt].DescrNote)
		}
	}

	// groups parent-child hierarchy
	for k := range meta.GroupPc {
		pub.Pc[k] = meta.GroupPc[k].GroupPcPub
	}

	return &pub, nil
}

// GetModelGroup return db rows of model parent-child groups of parameters and output tables.
// If langCode not empty then only specified language selected else all languages.
func GetModelGroup(dbConn *sql.DB, modelId int, langCode string) (*GroupMeta, error) {

	// select model name and digest by id
	meta := GroupMeta{}
	smId := strconv.Itoa(modelId)

	err := SelectFirst(dbConn,
		"SELECT model_name, model_digest FROM model_dic WHERE model_id = "+smId,
		func(row *sql.Row) error {
			return row.Scan(&meta.ModelName, &meta.ModelDigest)
		})
	switch {
	case err == sql.ErrNoRows:
		return nil, errors.New("model not found, invalid model id: " + smId)
	case err != nil:
		return nil, err
	}

	// select db rows from group_lst
	err = SelectRows(dbConn,
		"SELECT"+
			" model_id, group_id, is_parameter, group_name, is_hidden"+
			" FROM group_lst"+
			" WHERE model_id = "+smId+
			" ORDER BY 1, 2",
		func(rows *sql.Rows) error {
			var r GroupLstRow
			nParam := 0
			nHidden := 0
			if err := rows.Scan(
				&r.ModelId, &r.GroupId, &nParam, &r.Name, &nHidden); err != nil {
				return err
			}
			r.IsParam = nParam != 0   // oracle: smallint is float64
			r.IsHidden = nHidden != 0 // oracle: smallint is float64
			meta.GroupLst = append(meta.GroupLst, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from group_pc
	err = SelectRows(dbConn,
		"SELECT"+
			" model_id, group_id, child_pos, child_group_id, leaf_id"+
			" FROM group_pc"+
			" WHERE model_id = "+smId+
			" ORDER BY 1, 2, 3",
		func(rows *sql.Rows) error {
			var r GroupPcRow
			var cgId, leafId sql.NullInt64
			if err := rows.Scan(
				&r.ModelId, &r.GroupId, &r.ChildPos, &cgId, &leafId); err != nil {
				return err
			}
			if cgId.Valid {
				r.ChildGroupId = int(cgId.Int64)
			} else {
				r.ChildGroupId = -1
			}
			if leafId.Valid {
				r.ChildLeafId = int(leafId.Int64)
			} else {
				r.ChildLeafId = -1
			}

			meta.GroupPc = append(meta.GroupPc, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// select db rows from group_txt
	q := "SELECT" +
		" T.model_id, T.group_id, T.lang_id, L.lang_code, T.descr, T.note" +
		" FROM group_txt T" +
		" INNER JOIN lang_lst L ON (L.lang_id = T.lang_id)" +
		" WHERE T.model_id = " + smId
	if langCode != "" {
		q += " AND L.lang_code = " + toQuoted(langCode)
	}
	q += " ORDER BY 1, 2, 3"

	err = SelectRows(dbConn, q,
		func(rows *sql.Rows) error {
			var r GroupTxtRow
			var lId int
			var note sql.NullString
			if err := rows.Scan(
				&r.ModelId, &r.GroupId, &lId, &r.LangCode, &r.Descr, &note); err != nil {
				return err
			}
			if note.Valid {
				r.Note = note.String
			}
			meta.GroupTxt = append(meta.GroupTxt, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return &meta, nil
}
