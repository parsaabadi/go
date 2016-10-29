// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// DeleteWorkset delete workset metadata and workset parameter values from database.
func DeleteWorkset(dbConn *sql.DB, setId int) error {

	// validate parameters
	if setId <= 0 {
		return errors.New("invalid workset id: " + strconv.Itoa(setId))
	}

	// delete inside of transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err := dbDeleteWorkset(trx, setId); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// doDeleteWorkset delete workset metadata and workset parameter values from database.
// It does update as part of transaction
func dbDeleteWorkset(trx *sql.Tx, setId int) error {

	// update workset master record to prevent workset use
	sId := strconv.Itoa(setId)
	err := TrxUpdate(trx, "UPDATE workset_lst SET set_name = 'deleted' WHERE set_id = "+sId)
	if err != nil {
		return err
	}

	// build a list of workset parameters db-tables
	var tblArr []string
	err = TrxSelectRows(trx,
		"SELECT P.db_set_table"+
			" FROM workset_parameter W"+
			" INNER JOIN parameter_dic P ON (P.parameter_hid = W.parameter_hid)"+
			" WHERE W.set_id = "+sId,
		func(rows *sql.Rows) error {
			tn := ""
			if err := rows.Scan(&tn); err != nil {
				return err
			}
			tblArr = append(tblArr, tn)
			return nil
		})
	if err != nil {
		return err
	}

	// delete workset parameter values
	for k := range tblArr {
		err = TrxUpdate(trx, "DELETE FROM "+tblArr[k]+" WHERE set_id = "+sId)
		if err != nil {
			return err
		}
	}

	// delete model workset metadata
	err = TrxUpdate(trx, "DELETE FROM workset_parameter_txt WHERE set_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM workset_parameter WHERE set_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM workset_txt WHERE set_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM workset_lst WHERE set_id = "+sId)
	if err != nil {
		return err
	}

	return nil
}
