// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"database/sql"
	"hash"
)

// UpdateProfile insert new, update existing or delete existing profile in profile_lst and profile_option tables.
//
// It always delete from profile_lst and profile_option rows where profile_name = profile.Name
// If profile.Opts is not empty then new rows inserted into profile_lst and profile_option.
func UpdateProfile(dbConn *sql.DB, profile *ProfileMeta) error {

	// source is empty: nothing to do, exit
	if profile == nil {
		return nil
	}

	// do update in transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err = doUpdateProfile(trx, profile); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// doUpdateProfile insert new, update existing or delete existing profile in profile_lst and profile_option tables.
// It does update as part of transaction
func doUpdateProfile(trx *sql.Tx, profile *ProfileMeta) error {

	// delete existing profile
	qn := toQuoted(profile.Name)

	err := TrxUpdate(trx, "DELETE FROM profile_option WHERE profile_name = "+qn)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx, "DELETE FROM profile_lst WHERE profile_name = "+qn)
	if err != nil {
		return err
	}

	// if options not empty then insert new profile
	if len(profile.Opts) > 0 {

		// insert profile name
		err = TrxUpdate(trx, "INSERT INTO profile_lst (profile_name) VALUES ("+qn+")")
		if err != nil {
			return err
		}

		// insert profile options
		for key, val := range profile.Opts {

			err = TrxUpdate(trx, "INSERT INTO profile_option (profile_name, option_key, option_value)"+
				" VALUES ("+qn+", "+toQuoted(key)+", "+toQuoted(val)+")")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// digestCells add hash of cells (parameter values, accumulators or expressions) to hash.
// It is a hash of text values identical to csv file hash, for example:
//   acc_id,sub_id,dim0,dim1,acc_value\n
//   0,1,0,0,1234.5678\n
// Double format is used for float model types digest calculation, if non-empty format supplied
func digestCells(hSum hash.Hash, modelDef *ModelMeta, name string, cell CsvConverter, cellLst *list.List, doubleFmt string) error {

	// append header, like: acc_id,sub_id,dim0,dim1,acc_value\n
	cs, err := cell.CsvHeader(modelDef, name, true, "")
	if err != nil {
		return err
	}
	for k := range cs {
		if k != 0 {
			if _, err = hSum.Write([]byte(",")); err != nil {
				return err
			}
		}
		if _, err = hSum.Write([]byte(cs[k])); err != nil {
			return err
		}
	}
	if _, err = hSum.Write([]byte("\n")); err != nil {
		return err
	}

	// append dimensions and value to digest
	cvt, err := cell.CsvToIdRow(modelDef, name, doubleFmt, "") // converter from cell id's to csv row []string
	if err != nil {
		return err
	}
	for c := cellLst.Front(); c != nil; c = c.Next() {

		// convert to strings
		if err := cvt(c.Value, cs); err != nil {
			return err
		}

		// append to digest
		for k := range cs {
			if k != 0 {
				if _, err = hSum.Write([]byte(",")); err != nil {
					return err
				}
			}
			if _, err = hSum.Write([]byte(cs[k])); err != nil {
				return err
			}
		}
		if _, err = hSum.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}
