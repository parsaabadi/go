// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import "database/sql"

// UpdateProfile insert new, update existing or delete existing profile in profile_lst and profile_option tables.
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
