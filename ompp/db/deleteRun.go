// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// DeleteRun delete model run metadata, parameters run values and output tables run values from database.
func DeleteRun(dbConn *sql.DB, runId int) error {

	// validate parameters
	if runId <= 0 {
		return errors.New("invalid run id: " + strconv.Itoa(runId))
	}

	// delete inside of transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err := doDeleteRun(trx, runId); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// doDeleteRun delete model run metadata, parameters run values and output tables run values from database.
// It does update as part of transaction
func doDeleteRun(trx *sql.Tx, runId int) error {

	// update model run master record to prevent run use
	sId := strconv.Itoa(runId)
	err := TrxUpdate(trx, "UPDATE run_lst SET run_name = 'deleted' WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	// update to NULL base run id for all worksets where base run id = target run id
	err = TrxUpdate(trx, "UPDATE workset_lst SET base_run_id = NULL WHERE base_run_id = "+sId)
	if err != nil {
		return err
	}

	// delete model runs:
	// for all model parameters where parameter run value shared between runs
	// build list of new base run id's
	rbArr, err := selectBaseRunsOfSharedValues(trx,
		"SELECT"+
			" RP.parameter_hid, RP.run_id, RP.base_run_id,"+
			" ("+
			" SELECT MIN(NR.run_id)"+
			" FROM run_parameter NR"+
			" WHERE NR.parameter_hid = RP.parameter_hid"+
			" AND NR.base_run_id = RP.base_run_id"+
			" AND NR.run_id <> NR.base_run_id"+
			" )"+
			" FROM run_parameter RP"+
			" WHERE RP.run_id <> RP.base_run_id"+
			" AND RP.base_run_id = "+sId+
			" ORDER BY 1, 3")
	if err != nil {
		return err
	}

	// delete model runs:
	// where parameter run value shared between runs
	// re-base run values: update run id for parameter values with new base run id
	tblName := ""
	hId := 0
	oldId := 0
	for k := range rbArr {

		// find db table name for parameter run value
		// if not same parameter as before
		if hId == 0 || hId != rbArr[k].hId {

			err = TrxSelectFirst(trx,
				"SELECT db_run_table FROM parameter_dic WHERE parameter_hid = "+strconv.Itoa(rbArr[k].hId),
				func(row *sql.Row) error {
					if err := row.Scan(&tblName); err != nil {
						return err
					}
					return nil
				})
			if err != nil {
				return err
			}
		}

		// re-base run values in parameter value table and in run_parameter list
		// if not same parameter and base run id as before
		if hId == 0 || oldId == 0 || hId != rbArr[k].hId || oldId != rbArr[k].oldBase {

			hId = rbArr[k].hId
			oldId = rbArr[k].oldBase

			// update parameter value run id with new base run id
			err = TrxUpdate(trx,
				"UPDATE "+tblName+
					" SET run_id = "+strconv.Itoa(rbArr[k].newBase)+
					" WHERE run_id = "+strconv.Itoa(rbArr[k].oldBase))
			if err != nil {
				return err
			}

			// set new base run id in run_paramter table
			err = TrxUpdate(trx,
				"UPDATE run_parameter SET base_run_id = "+strconv.Itoa(rbArr[k].newBase)+
					" WHERE base_run_id = "+strconv.Itoa(rbArr[k].oldBase)+
					" AND parameter_hid = "+strconv.Itoa(rbArr[k].hId))
			if err != nil {
				return err
			}
		}
	}

	// delete model runs:
	// for all model output tables where output table value shared between runs
	// build list of new base run id's
	rbArr, err = selectBaseRunsOfSharedValues(trx,
		"SELECT"+
			" RT.table_hid, RT.run_id, RT.base_run_id,"+
			" ("+
			" SELECT MIN(NR.run_id)"+
			" FROM run_table NR"+
			" WHERE NR.table_hid = RT.table_hid"+
			" AND NR.base_run_id = RT.base_run_id"+
			" AND NR.run_id <> NR.base_run_id"+
			" )"+
			" FROM run_table RT"+
			" WHERE RT.run_id <> RT.base_run_id"+
			" AND RT.base_run_id = "+sId+
			" ORDER BY 1, 3")
	if err != nil {
		return err
	}

	// delete model runs:
	// where output table shared between models and output value shared between runs
	// re-base run values: update run id for accumulators and expressions with new base run id
	eTbl := ""
	aTbl := ""
	hId = 0
	oldId = 0
	for k := range rbArr {

		// find db table names for accumulators and expressions run value
		// if not same output table as before
		if hId == 0 || hId != rbArr[k].hId {

			err = TrxSelectFirst(trx,
				"SELECT db_expr_table, db_acc_table FROM table_dic WHERE table_hid = "+strconv.Itoa(rbArr[k].hId),
				func(row *sql.Row) error {
					if err := row.Scan(&eTbl, &aTbl); err != nil {
						return err
					}
					return nil
				})
			if err != nil {
				return err
			}
		}

		// re-base run values:
		// update run id in accumulators and expression tables with new base run id
		// update output value base run id in run_table list
		// if not same output table and base run id as before
		if hId == 0 || oldId == 0 || hId != rbArr[k].hId || oldId != rbArr[k].oldBase {

			hId = rbArr[k].hId
			oldId = rbArr[k].oldBase

			// update accumulators and expressions value run id with new base run id
			err = TrxUpdate(trx,
				"UPDATE "+eTbl+
					" SET run_id = "+strconv.Itoa(rbArr[k].newBase)+
					" WHERE run_id = "+strconv.Itoa(rbArr[k].oldBase))
			if err != nil {
				return err
			}
			err = TrxUpdate(trx,
				"UPDATE "+aTbl+
					" SET run_id = "+strconv.Itoa(rbArr[k].newBase)+
					" WHERE run_id = "+strconv.Itoa(rbArr[k].oldBase))
			if err != nil {
				return err
			}

			// set new base run id in run_table
			err = TrxUpdate(trx,
				"UPDATE run_table SET base_run_id = "+strconv.Itoa(rbArr[k].newBase)+
					" WHERE base_run_id = "+strconv.Itoa(rbArr[k].oldBase)+
					" AND table_hid = "+strconv.Itoa(rbArr[k].hId))
			if err != nil {
				return err
			}
		}
	}

	// delete model runs:
	// delete run metadata
	err = TrxUpdate(trx, "DELETE FROM run_table WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM run_parameter_txt WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM run_parameter WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM run_option WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM run_txt WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM run_lst WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	return nil
}
