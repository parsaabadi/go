// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/openmpp/go/ompp/helper"
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
	if err = doUnlinkRun(trx, runId); err != nil {
		trx.Rollback()
		return err
	}
	if err = doDeleteRunBody(trx, runId); err != nil {
		trx.Rollback()
		return err
	}
	if err = doDeleteRunMeta(trx, runId); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// doUnlinkRun set run status to d=deleted and unlink run values (parameter, output tables, microdata) from base run:
// if run values used by any other run as a base run then base run id updated to the next minimal run id.
// It does update as part of transaction
func doUnlinkRun(trx *sql.Tx, runId int) error {

	// update model run master record to prevent run use
	sId := strconv.Itoa(runId)
	delTs := helper.MakeTimeStamp(time.Now())

	err := TrxUpdate(trx, "UPDATE run_lst SET run_name = "+ToQuoted("deleted: "+delTs)+" WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	status := ""
	err = TrxSelectFirst(trx,
		"SELECT status FROM run_lst WHERE run_id = "+sId,
		func(row *sql.Row) error {
			if err := row.Scan(&status); err != nil {
				return err
			}
			return nil
		})
	if err != nil {
		return err
	}
	if status == "d" { // run unlink already done
		return nil
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
	// for all entity generations where microdata run value shared between runs
	// build list of new base run id's
	rbArr, err = selectBaseRunsOfSharedValues(trx,
		"SELECT"+
			" RE.entity_gen_hid, RE.run_id, RE.base_run_id,"+
			" ("+
			" SELECT MIN(NR.run_id)"+
			" FROM run_entity NR"+
			" WHERE NR.entity_gen_hid = RE.entity_gen_hid"+
			" AND NR.base_run_id = RE.base_run_id"+
			" AND NR.run_id <> NR.base_run_id"+
			" )"+
			" FROM run_entity RE"+
			" WHERE RE.run_id <> RE.base_run_id"+
			" AND RE.base_run_id = "+sId+
			" ORDER BY 1, 3")
	if err != nil {
		return err
	}

	// delete model runs:
	// where microdata run value shared between runs
	// re-base run values: update run id for microdata values with new base run id
	tblName = ""
	hId = 0
	oldId = 0
	for k := range rbArr {

		// find db table name for microdata run value
		// if not same microdata as before
		if hId == 0 || hId != rbArr[k].hId {

			err = TrxSelectFirst(trx,
				"SELECT db_entity_table FROM entity_gen WHERE entity_gen_hid = "+strconv.Itoa(rbArr[k].hId),
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

		// re-base run values in microdata value table and in run_entity list
		// if not same microdata and base run id as before
		if hId == 0 || oldId == 0 || hId != rbArr[k].hId || oldId != rbArr[k].oldBase {

			hId = rbArr[k].hId
			oldId = rbArr[k].oldBase

			// update microdata value run id with new base run id
			err = TrxUpdate(trx,
				"UPDATE "+tblName+
					" SET run_id = "+strconv.Itoa(rbArr[k].newBase)+
					" WHERE run_id = "+strconv.Itoa(rbArr[k].oldBase))
			if err != nil {
				return err
			}

			// set new base run id in run_entity table
			err = TrxUpdate(trx,
				"UPDATE run_entity SET base_run_id = "+strconv.Itoa(rbArr[k].newBase)+
					" WHERE base_run_id = "+strconv.Itoa(rbArr[k].oldBase)+
					" AND entity_gen_hid = "+strconv.Itoa(rbArr[k].hId))
			if err != nil {
				return err
			}
		}
	}

	// replace value digests for parameter, output tables and microdata to avoid using values as a base run values
	delDgst := ("del-" + sId + "-" + delTs)

	err = TrxUpdate(trx,
		"UPDATE run_parameter SET value_digest = "+toQuotedMax(delDgst, codeDbMax)+" WHERE run_id = "+sId)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx,
		"UPDATE run_table SET value_digest = "+toQuotedMax(delDgst, codeDbMax)+" WHERE run_id = "+sId)
	if err != nil {
		return err
	}
	err = TrxUpdate(trx,
		"UPDATE run_entity SET value_digest = "+toQuotedMax(delDgst, codeDbMax)+" WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	// set run status d=deleted and replace run digest
	err = TrxUpdate(trx,
		"UPDATE run_lst SET status = 'd', run_digest = "+toQuotedMax(delDgst, codeDbMax)+
			" WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	return nil
}

// doDeleteRunMeta delete model run metadata from database.
// Run status must be d=deleted and there are no rows in run_parameter, run_table and run_entity for that run id.
// It does update as part of transaction
func doDeleteRunMeta(trx *sql.Tx, runId int) error {

	// check run status, it must d=deleted
	sId := strconv.Itoa(runId)
	status := ""
	err := TrxSelectFirst(trx,
		"SELECT status FROM run_lst WHERE run_id = "+sId,
		func(row *sql.Row) error {
			if err := row.Scan(&status); err != nil {
				return err
			}
			return nil
		})
	if err != nil {
		return err
	}
	if status != "d" {
		return errors.New("invalid run status, it must be 'd': " + status + " run id: " + strconv.Itoa(runId))
	}

	// validate: should be no rows in run_parameter, run_table, run_entity for that run id
	var n int = 0

	err = TrxSelectFirst(trx,
		"SELECT COUNT(*) FROM run_parameter WHERE run_id = "+sId,
		func(row *sql.Row) error {
			if err := row.Scan(&n); err != nil {
				return err
			}
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if n != 0 {
		return errors.New("invalid run: it should zero parameters before delete: " + strconv.Itoa(n) + " run id: " + strconv.Itoa(runId))
	}

	err = TrxSelectFirst(trx,
		"SELECT COUNT(*) FROM run_table WHERE run_id = "+sId,
		func(row *sql.Row) error {
			if err := row.Scan(&n); err != nil {
				return err
			}
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if n != 0 {
		return errors.New("invalid run: it should zero output tables before delete: " + strconv.Itoa(n) + " run id: " + strconv.Itoa(runId))
	}

	err = TrxSelectFirst(trx,
		"SELECT COUNT(*) FROM run_entity WHERE run_id = "+sId,
		func(row *sql.Row) error {
			if err := row.Scan(&n); err != nil {
				return err
			}
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if n != 0 {
		return errors.New("invalid run: it should zero microdata values before delete: " + strconv.Itoa(n) + " run id: " + strconv.Itoa(runId))
	}

	// delete run metadata
	err = TrxUpdate(trx, "DELETE FROM run_progress WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM run_entity WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM run_table WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM run_parameter_import WHERE run_id = "+sId)
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

// doDeleteRunBody delete model run values from database: do delete parameters, output tables and microdata values.
// It does update as part of transaction
func doDeleteRunBody(trx *sql.Tx, runId int) error {

	// build a list of run parameter values db-tables
	sId := strconv.Itoa(runId)

	var pTbls []string
	err := TrxSelectRows(trx,
		"SELECT P.db_run_table"+
			" FROM run_parameter RP"+
			" INNER JOIN parameter_dic P ON (P.parameter_hid = RP.parameter_hid)"+
			" WHERE RP.run_id = "+sId,
		func(rows *sql.Rows) error {
			tn := ""
			if err := rows.Scan(&tn); err != nil {
				return err
			}
			pTbls = append(pTbls, tn)
			return nil
		})
	if err != nil {
		return err
	}

	// build a list of run expression and accumulator values db-tables
	var eTbls []string
	var aTbls []string
	err = TrxSelectRows(trx,
		"SELECT T.db_expr_table, T.db_acc_table"+
			" FROM run_table RT"+
			" INNER JOIN table_dic T ON (T.table_hid = RT.table_hid)"+
			" WHERE RT.run_id = "+sId,
		func(rows *sql.Rows) error {
			etn := ""
			atn := ""
			if err := rows.Scan(&etn, &atn); err != nil {
				return err
			}
			eTbls = append(eTbls, etn)
			aTbls = append(aTbls, atn)
			return nil
		})
	if err != nil {
		return err
	}

	// build a list of run microdata values db-tables
	var mTbls []string
	err = TrxSelectRows(trx,
		"SELECT EG.db_entity_table"+
			" FROM entity_gen EG"+
			" INNER JOIN run_entity RE ON (RE.entity_gen_hid = EG.entity_gen_hid)"+
			" WHERE RE.run_id = "+sId,
		func(rows *sql.Rows) error {
			tn := ""
			if err := rows.Scan(&tn); err != nil {
				return err
			}
			mTbls = append(mTbls, tn)
			return nil
		})
	if err != nil {
		return err
	}

	// delete rows from run microdata value tables
	// or drop microdata value tables where no run_entity exists
	for k := range mTbls {

		// delete entity generation and attributes where no run_entity exists
		dTbl := "--" + string([]rune(mTbls[k])[2:])

		err = TrxUpdate(trx,
			"UPDATE entity_gen SET db_entity_table = "+ToQuoted(dTbl)+
				" WHERE db_entity_table = "+ToQuoted(mTbls[k])+
				" AND NOT EXISTS (SELECT * FROM run_entity RE WHERE RE.entity_gen_hid = entity_gen.entity_gen_hid)",
		)
		if err != nil {
			return err
		}

		var n int = 0
		err = TrxSelectFirst(trx,
			"SELECT COUNT(*) FROM entity_gen WHERE db_entity_table = "+ToQuoted(dTbl),
			func(row *sql.Row) error {
				if err := row.Scan(&n); err != nil {
					return err
				}
				return nil
			})
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		err = TrxUpdate(trx,
			"UPDATE entity_gen SET db_entity_table = "+ToQuoted(mTbls[k])+
				" WHERE db_entity_table = "+ToQuoted(dTbl)+
				" AND NOT EXISTS (SELECT * FROM run_entity RE WHERE RE.entity_gen_hid = entity_gen.entity_gen_hid)",
		)
		if err != nil {
			return err
		}

		// delete rows from microdata table
		// or drop microdata table if there are no run_entity rows exist for that table
		if n <= 0 {

			err = TrxUpdate(trx, "DELETE FROM "+mTbls[k]+" WHERE run_id = "+sId)
			if err != nil {
				return err
			}

		} else { // no run_entity rows exist for that db table: drop db table

			err = TrxUpdate(trx,
				"DELETE FROM entity_gen_attr"+
					" WHERE EXISTS"+
					" ("+
					" SELECT * FROM entity_gen EG"+
					" WHERE EG.entity_gen_hid = entity_gen_attr.entity_gen_hid"+
					" AND EG.db_entity_table = "+ToQuoted(mTbls[k])+
					" )"+
					" AND NOT EXISTS (SELECT * FROM run_entity RE WHERE RE.entity_gen_hid = entity_gen_attr.entity_gen_hid)",
			)
			if err != nil {
				return err
			}
			err = TrxUpdate(trx,
				"DELETE FROM entity_gen"+
					" WHERE db_entity_table = "+ToQuoted(mTbls[k])+
					" AND NOT EXISTS (SELECT * FROM run_entity RE WHERE RE.entity_gen_hid = entity_gen.entity_gen_hid)",
			)
			if err != nil {
				return err
			}

			err = TrxUpdate(trx, "DROP TABLE "+mTbls[k])
			if err != nil {
				return err
			}
		}

		// delete microdata run row: there is no microdata values of that entity exist in that model run
		err = TrxUpdate(trx,
			"DELETE FROM run_entity"+
				" WHERE run_id = "+sId+
				" AND EXISTS"+
				" ("+
				" SELECT * FROM entity_gen EG"+
				" WHERE EG.entity_gen_hid = run_entity.entity_gen_hid"+
				" AND EG.db_entity_table = "+ToQuoted(mTbls[k])+
				" )",
		)
		if err != nil {
			return err
		}
	}

	// delete rows from run expression value and accumulator value tables
	for k := range eTbls {
		err = TrxUpdate(trx, "DELETE FROM "+eTbls[k]+" WHERE run_id = "+sId)
		if err != nil {
			return err
		}
	}
	for k := range aTbls {
		err = TrxUpdate(trx, "DELETE FROM "+aTbls[k]+" WHERE run_id = "+sId)
		if err != nil {
			return err
		}
	}
	// delete run output tables rows: there is no output values exist in that model run
	err = TrxUpdate(trx,
		"DELETE FROM run_table WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	// delete rows from run parameters value tables
	for k := range pTbls {
		err = TrxUpdate(trx, "DELETE FROM "+pTbls[k]+" WHERE run_id = "+sId)
		if err != nil {
			return err
		}
	}
	// delete run parameters rows: there is no parameters values exist in that model run
	err = TrxUpdate(trx,
		"DELETE FROM run_parameter WHERE run_id = "+sId)
	if err != nil {
		return err
	}

	return nil
}
