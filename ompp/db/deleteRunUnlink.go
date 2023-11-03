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

// UnlinkRun delete model run metadata, parameters run values and output tables run values from database.
// It is incremental delete using multiple small transactions and can be restarted if breaks in the middle.
func UnlinkRun(dbConn *sql.DB, modelId int, runId int) error {

	// validate parameters
	if runId <= 0 {
		return errors.New("invalid run id: " + strconv.Itoa(runId))
	}

	// start run delete inside of transaction scope:
	// update base run mark run metadata as delete-in-progress
	if err := doUnlinkRunSmallTrx(dbConn, modelId, runId); err != nil {
		return err
	}

	// delete run body using small tarnsactions:
	// delete values from microdata, output tables and parameters tables
	if err := doDeleteRunBodySmallTrx(dbConn, runId); err != nil {
		return err
	}

	// delete run metadata inside of transaction scope
	trx, e := dbConn.Begin()
	if e != nil {
		return e
	}
	if e = doDeleteRunMeta(trx, runId); e != nil {
		trx.Rollback()
		return e
	}
	trx.Commit()

	return nil
}

// doUnlinkRun set run status to d=deleted and unlink run values (parameter, output tables, microdata) from base run:
// if run values used by any other run as a base run then base run id updated to the next minimal run id.
// It does update as part of transaction
func doUnlinkRunSmallTrx(dbConn *sql.DB, modelId int, runId int) error {

	// update model run master record to prevent run use
	sId := strconv.Itoa(runId)
	delTs := helper.MakeTimeStamp(time.Now())

	tx, err := dbConn.Begin()
	if err != nil {
		return err
	}

	err = TrxUpdate(tx, "UPDATE run_lst SET run_name = "+ToQuoted("deleted: "+delTs)+" WHERE run_id = "+sId)
	if err != nil {
		tx.Rollback()
		return err
	}

	status := ""
	err = TrxSelectFirst(tx,
		"SELECT status FROM run_lst WHERE run_id = "+sId,
		func(row *sql.Row) error {
			if err := row.Scan(&status); err != nil {
				return err
			}
			return nil
		})
	if err != nil {
		tx.Rollback()
		return err
	}
	// exit and return success if run unlink already done
	if status == "d" {
		tx.Commit()
		return nil
	}

	// update to NULL base run id for all worksets where base run id = target run id
	err = TrxUpdate(tx, "UPDATE workset_lst SET base_run_id = NULL WHERE base_run_id = "+sId)
	if err != nil {
		tx.Rollback()
		return err
	}

	// replace value digests for parameter, output tables and microdata to avoid using values as a base run values
	delDgst := ("del-" + sId + "-" + delTs)

	err = TrxUpdate(tx,
		"UPDATE run_parameter SET value_digest = "+toQuotedMax(delDgst, codeDbMax)+" WHERE run_id = "+sId)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = TrxUpdate(tx,
		"UPDATE run_table SET value_digest = "+toQuotedMax(delDgst, codeDbMax)+" WHERE run_id = "+sId)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = TrxUpdate(tx,
		"UPDATE run_entity SET value_digest = "+toQuotedMax(delDgst, codeDbMax)+" WHERE run_id = "+sId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	// select list of parameters from parameter_dic join to model_parameter_dic
	type pTbl struct {
		hId    int    // parameter Hid
		name   string // parameter name
		dbName string // database table name
	}
	pArr := []pTbl{}
	err = SelectRows(dbConn,
		"SELECT"+
			" D.parameter_hid, D.parameter_name, D.db_run_table"+
			" FROM parameter_dic D"+
			" INNER JOIN model_parameter_dic M ON (M.parameter_hid = D.parameter_hid)"+
			" WHERE M.model_id = "+strconv.Itoa(modelId)+
			" ORDER BY 1",
		func(rows *sql.Rows) error {
			var r pTbl
			if err := rows.Scan(&r.hId, &r.name, &r.dbName); err != nil {
				return err
			}
			pArr = append(pArr, r)
			return nil
		})
	if err != nil {
		return err
	}

	// delete model runs:
	// where parameter run value shared between runs
	// re-base run values: update run id for parameter values with new base run id
	for _, p := range pArr {

		trx, err := dbConn.Begin()
		if err != nil {
			return err
		}

		// for all model parameters where parameter run value shared between runs
		// build list of new base run id's
		rbArr, err := selectBaseRunsOfSharedValues(trx,
			"SELECT"+
				" RP.parameter_hid, RP.run_id, RP.base_run_id,"+
				" ("+
				" SELECT MIN(NR.run_id)"+
				" FROM run_parameter NR"+
				" INNER JOIN run_lst RL ON (RL.run_id = NR.run_id)"+
				" WHERE NR.parameter_hid = RP.parameter_hid"+
				" AND NR.base_run_id = RP.base_run_id"+
				" AND NR.run_id <> NR.base_run_id"+
				" AND RL.status = "+ToQuoted(DoneRunStatus)+
				" )"+
				" FROM run_parameter RP"+
				" WHERE RP.parameter_hid = "+strconv.Itoa(p.hId)+
				" AND RP.run_id <> RP.base_run_id"+
				" AND RP.base_run_id = "+sId+
				" ORDER BY 1, 3")
		if err != nil {
			return err
		}

		// re-base run values in parameter value table and in run_parameter list
		for _, rb := range rbArr {

			err = TrxUpdate(trx,
				"UPDATE "+p.dbName+
					" SET run_id = "+strconv.Itoa(rb.newBase)+
					" WHERE run_id = "+strconv.Itoa(rb.oldBase))
			if err != nil {
				trx.Rollback()
				return err
			}

			// set new base run id in run_paramter table
			err = TrxUpdate(trx,
				"UPDATE run_parameter SET base_run_id = "+strconv.Itoa(rb.newBase)+
					" WHERE base_run_id = "+strconv.Itoa(rb.oldBase)+
					" AND parameter_hid = "+strconv.Itoa(rb.hId))
			if err != nil {
				trx.Rollback()
				return err
			}
		}

		trx.Commit()
	}

	// select list of output tables from parameter_dic join to model_parameter_dic
	type tTbl struct {
		hId     int    // output table Hid
		name    string // output table name
		eDbName string // expressions database table name
		aDbName string // accumulators database table name
	}
	tArr := []tTbl{}
	err = SelectRows(dbConn,
		"SELECT"+
			" D.table_hid, D.table_name, D.db_expr_table, D.db_acc_table"+
			" FROM table_dic D"+
			" INNER JOIN model_table_dic M ON (M.table_hid = D.table_hid)"+
			" WHERE M.model_id = "+strconv.Itoa(modelId)+
			" ORDER BY 1",
		func(rows *sql.Rows) error {
			var r tTbl
			if err := rows.Scan(&r.hId, &r.name, &r.eDbName, &r.aDbName); err != nil {
				return err
			}
			tArr = append(tArr, r)
			return nil
		})
	if err != nil {
		return err
	}

	// delete model runs:
	// where output table shared between models and output value shared between runs
	// re-base run values: update run id for accumulators and expressions with new base run id
	for _, t := range tArr {

		trx, err := dbConn.Begin()
		if err != nil {
			return err
		}

		// for all model output tables where output table value shared between runs
		// build list of new base run id's
		rbArr, err := selectBaseRunsOfSharedValues(trx,
			"SELECT"+
				" RT.table_hid, RT.run_id, RT.base_run_id,"+
				" ("+
				" SELECT MIN(NR.run_id)"+
				" FROM run_table NR"+
				" INNER JOIN run_lst RL ON (RL.run_id = NR.run_id)"+
				" WHERE NR.table_hid = RT.table_hid"+
				" AND NR.base_run_id = RT.base_run_id"+
				" AND NR.run_id <> NR.base_run_id"+
				" AND RL.status = "+ToQuoted(DoneRunStatus)+
				" )"+
				" FROM run_table RT"+
				" WHERE RT.table_hid = "+strconv.Itoa(t.hId)+
				" AND RT.run_id <> RT.base_run_id"+
				" AND RT.base_run_id = "+sId+
				" ORDER BY 1, 3")
		if err != nil {
			return err
		}

		// re-base run values:
		// update run id in accumulators and expression tables with new base run id
		// update output value base run id in run_table list
		for _, rb := range rbArr {

			// update accumulators and expressions value run id with new base run id
			err = TrxUpdate(trx,
				"UPDATE "+t.eDbName+
					" SET run_id = "+strconv.Itoa(rb.newBase)+
					" WHERE run_id = "+strconv.Itoa(rb.oldBase))
			if err != nil {
				trx.Rollback()
				return err
			}
			err = TrxUpdate(trx,
				"UPDATE "+t.aDbName+
					" SET run_id = "+strconv.Itoa(rb.newBase)+
					" WHERE run_id = "+strconv.Itoa(rb.oldBase))
			if err != nil {
				trx.Rollback()
				return err
			}

			// set new base run id in run_table
			err = TrxUpdate(trx,
				"UPDATE run_table SET base_run_id = "+strconv.Itoa(rb.newBase)+
					" WHERE base_run_id = "+strconv.Itoa(rb.oldBase)+
					" AND table_hid = "+strconv.Itoa(rb.hId))
			if err != nil {
				trx.Rollback()
				return err
			}
		}

		trx.Commit()
	}

	// select list of entity generations for that run
	type gTbl struct {
		hId    int    // parameter Hid
		dbName string // database table name
	}
	gArr := []gTbl{}
	err = SelectRows(dbConn,
		"SELECT"+
			" RE.entity_gen_hid, EG.db_entity_table"+
			" FROM run_entity RE"+
			" INNER JOIN entity_gen EG ON (EG.entity_gen_hid = RE.entity_gen_hid)"+
			" WHERE RE.run_id = "+sId+
			" ORDER BY 1",
		func(rows *sql.Rows) error {
			var r gTbl
			if err := rows.Scan(&r.hId, &r.dbName); err != nil {
				return err
			}
			gArr = append(gArr, r)
			return nil
		})
	if err != nil {
		return err
	}

	// delete model runs:
	// where microdata run value shared between runs
	// re-base run values: update run id for microdata values with new base run id
	for _, g := range gArr {

		trx, err := dbConn.Begin()
		if err != nil {
			return err
		}

		// for all entity generations where microdata run value shared between runs
		// build list of new base run id's
		rbArr, err := selectBaseRunsOfSharedValues(trx,
			"SELECT"+
				" RE.entity_gen_hid, RE.run_id, RE.base_run_id,"+
				" ("+
				" SELECT MIN(NR.run_id)"+
				" FROM run_entity NR"+
				" INNER JOIN run_lst RL ON (RL.run_id = NR.run_id)"+
				" WHERE NR.entity_gen_hid = RE.entity_gen_hid"+
				" AND NR.base_run_id = RE.base_run_id"+
				" AND NR.run_id <> NR.base_run_id"+
				" AND RL.status = "+ToQuoted(DoneRunStatus)+
				" )"+
				" FROM run_entity RE"+
				" WHERE RE.entity_gen_hid = "+strconv.Itoa(g.hId)+
				" AND RE.run_id <> RE.base_run_id"+
				" AND RE.base_run_id = "+sId+
				" ORDER BY 1, 3")
		if err != nil {
			return err
		}

		// re-base run values in microdata value table and in run_entity list
		// if not same microdata and base run id as before
		for _, rb := range rbArr {

			// update microdata value run id with new base run id
			err = TrxUpdate(trx,
				"UPDATE "+g.dbName+
					" SET run_id = "+strconv.Itoa(rb.newBase)+
					" WHERE run_id = "+strconv.Itoa(rb.oldBase))
			if err != nil {
				trx.Rollback()
				return err
			}

			// set new base run id in run_entity table
			err = TrxUpdate(trx,
				"UPDATE run_entity SET base_run_id = "+strconv.Itoa(rb.newBase)+
					" WHERE base_run_id = "+strconv.Itoa(rb.oldBase)+
					" AND entity_gen_hid = "+strconv.Itoa(rb.hId))
			if err != nil {
				trx.Rollback()
				return err
			}
		}

		trx.Commit()
	}

	// set run status d=deleted and replace run digest
	tx, err = dbConn.Begin()
	if err != nil {
		return err
	}
	err = TrxUpdate(tx,
		"UPDATE run_lst SET status = 'd', run_digest = "+toQuotedMax(delDgst, codeDbMax)+
			" WHERE run_id = "+sId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	return nil
}

// doDeleteRunBodySmallTrx delete model run values from database without transaction.
// It is incermental delete and can be restarted at any time.
// Run status must be d=deleted.
func doDeleteRunBodySmallTrx(dbConn *sql.DB, runId int) error {

	// check run status, it must d=deleted
	sId := strconv.Itoa(runId)
	status := ""

	err := SelectFirst(dbConn,
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

	// delete rows from run microdata value tables
	// or drop microdata value tables where no run_entity exists
	tbls := []string{}
	hIds := []int{}

	err = SelectRows(dbConn,
		"SELECT EG.entity_gen_hid, EG.db_entity_table"+
			" FROM entity_gen EG"+
			" INNER JOIN run_entity RE ON (RE.entity_gen_hid = EG.entity_gen_hid)"+
			" WHERE RE.run_id = "+sId,
		func(rows *sql.Rows) error {
			tn := ""
			var n int = 0
			if err := rows.Scan(&n, &tn); err != nil {
				return err
			}
			tbls = append(tbls, tn)
			hIds = append(hIds, n)
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	for k := 0; k < len(hIds); k++ {

		// delete rows from microdata values table
		err := Update(dbConn, "DELETE FROM "+tbls[k]+" WHERE run_id = "+strconv.Itoa(runId))
		if err != nil {
			return err
		}

		// delete run metadata inside of transaction scope
		trx, err := dbConn.Begin()
		if err != nil {
			return err
		}
		if err := doTrxDeleteEntityMicrodata(trx, runId, hIds[k], tbls[k]); err != nil {
			trx.Rollback()
			return err
		}
		trx.Commit()
	}

	// delete run expressions value and accumulators values
	eTbls := []string{}
	aTbls := []string{}
	hIds = []int{}

	err = SelectRows(dbConn,
		"SELECT T.table_hid, T.db_expr_table, T.db_acc_table"+
			" FROM run_table RT"+
			" INNER JOIN table_dic T ON (T.table_hid = RT.table_hid)"+
			" WHERE RT.run_id = "+sId,
		func(rows *sql.Rows) error {
			etn := ""
			atn := ""
			var n int = 0
			if err := rows.Scan(&n, &etn, &atn); err != nil {
				return err
			}
			eTbls = append(eTbls, etn)
			aTbls = append(aTbls, atn)
			hIds = append(hIds, n)
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	for k := 0; k < len(hIds); k++ {

		err = Update(dbConn, "DELETE FROM "+eTbls[k]+" WHERE run_id = "+sId)
		if err != nil {
			return err
		}
		err = Update(dbConn, "DELETE FROM "+aTbls[k]+" WHERE run_id = "+sId)
		if err != nil {
			return err
		}
		err = Update(dbConn,
			"DELETE FROM run_table WHERE run_id = "+sId+" AND table_hid = "+strconv.Itoa(hIds[k]))
		if err != nil {
			return err
		}
	}

	// delete parameter values
	tbls = []string{}
	hIds = []int{}

	err = SelectRows(dbConn,
		"SELECT P.parameter_hid, P.db_run_table"+
			" FROM run_parameter RP"+
			" INNER JOIN parameter_dic P ON (P.parameter_hid = RP.parameter_hid)"+
			" WHERE RP.run_id = "+sId,
		func(rows *sql.Rows) error {
			tn := ""
			var n int = 0
			if err := rows.Scan(&n, &tn); err != nil {
				return err
			}
			tbls = append(tbls, tn)
			hIds = append(hIds, n)
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	for k := range tbls {

		err = Update(dbConn, "DELETE FROM "+tbls[k]+" WHERE run_id = "+sId)
		if err != nil {
			return err
		}
		err = Update(dbConn,
			"DELETE FROM run_parameter WHERE run_id = "+sId+" AND parameter_hid = "+strconv.Itoa(hIds[k]))
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete run entity metadata:
// delete run_entity row for the entity generation and run id,
// if there are no any run_entity rows exist for that table then drop entity generation table (microdata values table)
// It does update as part of transaction
func doTrxDeleteEntityMicrodata(trx *sql.Tx, runId, genHid int, dbTableName string) error {

	// delete microdata run row: there is no microdata values of that entity exist in that model run
	sId := strconv.Itoa(runId)
	sGid := strconv.Itoa(genHid)

	err := TrxUpdate(trx,
		"DELETE FROM run_entity"+" WHERE run_id = "+sId+" AND entity_gen_hid = "+sGid)
	if err != nil {
		return err
	}

	// check if there are any runs where microdata values are the same entity generation as current run
	dTbl := "--" + string([]rune(dbTableName)[2:])

	err = TrxUpdate(trx,
		"UPDATE entity_gen SET db_entity_table = "+ToQuoted(dTbl)+
			" WHERE db_entity_table = "+ToQuoted(dbTableName)+
			" AND NOT EXISTS (SELECT * FROM run_entity RE WHERE RE.entity_gen_hid = entity_gen.entity_gen_hid)",
	)
	if err != nil {
		return err
	}

	isNoData := false
	err = TrxSelectFirst(trx,
		"SELECT COUNT(*) FROM entity_gen WHERE db_entity_table = "+ToQuoted(dTbl),
		func(row *sql.Row) error {
			var n int = 0
			if err := row.Scan(&n); err != nil {
				return err
			}
			isNoData = n > 0
			return nil
		})
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	err = TrxUpdate(trx,
		"UPDATE entity_gen SET db_entity_table = "+ToQuoted(dbTableName)+
			" WHERE db_entity_table = "+ToQuoted(dTbl)+
			" AND NOT EXISTS (SELECT * FROM run_entity RE WHERE RE.entity_gen_hid = entity_gen.entity_gen_hid)",
	)
	if err != nil {
		return err
	}

	// drop microdata table if there are no run_entity rows exist for that table
	if isNoData {

		err = TrxUpdate(trx,
			"DELETE FROM entity_gen_attr"+
				" WHERE EXISTS"+
				" ("+
				" SELECT * FROM entity_gen EG"+
				" WHERE EG.entity_gen_hid = entity_gen_attr.entity_gen_hid"+
				" AND EG.db_entity_table = "+ToQuoted(dbTableName)+
				" )"+
				" AND NOT EXISTS (SELECT * FROM run_entity RE WHERE RE.entity_gen_hid = entity_gen_attr.entity_gen_hid)",
		)
		if err != nil {
			return err
		}
		err = TrxUpdate(trx,
			"DELETE FROM entity_gen"+
				" WHERE db_entity_table = "+ToQuoted(dbTableName)+
				" AND NOT EXISTS (SELECT * FROM run_entity RE WHERE RE.entity_gen_hid = entity_gen.entity_gen_hid)",
		)
		if err != nil {
			return err
		}

		err = TrxUpdate(trx, "DROP TABLE "+dbTableName)
		if err != nil {
			return err
		}
	}

	return nil
}
