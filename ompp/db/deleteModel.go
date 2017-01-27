// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// DeleteModel delete existing model metadata and drop model data tables from database.
func DeleteModel(dbConn *sql.DB, modelId int) error {

	// validate parameters
	if modelId <= 0 {
		return errors.New("invalid model id: " + strconv.Itoa(modelId))
	}

	// delete inside of transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err := doDeleteModel(trx, modelId); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// doDeleteModel delete existing model metadata and drop model data tables from database.
// It does update as part of transaction
func doDeleteModel(trx *sql.Tx, modelId int) error {

	// update model master record to prevent model use
	smId := strconv.Itoa(modelId)
	err := TrxUpdate(trx, "UPDATE model_dic SET model_name = 'deleted' WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// delete modeling tasks and task run history
	err = TrxUpdate(trx,
		"DELETE FROM task_run_set WHERE EXISTS"+
			" (SELECT task_id FROM task_lst M WHERE M.task_id = task_run_set.task_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM task_run_lst WHERE EXISTS"+
			" (SELECT task_id FROM task_lst M WHERE M.task_id = task_run_lst.task_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM task_set WHERE EXISTS"+
			" (SELECT task_id FROM task_lst M WHERE M.task_id = task_set.task_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM task_txt WHERE EXISTS"+
			" (SELECT task_id FROM task_lst M WHERE M.task_id = task_txt.task_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM task_lst WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// delete model worksets metadata
	err = TrxUpdate(trx,
		"DELETE FROM workset_parameter_txt WHERE EXISTS"+
			" (SELECT set_id FROM workset_lst M WHERE M.set_id = workset_parameter_txt.set_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM workset_parameter WHERE EXISTS"+
			" (SELECT set_id FROM workset_lst M WHERE M.set_id = workset_parameter.set_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM workset_txt WHERE EXISTS"+
			" (SELECT set_id FROM workset_lst M WHERE M.set_id = workset_txt.set_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM workset_lst WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// delete model runs:
	// update to NULL base run id for all worksets where base run id's belong to model
	// it is expected to be empty result unless workset based on model run of other model (reserved for the future)
	err = TrxUpdate(trx,
		"UPDATE workset_lst SET base_run_id = NULL"+
			" WHERE EXISTS"+
			" ("+
			" SELECT RL.run_id FROM run_lst RL"+
			" WHERE RL.run_id = workset_lst.base_run_id"+
			" AND RL.model_id = "+smId+
			" )")
	if err != nil {
		return err
	}

	// delete model runs:
	// for all model parameters
	// where parameter shared between models and parameter run value shared between runs
	// build list of new base run id's
	rbArr, err := selectBaseRunsOfSharedValues(trx,
		"SELECT"+
			" MRP.parameter_hid, MRP.run_id, MRP.base_run_id,"+
			" ("+
			" SELECT MIN(NR.run_id)"+
			" FROM run_parameter NR"+
			" WHERE NR.base_run_id = MRP.base_run_id AND NR.parameter_hid = MRP.parameter_hid"+
			" AND NR.run_id <> MRP.base_run_id"+
			" )"+
			" FROM run_parameter MRP"+
			" INNER JOIN run_lst MRL ON (MRL.run_id = MRP.run_id)"+
			" WHERE EXISTS"+
			" ("+
			" SELECT RP.run_id"+
			" FROM run_parameter RP"+
			" INNER JOIN run_lst RL ON (RL.run_id = RP.base_run_id)"+
			" WHERE RP.run_id = MRP.run_id"+
			" AND RP.parameter_hid = MRP.parameter_hid"+
			" AND RL.model_id <> MRL.model_id"+
			" AND RL.model_id = "+smId+
			" )"+
			" ORDER BY 1, 3")
	if err != nil {
		return err
	}

	// delete model runs:
	// where parameter shared between models and parameter run value shared between runs
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
	// for all model output tables
	// where output table shared between models and output table value shared between runs
	// build list of new base run id's
	rbArr, err = selectBaseRunsOfSharedValues(trx,
		"SELECT"+
			" MRT.table_hid, MRT.run_id, MRT.base_run_id,"+
			" ("+
			" SELECT MIN(NR.run_id)"+
			" FROM run_table NR"+
			" WHERE NR.base_run_id = MRT.base_run_id AND NR.table_hid = MRT.table_hid"+
			" AND NR.run_id <> MRT.base_run_id"+
			" )"+
			" FROM run_table MRT"+
			" INNER JOIN run_lst MRL ON (MRL.run_id = MRT.run_id)"+
			" WHERE EXISTS"+
			" ("+
			" SELECT RT.run_id"+
			" FROM run_table RT"+
			" INNER JOIN run_lst RL ON (RL.run_id = RT.base_run_id)"+
			" WHERE RT.run_id = MRT.run_id"+
			" AND RT.table_hid = MRT.table_hid"+
			" AND RL.model_id <> MRL.model_id"+
			" AND RL.model_id = "+smId+
			" )"+
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
	err = TrxUpdate(trx,
		"DELETE FROM run_table WHERE EXISTS"+
			" (SELECT run_id FROM run_lst M WHERE M.run_id = run_table.run_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM run_parameter_txt WHERE EXISTS"+
			" (SELECT run_id FROM run_lst M WHERE M.run_id = run_parameter_txt.run_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM run_parameter WHERE EXISTS"+
			" (SELECT run_id FROM run_lst M WHERE M.run_id = run_parameter.run_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM run_option WHERE EXISTS"+
			" (SELECT run_id FROM run_lst M WHERE M.run_id = run_option.run_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM run_txt WHERE EXISTS"+
			" (SELECT run_id FROM run_lst M WHERE M.run_id = run_txt.run_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM run_lst WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// skip model default profile: profile_lst and profile_option
	// there is no explicit link between profile and model

	// delete model groups
	err = TrxUpdate(trx,
		"DELETE FROM group_pc WHERE EXISTS"+
			" (SELECT group_id FROM group_lst M WHERE M.group_id = group_pc.group_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM group_txt WHERE EXISTS"+
			" (SELECT group_id FROM group_lst M WHERE M.group_id = group_txt.group_id AND M.model_id = "+smId+")")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM group_lst WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// delete model output tables:
	// build list of model output tables where table not shared between models
	type outTbl struct {
		hId    int    // table Hid
		expr   string // expressions db table
		acc    string // accumulators db table
		accAll string // all accumulators db view
	}
	var tblArr []outTbl

	err = TrxSelectRows(trx,
		"SELECT"+
			" D.table_hid, D.db_expr_table, D.db_acc_table, D.db_acc_all_view"+
			" FROM table_dic D"+
			" WHERE EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic M"+
			" WHERE M.table_hid = D.table_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic NE"+
			" WHERE NE.table_hid = D.table_hid AND NE.model_id <> "+smId+
			" )",
		func(rows *sql.Rows) error {
			var r outTbl
			if err := rows.Scan(&r.hId, &r.expr, &r.acc, &r.accAll); err != nil {
				return err
			}
			tblArr = append(tblArr, r)
			return nil
		})
	if err != nil {
		return err
	}

	// delete model output tables:
	// delete output tables metadata where table not shared between models
	err = TrxUpdate(trx,
		"DELETE FROM table_expr_txt"+
			" WHERE EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic M"+
			" WHERE M.table_hid = table_expr_txt.table_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic NE"+
			" WHERE NE.table_hid = table_expr_txt.table_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM table_expr"+
			" WHERE EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic M"+
			" WHERE M.table_hid = table_expr.table_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic NE"+
			" WHERE NE.table_hid = table_expr.table_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM table_acc_txt"+
			" WHERE EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic M"+
			" WHERE M.table_hid = table_acc_txt.table_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic NE"+
			" WHERE NE.table_hid = table_acc_txt.table_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM table_acc"+
			" WHERE EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic M"+
			" WHERE M.table_hid = table_acc.table_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic NE"+
			" WHERE NE.table_hid = table_acc.table_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM table_dims_txt"+
			" WHERE EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic M"+
			" WHERE M.table_hid = table_dims_txt.table_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic NE"+
			" WHERE NE.table_hid = table_dims_txt.table_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM table_dims"+
			" WHERE EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic M"+
			" WHERE M.table_hid = table_dims.table_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic NE"+
			" WHERE NE.table_hid = table_dims.table_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM table_dic_txt"+
			" WHERE EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic M"+
			" WHERE M.table_hid = table_dic_txt.table_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT table_hid"+
			" FROM model_table_dic NE"+
			" WHERE NE.table_hid = table_dic_txt.table_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	// delete model output tables:
	// delete model output table master rows
	err = TrxUpdate(trx, "DELETE FROM model_table_dic WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// delete model output tables:
	// delete output table master rows where output table does not belong to any model
	err = TrxUpdate(trx,
		"DELETE FROM table_dic"+
			" WHERE NOT EXISTS"+
			" (SELECT table_hid FROM model_table_dic NE WHERE NE.table_hid = table_dic.table_hid)")
	if err != nil {
		return err
	}

	// delete model parameters:
	// build list of model parameters where parameter not shared between models
	type paramItem struct {
		hId int    // parameter Hid
		ws  string // workset parameter values db-table name
		run string // run parameter values db-table name
	}
	var paramArr []paramItem

	err = TrxSelectRows(trx,
		"SELECT"+
			" D.parameter_hid, D.db_set_table, D.db_run_table"+
			" FROM parameter_dic D"+
			" WHERE EXISTS"+
			" ("+
			" SELECT parameter_hid"+
			" FROM model_parameter_dic M"+
			" WHERE M.parameter_hid = D.parameter_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT parameter_hid"+
			" FROM model_parameter_dic NE"+
			" WHERE NE.parameter_hid = D.parameter_hid AND NE.model_id <> "+smId+
			" )",
		func(rows *sql.Rows) error {
			var r paramItem
			if err := rows.Scan(&r.hId, &r.ws, &r.run); err != nil {
				return err
			}
			paramArr = append(paramArr, r)
			return nil
		})
	if err != nil {
		return err
	}

	// delete model parameters:
	// delete parameters metadata where parameter not shared between models
	err = TrxUpdate(trx,
		"DELETE FROM parameter_dims_txt"+
			" WHERE EXISTS"+
			" ("+
			" SELECT parameter_hid"+
			" FROM model_parameter_dic M"+
			" WHERE M.parameter_hid = parameter_dims_txt.parameter_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT parameter_hid"+
			" FROM model_parameter_dic NE"+
			" WHERE NE.parameter_hid = parameter_dims_txt.parameter_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM parameter_dims"+
			" WHERE EXISTS"+
			" ("+
			" SELECT parameter_hid"+
			" FROM model_parameter_dic M"+
			" WHERE M.parameter_hid = parameter_dims.parameter_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT parameter_hid"+
			" FROM model_parameter_dic NE"+
			" WHERE NE.parameter_hid = parameter_dims.parameter_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM parameter_dic_txt"+
			" WHERE EXISTS"+
			" ("+
			" SELECT parameter_hid"+
			" FROM model_parameter_dic M"+
			" WHERE M.parameter_hid = parameter_dic_txt.parameter_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT parameter_hid"+
			" FROM model_parameter_dic NE"+
			" WHERE NE.parameter_hid = parameter_dic_txt.parameter_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	// delete model parameters:
	// delete model parameter master rows
	err = TrxUpdate(trx, "DELETE FROM model_parameter_dic WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// delete model parameters:
	// delete parameter master row where parameter does not belong to any model
	err = TrxUpdate(trx,
		"DELETE FROM parameter_dic"+
			" WHERE NOT EXISTS"+
			" (SELECT parameter_hid FROM model_parameter_dic NE WHERE NE.parameter_hid = parameter_dic.parameter_hid)")
	if err != nil {
		return err
	}

	// delete model types:
	// delete types metadata where type is not built-in and not shared between models
	err = TrxUpdate(trx,
		"DELETE FROM type_enum_txt"+
			" WHERE type_hid > "+strconv.Itoa(maxBuiltInTypeId)+
			" AND EXISTS"+
			" ("+
			" SELECT type_hid"+
			" FROM model_type_dic M"+
			" WHERE M.type_hid = type_enum_txt.type_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT type_hid"+
			" FROM model_type_dic NE"+
			" WHERE NE.type_hid = type_enum_txt.type_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM type_enum_lst"+
			" WHERE type_hid > "+strconv.Itoa(maxBuiltInTypeId)+
			" AND EXISTS"+
			" ("+
			" SELECT type_hid"+
			" FROM model_type_dic M"+
			" WHERE M.type_hid = type_enum_lst.type_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT type_hid"+
			" FROM model_type_dic NE"+
			" WHERE NE.type_hid = type_enum_lst.type_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	err = TrxUpdate(trx,
		"DELETE FROM type_dic_txt"+
			" WHERE type_hid > "+strconv.Itoa(maxBuiltInTypeId)+
			" AND EXISTS"+
			" ("+
			" SELECT type_hid"+
			" FROM model_type_dic M"+
			" WHERE M.type_hid = type_dic_txt.type_hid AND M.model_id = "+smId+
			" )"+
			" AND NOT EXISTS"+
			" ("+
			" SELECT type_hid"+
			" FROM model_type_dic NE"+
			" WHERE NE.type_hid = type_dic_txt.type_hid AND NE.model_id <> "+smId+
			" )")
	if err != nil {
		return err
	}

	// delete model types:
	// delete model type master rows
	err = TrxUpdate(trx, "DELETE FROM model_type_dic WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// delete model types:
	// delete type master rows where type is not built-in and not belong to any model
	err = TrxUpdate(trx,
		"DELETE FROM type_dic"+
			" WHERE type_hid > "+strconv.Itoa(maxBuiltInTypeId)+
			" AND NOT EXISTS (SELECT type_hid FROM model_type_dic MT WHERE MT.type_hid = type_dic.type_hid)")
	if err != nil {
		return err
	}

	// delete model language-specific message rows
	err = TrxUpdate(trx, "DELETE FROM model_word WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// delete model text rows: model description and notes
	err = TrxUpdate(trx, "DELETE FROM model_dic_txt WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// delete model master row
	err = TrxUpdate(trx, "DELETE FROM model_dic WHERE model_id = "+smId)
	if err != nil {
		return err
	}

	// drop db tables and views for accumulators and expressions
	// where output table not shared between models
	for k := range tblArr {

		err = TrxUpdate(trx, "DROP VIEW "+tblArr[k].accAll)
		if err != nil {
			return err
		}

		err = TrxUpdate(trx, "DROP TABLE "+tblArr[k].expr)
		if err != nil {
			return err
		}

		err = TrxUpdate(trx, "DROP TABLE "+tblArr[k].acc)
		if err != nil {
			return err
		}
	}

	// drop db-tables for parameter workset values and run values
	// where parameter not shared between models
	for k := range paramArr {

		err = TrxUpdate(trx, "DROP TABLE "+paramArr[k].ws)
		if err != nil {
			return err
		}

		err = TrxUpdate(trx, "DROP TABLE "+paramArr[k].run)
		if err != nil {
			return err
		}
	}

	return nil
}

// runBaseItem is holder for Hid base run id's of parameter or output table.
// It is used to update base run during delete operations (re-base) in run_parameter and run_table.
type runBaseItem struct {
	hId     int // output table Hid
	runId   int // run id in run_table
	oldBase int // current base run id
	newBase int // new base run id
}

// selectBaseRunsOfSharedValues return list of base run id's for parameters and (or output tables)
// where parameter shared between models and parameter value shared between runs
// build list of new base run id's
func selectBaseRunsOfSharedValues(trx *sql.Tx, q string) ([]runBaseItem, error) {

	var rbArr []runBaseItem
	err := TrxSelectRows(trx, q,
		func(rows *sql.Rows) error {
			var r runBaseItem
			var n sql.NullInt64
			if err := rows.Scan(&r.hId, &r.runId, &r.oldBase, &n); err != nil {
				return err
			}
			if n.Valid {
				r.newBase = int(n.Int64)
			} else {
				r.newBase = r.runId // if no new base run found then use run itself
			}
			rbArr = append(rbArr, r)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return rbArr, nil
}
