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

	// delete model runs metadata
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
		hId  int    // table Hid
		expr string // expressions db-table name
		acc  string // accumulators db-table name
	}
	var tblArr []outTbl

	err = TrxSelectRows(trx,
		"SELECT"+
			" D.table_hid, D.db_expr_table, D.db_acc_table"+
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
			if err := rows.Scan(&r.hId, &r.expr, &r.acc); err != nil {
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
	// delete types metadata where type not shared between models and type is not built-in
	sBltIn := strconv.Itoa(maxBuiltInTypeId)
	err = TrxUpdate(trx,
		"DELETE FROM type_enum_txt"+
			" WHERE EXISTS"+
			" ("+
			" SELECT type_hid"+
			" FROM model_type_dic M"+
			" WHERE M.type_hid = type_enum_txt.type_hid AND M.model_id = "+smId+
			" AND M.model_type_id > "+sBltIn+
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
			" WHERE EXISTS"+
			" ("+
			" SELECT type_hid"+
			" FROM model_type_dic M"+
			" WHERE M.type_hid = type_enum_lst.type_hid AND M.model_id = "+smId+
			" AND M.model_type_id > "+sBltIn+
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
			" WHERE EXISTS"+
			" ("+
			" SELECT type_hid"+
			" FROM model_type_dic M"+
			" WHERE M.type_hid = type_dic_txt.type_hid AND M.model_id = "+smId+
			" AND M.model_type_id > "+sBltIn+
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
	// delete type master rows where type does not belong to any model
	err = TrxUpdate(trx,
		"DELETE FROM type_dic"+
			" WHERE NOT EXISTS"+
			" (SELECT type_hid FROM model_type_dic NE WHERE NE.type_hid = type_dic.type_hid)")
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

	// drop db-tables for accumulators and expressions
	// where output table not shared between models
	for k := range tblArr {

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
