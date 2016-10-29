// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"database/sql"
	"errors"
	"strconv"
)

// DeleteTask delete modeling task and task run history from database.
func DeleteTask(dbConn *sql.DB, taskId int) error {

	// validate parameters
	if taskId <= 0 {
		return errors.New("invalid task id: " + strconv.Itoa(taskId))
	}

	// delete inside of transaction scope
	trx, err := dbConn.Begin()
	if err != nil {
		return err
	}
	if err := doDeleteTask(trx, taskId); err != nil {
		trx.Rollback()
		return err
	}
	trx.Commit()
	return nil
}

// dbDeleteTask delete modeling task and task run history from database.
// It does update as part of transaction
func doDeleteTask(trx *sql.Tx, taskId int) error {

	// update task master record to prevent task use
	stId := strconv.Itoa(taskId)
	err := TrxUpdate(trx, "UPDATE task_lst SET task_name = 'deleted' WHERE task_id = "+stId)
	if err != nil {
		return err
	}

	// delete modeling task and task run history
	err = TrxUpdate(trx, "DELETE FROM task_run_set WHERE task_id = "+stId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM task_run_lst WHERE task_id = "+stId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM task_set WHERE task_id = "+stId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM task_txt WHERE task_id = "+stId)
	if err != nil {
		return err
	}

	err = TrxUpdate(trx, "DELETE FROM task_lst WHERE task_id = "+stId)
	if err != nil {
		return err
	}

	return nil
}
