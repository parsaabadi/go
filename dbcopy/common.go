// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"os"

	"github.com/openmpp/go/ompp/omppLog"
)

// isDirExist true if path exists and it is directory, return error if path is not a directory or not accessible
func isDirExist(dirPath string) (bool, error) {
	fi, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.New("Error: unable to access directory: " + dirPath + " : " + err.Error())
	}
	if !fi.IsDir() {
		return false, errors.New("Error: directory expected: " + dirPath)
	}
	return true, nil
}

// Delete file and log path if isLog is true, return false on delete error.
func dirDeleteAndLog(path string) bool {

	isExist, err := isDirExist(path)
	if err != nil {
		return false // error: path not accessible or it is not a directory
	}
	if !isExist {
		return true // OK: nothing to delete
	}

	omppLog.Log("Delete: ", path)

	if e := os.RemoveAll(path); e != nil && !os.IsNotExist(e) {
		omppLog.Log(e)
		return false // error: delete failed
	}
	return true // OK: deleted successfully
}
