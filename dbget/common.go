// Copyright OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"runtime"

	_ "github.com/mattn/go-sqlite3"

	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

// return row []string or isEof = true
type rowConverter func() (isEof bool, row []string, err error)

// write into outputDir/file.json if jsonPath is not "" empty and/or to console
func toJsonOutput(isConsole bool, jsonPath string, src interface{}) error {

	if isConsole {
		ce := json.NewEncoder(os.Stdout)
		ce.SetIndent("", "  ")
		if err := ce.Encode(src); err != nil {
			return errors.New("json encode error: " + err.Error())
		}
	}
	if jsonPath != "" {
		return helper.ToJsonIndentFile(jsonPath, src)
	}
	return nil
}

// write into outputDir/file.csv if csvPath is not "" empty and/or to console
func toCsvOutput(isConsole bool, csvPath string, columnNames []string, lineCvt rowConverter) error {

	// create csv file
	isFile := csvPath != ""
	var f *os.File
	var err error

	if isFile {
		f, err = os.OpenFile(csvPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
	}
	defer func() {
		if isFile {
			f.Close()
		}
	}()

	if isFile && theCfg.isWriteUtf8Bom { // if required then write utf-8 bom
		if _, err = f.Write(helper.Utf8bom); err != nil {
			return err
		}
	}

	// create csv writes to file and/or to console
	var wr *csv.Writer
	var cw *csv.Writer
	if isFile {
		wr = csv.NewWriter(f)
	}
	if isConsole {
		cw = csv.NewWriter(os.Stdout)
		if runtime.GOOS == "windows" {
			cw.UseCRLF = true
		}
	}

	// write header line: column names, if provided
	if len(columnNames) > 0 {
		if isConsole {
			err = cw.Write(columnNames)
			isConsole = err == nil
		}
		if isFile {
			if err = wr.Write(columnNames); err != nil {
				return err
			}
		}
	}

	// write csv lines until eof
	for {
		isEof, row, err := lineCvt()
		if err != nil {
			return err
		}
		if isEof {
			break
		}
		if isConsole {
			err = cw.Write(row)
			isConsole = err == nil
			if !isConsole && !isFile {
				return err
			}
		}
		if isFile {
			if err = wr.Write(row); err != nil {
				return err
			}
		}
	}

	// flush and return error, if any
	if isConsole {
		cw.Flush()
	}
	if isFile {
		wr.Flush()
		return wr.Error()
	}
	return nil
}

// remove output directory if required, create output directory if not already exists
func makeOutputDir() error {

	if !theCfg.isNoFile && theCfg.dir != "" {
		if !theCfg.isKeepOutputDir {
			if isOk := dirDeleteAndLog(theCfg.dir); !isOk {
				return errors.New("Error: unable to delete: " + theCfg.dir)
			}
		}
		if err := os.MkdirAll(theCfg.dir, 0750); err != nil {
			return err
		}
	}
	return nil
}

// Delete directory and log path, return false on delete error.
func dirDeleteAndLog(path string) bool {

	isExist, err := helper.IsDirExist(path)
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
