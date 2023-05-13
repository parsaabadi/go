// Copyright (c) 2020 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"errors"
	"strconv"
	"strings"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/omppLog"
)

// display list of the models in SQLite database file
func dbListModels(filePath string) error {

	// sqlite file path argument required and cannot be empty
	if filePath == "" {
		return errors.New("dbcopy invalid (empty or missing) argument of: " + listModelsArgKey)
	}

	// open source database connection and check is it valid
	srcDb, _, err := db.Open(db.MakeSqliteDefaultReadOnly(filePath), db.SQLiteDbDriver, false)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	nv, err := db.OpenmppSchemaVersion(srcDb)
	if err != nil || nv <= 0 {
		return errors.New("error: invalid database, likely not an openM++ database")
	}

	// get model list
	mLst, err := db.GetModelList(srcDb)
	if err != nil {
		return err
	}
	if len(mLst) <= 0 {
		omppLog.Log("Database is empty, models not found")
		return nil
	}

	// find padding size for model id, name, version and created date-time
	mId := len("Id")
	mName := len("Name")
	mVer := len("Ver")
	mCr := len("Created")

	for _, md := range mLst {

		sId := strconv.Itoa(md.ModelId)
		if mId < len(sId) {
			mId = len(sId)
		}
		if mName < len(md.Name) {
			mName = len(md.Name)
		}
		if mVer < len(md.Version) {
			mVer = len(md.Version)
		}
		if mCr < len(md.CreateDateTime) {
			mCr = len(md.CreateDateTime)
		}
	}

	lPad := func(src string, max int) string {
		if len(src) >= max {
			return src
		}
		return strings.Repeat("\x20", max-len(src)) + src
	}
	rPad := func(src string, max int) string {
		if len(src) >= max {
			return src
		}
		return src + strings.Repeat("\x20", max-len(src))
	}

	omppLog.Log(rPad("Id", mId+1), "| ", rPad("Name", mName+1), "| ", rPad("Ver", mVer+1), "| ", rPad("Created", mCr+1), "| ", "Digest")

	// display list of the models
	for _, md := range mLst {

		sId := lPad(strconv.Itoa(md.ModelId), mId)
		omppLog.Log(rPad(sId, mId+1), "| ", rPad(md.Name, mName+1), "| ", rPad(md.Version, mVer+1), "| ", rPad(md.CreateDateTime, mCr+1), "| ", md.Digest)
	}

	return nil
}
