// Copyright OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"errors"
	"strconv"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/db"
)

// write old compatibilty model run parameters and output tables into csv or tsv files
func runOldValue(srcDb *sql.DB, modelId int) error {

	return nil
}

// write old compatibilty run paratemer values into csv or tsv file
func parameterOldValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find first model run
	msg, run, err := findRun(srcDb, modelId, "", 0, true, false)
	if err != nil {
		return errors.New("Error at get model run: " + msg + " " + err.Error())
	}
	if run == nil {
		return errors.New("Error: first model run not found")
	}

	// get model metadata and find parameter
	name := runOpts.String(paramArgKey)
	if name == "" {
		return errors.New("Invalid (empty) parameter name")
	}
	meta, err := db.GetModelById(srcDb, modelId)
	if err != nil {
		return errors.New("Error at get model metadata by id: " + strconv.Itoa(modelId) + ": " + err.Error())
	}
	idx, ok := meta.ParamByName(name)
	if !ok {
		return errors.New("Error: model parameter not found: " + name)
	}

	// create compatibility view parameter header: Dim0 Dim1....Value
	hdr := []string{}

	for k := 0; k < meta.Param[idx].Rank; k++ {
		hdr = append(hdr, "Dim"+strconv.Itoa(k))
	}
	hdr = append(hdr, "Value")

	// write to csv rows starting from column 1, skip sub_id column
	return parameterRunValue(srcDb, meta, name, run, true, hdr)

}

// write old compatibilty output table values into csv or tsv file
func tableOldValue(srcDb *sql.DB, modelId int, runOpts *config.RunOptions) error {

	// find model run
	msg, run, err := findRun(srcDb, modelId, runOpts.String(runArgKey), runOpts.Int(runIdArgKey, 0), runOpts.Bool(runFirstArgKey), runOpts.Bool(runLastArgKey))
	if err != nil {
		return errors.New("Error at get model run: " + msg + " " + err.Error())
	}
	if run == nil {
		return errors.New("Error: model run not found")
	}

	// get model metadata and find output table
	name := runOpts.String(tableArgKey)
	if name == "" {
		return errors.New("Invalid (empty) output tabel name")
	}
	meta, err := db.GetModelById(srcDb, modelId)
	if err != nil {
		return errors.New("Error at get model metadata by id: " + strconv.Itoa(modelId) + ": " + err.Error())
	}
	idx, ok := meta.OutTableByName(name)
	if !ok {
		return errors.New("Error: model output table not found: " + name)
	}

	// create compatibility view output table header: Dim0 Dim1....Value
	// measure dimension is the last, at [rank] postion
	hdr := []string{}

	for k := 0; k < meta.Table[idx].Rank; k++ {
		hdr = append(hdr, "Dim"+strconv.Itoa(k))
	}
	hdr = append(hdr, "Dim"+strconv.Itoa(meta.Table[idx].Rank))
	hdr = append(hdr, "Value")

	// write output table values to csv or tsv file
	return tableRunValue(srcDb, meta, runOpts.String(tableArgKey), run, runOpts, true, hdr)
}
