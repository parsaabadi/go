// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"database/sql"
	"errors"
	"strconv"
)

// ReadParameter read input parameter page (dimensions and value) from workset or model run results.
func ReadParameter(dbConn *sql.DB, modelDef *ModelMeta, layout *ReadLayout) (*list.List, error) {

	// validate parameters
	if modelDef == nil {
		return nil, errors.New("invalid (empty) model metadata, look like model not found")
	}
	if layout == nil {
		return nil, errors.New("invalid (empty) page layout")
	}
	if layout.Name == "" {
		return nil, errors.New("invalid (empty) parameter name")
	}

	// find parameter id by name
	var param *ParamMeta
	if k, ok := modelDef.ParamByName(layout.Name); ok {
		param = &modelDef.Param[k]
	} else {
		return nil, errors.New("parameter not found: " + layout.Name)
	}

	// if this is workset parameter then:
	//   if source workset exist
	//   check readonly status: it must be !=layout.IsEditSet
	//   if parameter not in workset then select base run id, it must be >0
	var srcRunId int
	var isWsParam bool

	if !layout.IsFromSet {
		srcRunId = layout.FromId // this is parameter from existing run
	} else {

		// validate workset: it must exist
		setRow, err := GetWorkset(dbConn, layout.FromId)
		if err != nil {
			return nil, err
		}
		if setRow == nil {
			return nil, errors.New("workset not found, id: " + strconv.Itoa(layout.FromId))
		}

		// workset readonly status must be compatible with (oposite to) "edit workset" status
		if setRow.IsReadonly == layout.IsEditSet {
			return nil, errors.New("cannot read or edit parameter " + param.Name + " from workset, id: " + strconv.Itoa(layout.FromId))
		}

		// check is this workset contain the parameter
		err = SelectFirst(dbConn,
			"SELECT COUNT(*) FROM workset_parameter"+
				" WHERE set_id = "+strconv.Itoa(layout.FromId)+
				" AND parameter_hid = "+strconv.Itoa(param.ParamHid),
			func(row *sql.Row) error {
				var n int
				if err := row.Scan(&n); err != nil {
					return err
				}
				isWsParam = n != 0
				return nil
			})
		switch {
		case err == sql.ErrNoRows: // unknown error: should never be there
			return nil, errors.New("cannot count parameter " + param.Name + " in workset, id: " + strconv.Itoa(layout.FromId))
		case err != nil:
			return nil, err
		}

		// if parameter not in that workset then workset must have base run
		if !isWsParam {
			if setRow.BaseRunId <= 0 {
				return nil, errors.New("workset does not contain parameter " + param.Name + " and not run-based, workset id: " + strconv.Itoa(layout.FromId))
			}
			srcRunId = setRow.BaseRunId
		}
	}

	// if parameter from run (or from workset base run) then:
	//   check if model run exist and model run completed
	if !isWsParam {
		runRow, err := GetRun(dbConn, srcRunId)
		if err != nil {
			return nil, err
		}
		if runRow == nil {
			return nil, errors.New("model run not found, id: " + strconv.Itoa(srcRunId))
		}
		if runRow.Status != DoneRunStatus && runRow.Status != ExitRunStatus && runRow.Status != ErrorRunStatus {
			return nil, errors.New("model run not completed, id: " + strconv.Itoa(srcRunId))
		}
	}

	// make sql to select parameter from model run:
	//   SELECT dim0, dim1, param_value
	//   FROM ageSex_p2012_817
	//   WHERE run_id = (SELECT base_run_id FROM run_parameter WHERE run_id = 1234 AND parameter_hid = 1)
	//   AND dim1 IN (1, 2, 3, 4)
	//   ORDER BY 1, 2
	// or workset:
	//   SELECT dim0, dim1, param_value
	//   FROM ageSex_w2012_817
	//   WHERE set_id = 9876
	//   AND dim1 IN (1, 2, 3, 4)
	//   ORDER BY 1, 2
	q := "SELECT "
	for k := range param.Dim {
		q += param.Dim[k].Name + ", "
	}
	q += "param_value FROM "

	if isWsParam {
		q += param.DbSetTable +
			" WHERE set_id = " + strconv.Itoa(layout.FromId)
	} else {
		q += param.DbRunTable +
			" WHERE run_id =" +
			" (SELECT base_run_id FROM run_parameter" +
			" WHERE run_id = " + strconv.Itoa(srcRunId) +
			" AND parameter_hid = " + strconv.Itoa(param.ParamHid) + ")"
	}

	// append dimension filters, if specified
	for k := range layout.Filter {

		// find dimension index by name
		dix := -1
		for j := range param.Dim {
			if param.Dim[j].Name == layout.Filter[k].DimName {
				dix = j
				break
			}
		}
		if dix < 0 {
			return nil, errors.New("parameter " + param.Name + " does not have dimension " + layout.Filter[k].DimName)
		}

		f, err := makeDimFilter(
			modelDef, &layout.Filter[k], param.Dim[dix].Name, param.Dim[dix].typeOf, "parameter "+param.Name)
		if err != nil {
			return nil, err
		}

		q += " AND " + f
	}

	// append order by
	q += makeOrderBy(param.Rank, layout.OrderBy, 0)

	// prepare db-row conversion buffer
	d := make([]int, param.Rank)
	var v interface{}
	var vb bool
	var vs string
	var fc func(c *Cell)

	var scanBuf []interface{}
	for k := 0; k < param.Rank; k++ {
		scanBuf = append(scanBuf, &d[k])
	}
	switch {
	case param.typeOf.IsBool():
		scanBuf = append(scanBuf, &vb)
		fc = func(c *Cell) { copy(c.DimIds, d); c.Value = vb }
	case param.typeOf.IsString():
		scanBuf = append(scanBuf, &vs)
		fc = func(c *Cell) { copy(c.DimIds, d); c.Value = vs }
	default:
		scanBuf = append(scanBuf, &v)
		fc = func(c *Cell) { copy(c.DimIds, d); c.Value = v }
	}

	// select parameter cells: dimension(s) enum ids and parameter value
	cLst, err := SelectToList(dbConn, q, layout.Offset, layout.Size,
		func(rows *sql.Rows) (interface{}, error) {
			if err := rows.Scan(scanBuf...); err != nil {
				return nil, err
			}
			var c = Cell{DimIds: make([]int, param.Rank)} // make new cell from conversion buffer
			fc(&c)
			return c, nil
		})
	if err != nil {
		return nil, err
	}

	return cLst, nil
}
