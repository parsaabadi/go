// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"database/sql"
	"errors"
	"strconv"
)

// ReadParameter read input parameter page (sub id, dimensions, value) from workset or model run results.
func ReadParameter(dbConn *sql.DB, modelDef *ModelMeta, layout *ReadParamLayout) (*list.List, error) {

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
	//   check readonly status: if layout.IsEditSet then read-only must be false
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
		if layout.IsEditSet && setRow.IsReadonly {
			return nil, errors.New("cannot edit parameter " + param.Name + " from read-only workset, id: " + strconv.Itoa(layout.FromId))
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
		if !IsRunCompleted(runRow.Status) {
			return nil, errors.New("model run not completed, id: " + strconv.Itoa(srcRunId))
		}
	}

	// make sql to select parameter from model run:
	//   SELECT sub_id, dim0, dim1, param_value
	//   FROM ageSex_p2012_817
	//   WHERE run_id = (SELECT base_run_id FROM run_parameter WHERE run_id = 1234 AND parameter_hid = 1)
	//   AND dim1 IN (1, 2, 3, 4)
	//   ORDER BY 1, 2, 3
	// or workset:
	//   SELECT sub_id, dim0, dim1, param_value
	//   FROM ageSex_w2012_817
	//   WHERE set_id = 9876
	//   AND dim1 IN (1, 2, 3, 4)
	//   ORDER BY 1, 2, 3
	q := "SELECT sub_id, "
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
	q += makeOrderBy(param.Rank, layout.OrderBy, 1)

	// prepare db-row conversion buffer: sub_id, dimensions, value
	var nSub int
	d := make([]int, param.Rank)
	var v interface{}
	var vb bool
	var vs string
	var fc func(c *CellParam)
	var scanBuf []interface{}

	scanBuf = append(scanBuf, &nSub)
	for k := 0; k < param.Rank; k++ {
		scanBuf = append(scanBuf, &d[k])
	}
	switch {
	case param.typeOf.IsBool():
		scanBuf = append(scanBuf, &vb)
		fc = func(c *CellParam) { c.SubId = nSub; copy(c.DimIds, d); c.Value = vb }
	case param.typeOf.IsString():
		scanBuf = append(scanBuf, &vs)
		fc = func(c *CellParam) { c.SubId = nSub; copy(c.DimIds, d); c.Value = vs }
	default:
		scanBuf = append(scanBuf, &v)
		fc = func(c *CellParam) { c.SubId = nSub; copy(c.DimIds, d); c.Value = v }
	}

	// select parameter cells: (sub id, dimension(s) enum ids, parameter value)
	cLst, err := SelectToList(dbConn, q, layout.Offset, layout.Size,
		func(rows *sql.Rows) (interface{}, error) {
			if err := rows.Scan(scanBuf...); err != nil {
				return nil, err
			}
			// make new cell from conversion buffer
			var c = CellParam{cellValue: cellValue{cellDims: cellDims{DimIds: make([]int, param.Rank)}}}
			fc(&c)
			return c, nil
		})
	if err != nil {
		return nil, err
	}

	return cLst, nil
}
