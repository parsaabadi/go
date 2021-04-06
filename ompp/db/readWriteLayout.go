// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"strconv"
)

// WriteLayout describes parameters or output tables values for insert or update.
//
// Name is a parameter or output table name to read.
type WriteLayout struct {
	Name string // parameter name or output table name
	ToId int    // run id or set id to write parameter or output table values
}

// WriteParamLayout describes parameter values for insert or update.
// Double format string is used for digest calcultion if value type if float or double.
type WriteParamLayout struct {
	WriteLayout        // common write layout: parameter name, run or set id
	IsToRun     bool   // if true then write into into model run else into workset
	IsPage      bool   // if true then write only page of data else all parameter values
	SubCount    int    // parameter sub-values count
	DoubleFmt   string // used for float model types digest calculation
}

// WriteTableLayout describes output table values for insert or update.
// Double format string is used for digest calcultion if value type if float or double.
type WriteTableLayout struct {
	WriteLayout        // common write layout: output table name, run or set id
	DoubleFmt   string // used for float model types digest calculation
}

// ReadLayout describes source and size of data page to read input parameter or output table values.
//
// If IsLastPage true then return non-empty last page and actual first row offset and size.
//
// Row filters combined by AND and allow to select dimension items,
// it can be enum codes or enum id's, ex.: dim0 = 'CA' AND dim1 IN (2010, 2011, 2012)
//
// Order by applied to output columns, dimension columns always contain enum id's,
// therefore result ordered by id's and not by enum codes.
// Columns list depending on output table or parameter query:
//
// parameter values:
//   SELECT sub_id, dim0, dim1, param_value FROM parameterTable ORDER BY...
//
// output table expressions:
//   SELECT expr_id, dim0, dim1, expr_value FROM outputTable ORDER BY...
//
// output table accumulators:
//   SELECT acc_id, sub_id, dim0, dim1, acc_value FROM outputTable ORDER BY...
//
// all-accumulators view:
//   SELECT sub_id, dim0, dim1, acc0_value, acc1_value... FROM outputTable ORDER BY...
//
type ReadLayout struct {
	Name           string           // parameter name or output table name
	FromId         int              // run id or set id to select input parameter or output table values
	ReadPageLayout                  // read page first row offset, size and last page flag
	Filter         []FilterColumn   // dimension filters, final WHERE does join all filters by AND
	FilterById     []FilterIdColumn // dimension filters by enum ids, final WHERE does join filters by AND
	OrderBy        []OrderByColumn  // order by columnns, if empty then dimension id ascending order is used
}

// ReadParamLayout describes source and size of data page to read input parameter values.
//
// It can read parameter values from model run results or from input working set (workset).
// If this is read from workset then it can be read-only or read-write (editable) workset.
type ReadParamLayout struct {
	ReadLayout      // parameter name, page size, where filters and order by
	IsFromSet  bool // if true then select from workset else from model run
	IsEditSet  bool // if true then workset must be editable (readonly = false)
}

// ReadTableLayout describes source and size of data page to read output table values.
//
// If ValueName is not empty then only accumulator or output expression
// with that name selected (i.e: "acc1" or "expr4") else all output table accumulators (expressions) selected.
type ReadTableLayout struct {
	ReadLayout        // output table name, page size, where filters and order by
	ValueName  string // if not empty then expression or accumulator name to select
	IsAccum    bool   // if true then select output table accumulator else expression
	IsAllAccum bool   // if true then select from all accumulators view else from accumulators table
}

// ReadTableCompareLayout describes source and size of data page to read output table values from multiple runs.
//
// Result is a page of data where each row contains base run value, current run value and optional difference and ratio.
// Columns list depending on output table or parameter query:
//
// output table expressions from multiple runs:
//   SELECT
//     M.run_id, C.run_id, M.expr_id, M.dim0, M.dim1,
//     M.expr_value,
//     C.expr_value,
//     C.expr_value - M.expr_value AS "diff",
//     C.expr_value / CASE WHEN ABS(M.expr_value) > 1.0e-37 THEN M.expr_value ELSE NULL END AS "ratio"
//   FROM outputTable M
//   INNER JOIN outputTable C ON (M.expr_id = C.expr_id AND M.dim0 = C.dim0 AND M.dim1 = C.dim1)
//   WHERE M.run_id = 123 AND C.run_id = 456
//   AND ....filter by dimensions....
//   ORDER BY...
//
// output table accumulators from multiple runs:
//   SELECT
//     M.run_id, C.run_id, M.acc_id, M.sub_id, M.dim0, M.dim1,
//     M.acc_value,
//     C.acc_value,
//     C.acc_value - M.acc_value AS "diff",
//     C.acc_value / CASE WHEN ABS(M.acc_value) > 1.0e-37 THEN M.acc_value ELSE NULL END AS "ratio"
//   FROM outputTable M
//   INNER JOIN outputTable C ON (M.acc_id = C.acc_id AND M.sub_id = C.sub_id AND M.dim0 = C.dim0 AND M.dim1 = C.dim1)
//   WHERE M.run_id = 123 AND C.run_id = 456
//   AND ....filter by dimensions....
//   ORDER BY...
//
type ReadTableCompareLayout struct {
	ReadTableLayout       // output table name, page size of rows to select, where filters and order by
	IsDiff          bool  // if true then also select difference: current run value - base run value
	IsRatio         bool  // if true then also select ratio: current run value / base run value
	runIds          []int // run id's to compare with base run
}

// TableRunsCompareLayout describes source and size of data page to read output table values from multiple runs.
//
// Result is a page of data where each row contains base run value, current run value and optional difference and ratio.
//
// Runs to compare are selected either task_run_set table or from run_lst table.
// Select from task_run_set table is done by task name and task run name or stamp.
// Select from run_lst table is done by RunTags[] elements, which are run digests, or run stamps or run names.
// Content of RunTags[] must be uniform, all elements must be of the same kind:
// run digest or run stamp or run name, it cannot be a mix of digests, stamps, names.
type TableRunsCompareLayout struct {
	TableLayout ReadTableCompareLayout // output table name, page size, where filters and order by
	RunTags     []string               // model runs to compare with base run: run digests or run stamps or run names
	Task        struct {
		Name   string // task name to select model runs for comparison
		RunTag string // task run name or task run stamp to select model runs for comparison
	}
}

// ReadPageLayout describes first row offset and size of data page to read input parameter or output table values.
// If IsLastPage true then return non-empty last page and actual first row offset and size.
type ReadPageLayout struct {
	Offset     int64 // first row to return from select, zero-based ofsset
	Size       int64 // max row count to select, if <= 0 then all rows
	IsLastPage bool  // if true then return non-empty last page
}

// FilterOp is enum type for filter operators in select where conditions
type FilterOp string

// Select filter operators for dimension enum ids.
const (
	InAutoOpFilter  FilterOp = "IN_AUTO" // auto convert IN list filter into equal or BETWEEN if possible
	InOpFilter      FilterOp = "IN"      // dimension enum ids in: dim2 IN (11, 22, 33)
	EqOpFilter      FilterOp = "="       // dimension equal: dim1 = 12
	BetweenOpFilter FilterOp = "BETWEEN" // dimension enum ids between: dim3 BETWEEN 44 AND 88
)

// FilterColumn define dimension column and condition to filter enum codes to build select where
type FilterColumn struct {
	DimName string   // dimension name
	Op      FilterOp // filter operator: equal, IN, BETWEEN
	Enums   []string // enum code(s): one, two or many ids depending on filter condition
}

// FilterIdColumn define dimension column and condition to filter enum ids to build select where
type FilterIdColumn struct {
	DimName string   // dimension name
	Op      FilterOp // filter operator: equal, IN, BETWEEN
	EnumIds []int    // enum id(s): one, two or many ids depending on filter condition
}

// OrderByColumn define column to order by rows selected from parameter or output table.
type OrderByColumn struct {
	IndexOne int  // one-based column index
	IsDesc   bool // if true then descending order
}

// makeOrderBy return ORDER BY clause either from explicitly specified column list
// or default: 1,...rank+1
// or empty if rank zero
func makeOrderBy(rank int, orderBy []OrderByColumn, extraIdColumns int) string {

	if len(orderBy) > 0 { // if order by excplicitly specified

		q := " ORDER BY "
		for k, co := range orderBy {
			if k > 0 {
				q += ", "
			}
			q += strconv.Itoa(co.IndexOne)
			if co.IsDesc {
				q += " DESC"
			}
		}
		return q
	}
	// else
	if rank > 0 || extraIdColumns > 0 { // default: order by  acc_id, sub_id, dimensions

		q := " ORDER BY "
		for k := 1; k <= extraIdColumns; k++ {
			if k > 1 {
				q += ", "
			}
			q += strconv.Itoa(k)
		}
		for k := 1; k <= rank; k++ {
			if k > 1 || extraIdColumns > 0 {
				q += ", "
			}
			q += strconv.Itoa(extraIdColumns + k)
		}
		return q
	}

	return ""
}

// makeDimFilter convert dimension enum codes to enum ids and return filter condition, eg: dim1 IN (1, 2, 3, 4)
func makeDimFilter(
	modelDef *ModelMeta, flt *FilterColumn, dimName string, typeOf *TypeMeta, isTotalEnabled bool, msgName string,
) (string, error) {

	// convert enum codes to ids
	cvt, err := cvtItemCodeToId(dimName, typeOf, isTotalEnabled)
	if err != nil {
		return "", err
	}
	fltId := FilterIdColumn{
		DimName: flt.DimName,
		Op:      flt.Op,
		EnumIds: make([]int, len(flt.Enums)),
	}
	for k := range flt.Enums {
		id, err := cvt(flt.Enums[k])
		if err != nil {
			return "", err
		}
		fltId.EnumIds[k] = id
	}

	// return filter condition
	return makeDimIdFilter(modelDef, &fltId, dimName, typeOf, msgName)
}

// makeDimIdFilter return dimension filter condition for enum ids, eg: dim1 IN (1, 2, 3, 4)
// It is also can be equal or BETWEEN fitler.
func makeDimIdFilter(
	modelDef *ModelMeta, flt *FilterIdColumn, dimName string, typeOf *TypeMeta, msgName string) (string, error) {

	// validate number of enum ids in enum list
	if len(flt.EnumIds) <= 0 || flt.Op == EqOpFilter && len(flt.EnumIds) != 1 || flt.Op == BetweenOpFilter && len(flt.EnumIds) != 2 {
		return "", errors.New("invalid number of arguments to filter " + msgName + " dimension " + dimName)
	}

	emin := flt.EnumIds[0]
	emax := flt.EnumIds[len(flt.EnumIds)-1]
	if emin > emax {
		emin, emax = emax, emin
	}
	op := flt.Op

	// if filter condition is "auto" and only single enum supplied then use = equal filter
	// else use BETWEEN if all enum values between supplied min and max (no holes)
	// use IN filter by default
	if op == InAutoOpFilter {

		if len(typeOf.Enum) <= 0 {
			return "", errors.New("auto filter cannot be applied to " + msgName + " dimension " + dimName)
		}

		if len(flt.EnumIds) == 1 {
			op = EqOpFilter // single value: use equal
		} else { // multiple values: check if BETWEEN possible else use IN

			op = InOpFilter // use IN by default

			for _, e := range flt.EnumIds {
				if e < emin {
					emin = e
				}
				if e > emax {
					emax = e
				}
			}

			// check if all type enums between min and max (no holes)
			if len(typeOf.Enum) > 0 {

				isHole := true
				for k := 0; k < len(typeOf.Enum); k++ {

					if typeOf.Enum[k].EnumId < emin {
						continue
					}
					if typeOf.Enum[k].EnumId > emax {
						break
					}
					// between min and max
					isHole = true
					for _, e := range flt.EnumIds {
						if e == typeOf.Enum[k].EnumId {
							isHole = false
							break
						}
					}
					if isHole {
						break // current type enum id is not between min and max filter
					}
				}

				if !isHole {
					op = BetweenOpFilter // all type enum ids is between filter min and max enum ids
				}
			}

		}
	}

	// make dimension filter
	q := dimName
	switch op {
	case EqOpFilter: // AND dim1 = 2
		q += " = " + strconv.Itoa(flt.EnumIds[0])
	case InOpFilter: // AND dim1 IN (10, 20, 30)
		q += " IN ("
		for k, e := range flt.EnumIds {
			if k > 0 {
				q += ", "
			}
			q += strconv.Itoa(e)
		}
		q += ")"
	case BetweenOpFilter: // AND dim1 BETWEEN 100 AND 200
		q += " BETWEEN " + strconv.Itoa(emin) + " AND " + strconv.Itoa(emax)
	default:
		return "", errors.New("invalid filter operation to read " + msgName + " dimension " + dimName)
	}
	return q, nil
}
