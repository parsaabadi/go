// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"errors"
	"strconv"
)

// WriteLayout describes parameters or output tables values for insert or update.
// Name is a parameter or output table name to read, also for output table if ValueName is not empty
type WriteLayout struct {
	Name      string // parameter name or output table name
	ToId      int    // run id or set id to write parameter or output table values
	IsToRun   bool   // only for parameter: if true then write into into model run else into workset
	IsEditSet bool   // only for parameter: if true then workset must be editable (readonly = false) else must be readonly
}

// ReadLayout describes source and size of data page to read input parameters or output tables.
// Name is a parameter or output table name to read.
// For output table if ValueName is not empty then only accumulator or output expression
// with that name selected (i.e: "acc1" or "expr4") else all output table accumulators (expressions) selected.
type ReadLayout struct {
	Name      string          // parameter name or output table name
	ValueName string          // only for output table: if not empty then expression or accumulator name to select
	FromId    int             // run id or set id to select input parameter or output table values
	IsFromSet bool            // only for parameter: if true then select from workset else from model run
	IsEditSet bool            // only for parameter: if true then workset must be editable (readonly = false) else must be readonly
	IsAccum   bool            // only for output table: if true then select output table accumulator else expression
	Offset    int64           // first row to return from select, zero-based ofsset
	Size      int64           // max row count to select, if <= 0 then all rows
	Filter    []FilterColumn  // dimension filters; final WHERE join filters by AND
	OrderBy   []OrderByColumn // order by columnns, if empty then dimension id ascending order is used
}

// Select filter operations for dimension enum ids.
const (
	InAutoOpFilter  = iota // auto convert IN list filter into equal or BETWEEN if possible
	InOpFilter             // dimension enum ids in: dim2 IN (11, 22, 33)
	EqOpFilter             // dimension equal: dim1 = 12
	BetweenOpFilter        // dimension enum ids between: dim3 BETWEEN 44 AND 88
)

type FilterColumn struct {
	DimName string // dimension name
	Op      int    // filter operation: equal, IN, BETWEEN
	EnumIds []int  // enum ids: one, two or many ids depending on filter condition
}

// OrderBy define column to order by rows selected from parameter or output table.
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
	} // else
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

// makeDimFilter return dimension filter, eg: dim1 IN (1, 2, 3, 4)
// It is also can be equal or BETWEEN fitler
func makeDimFilter(
	modelDef *ModelMeta, flt *FilterColumn, dimName string, typeOf *TypeMeta, msgName string) (string, error) {

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
