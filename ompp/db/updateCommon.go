// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"hash"
)

// digestCellsFrom append header to hash and return closure
// to add hash of cells (parameter values, accumulators or expressions) to digest.
// It is also return reference to bool flag to indicate all source rows are ordered by primary key.
// If there is no order by primery key then digest calculation is incorrect.
// It is a hash of text values identical to csv file hash, for example:
//   acc_id,sub_id,dim0,dim1,acc_value\n
//   0,1,0,0,1234.5678\n
func digestCellsFrom(hSum hash.Hash, modelDef *ModelMeta, name string, csvCvt CsvConverter) (func(interface{}) error, *bool, error) {

	isOrderBy := true // return true if rows ordered by primary key

	// append header, like: acc_id,sub_id,dim0,dim1,acc_value\n
	cs, err := csvCvt.CsvHeader(modelDef, name)
	if err != nil {
		return nil, &isOrderBy, err
	}
	for k := range cs {
		if k != 0 {
			if _, err = hSum.Write([]byte(",")); err != nil {
				return nil, &isOrderBy, err
			}
		}
		if _, err = hSum.Write([]byte(cs[k])); err != nil {
			return nil, &isOrderBy, err
		}
	}
	if _, err = hSum.Write([]byte("\n")); err != nil {
		return nil, &isOrderBy, err
	}

	// rows must be order by primary key, e.g.: acc_id, sub_id, dim0, dim1
	// for correct digest calculation
	// store previous row order by columns to check source rows order
	nOrder := len(cs) - 1
	prevKey := make([]int, nOrder)
	nowKey := make([]int, nOrder)
	isFirst := true

	keyCvt, err := csvCvt.KeyIds(name)
	if err != nil {
		return nil, &isOrderBy, err
	}

	// append dimensions and value to digest
	cvt, err := csvCvt.CsvToIdRow(modelDef, name) // converter from cell id's to csv row []string
	if err != nil {
		return nil, &isOrderBy, err
	}

	digestRow := func(src interface{}) error {

		// check row order by: if previous row key is less than current ror key
		if nOrder > 0 && isOrderBy {
			if isFirst {
				if e := keyCvt(src, prevKey); e != nil {
					return e
				}
				isFirst = false
			} else {
				if e := keyCvt(src, nowKey); e != nil {
					return e
				}
				for k := 0; isOrderBy && k < nOrder; k++ {
					isOrderBy = nowKey[k] >= prevKey[k]
					nowKey[k] = prevKey[k]
				}
			}
		}

		// convert to strings
		if err := cvt(src, cs); err != nil {
			return err
		}

		// append to digest
		for k := range cs {
			if k != 0 {
				if _, err = hSum.Write([]byte(",")); err != nil {
					return err
				}
			}
			if _, err = hSum.Write([]byte(cs[k])); err != nil {
				return err
			}
		}
		if _, err = hSum.Write([]byte("\n")); err != nil {
			return err
		}

		return nil
	}

	return digestRow, &isOrderBy, nil
}
