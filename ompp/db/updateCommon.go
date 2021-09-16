// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import (
	"container/list"
	"hash"
)

// digestCells add hash of cells (parameter values, accumulators or expressions) to hash.
// It is a hash of text values identical to csv file hash, for example:
//   acc_id,sub_id,dim0,dim1,acc_value\n
//   0,1,0,0,1234.5678\n
func digestCells(hSum hash.Hash, modelDef *ModelMeta, name string, csvCvt CsvConverter, cellLst *list.List) error {

	// append header, like: acc_id,sub_id,dim0,dim1,acc_value\n
	cs, err := csvCvt.CsvHeader(modelDef, name)
	if err != nil {
		return err
	}
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

	// append dimensions and value to digest
	cvt, err := csvCvt.CsvToIdRow(modelDef, name) // converter from cell id's to csv row []string
	if err != nil {
		return err
	}
	for c := cellLst.Front(); c != nil; c = c.Next() {

		// convert to strings
		if err := cvt(c.Value, cs); err != nil {
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
	}
	return nil
}
