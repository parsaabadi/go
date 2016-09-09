// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package helper

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
)

// FromJsonFile reads read from json file and convert to destination pointer.
func FromJsonFile(jsonPath string, dst interface{}) (bool, error) {

	f, err := os.Open(jsonPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // retrun: json file not exist
		}
		return false, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	err = dec.Decode(dst)
	if err == io.EOF {
		return false, nil // return "not exist" if json file empty
	}
	return true, err
}

// FromJson restore from json string bytes and convert to destination pointer.
func FromJson(srcJson []byte, dst interface{}) (bool, error) {

	dec := json.NewDecoder(bytes.NewReader(srcJson))
	err := dec.Decode(dst)
	if err == io.EOF {
		return false, nil // return "not exist" if json empty
	}
	return true, err
}

// ToJsonFile convert source to json and write into jsonPath file.
func ToJsonFile(jsonPath string, src interface{}) error {

	f, err := os.OpenFile(jsonPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	return enc.Encode(src)
}

// ToJsonIndent return source conveted to json indeneted string.
func ToJsonIndent(src interface{}) (string, error) {

	srcJson, err := json.Marshal(src)
	if err != nil {
		return "", err
	}
	var srcIndent bytes.Buffer
	err = json.Indent(&srcIndent, srcJson, "", "  ")
	if err != nil {
		return "", err
	}
	return srcIndent.String(), nil
}
