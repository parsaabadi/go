// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package helper

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
)

// FromJsonFile reads read from json file and convert to destination pointer.
func FromJsonFile(jsonPath string, dst interface{}) (bool, error) {

	// open file and convert to utf-8
	f, err := os.Open(jsonPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // retrun: json file not exist
		}
		return false, errors.New("json file open error" + err.Error())
	}
	defer f.Close()

	// make utf-8 converter:
	// assume utf-8 as default encoding on any OS because json file must be unicode and cannot be "windows code page"
	rd, err := Utf8Reader(f, "utf-8")

	// decode json
	err = json.NewDecoder(rd).Decode(dst)
	if err != nil {
		if err == io.EOF {
			return false, nil // return "not exist" if json file empty
		}
		return false, errors.New("json decode error" + err.Error())
	}
	return true, nil
}

// FromJson restore from json string bytes and convert to destination pointer.
func FromJson(srcJson []byte, dst interface{}) (bool, error) {

	err := json.NewDecoder(bytes.NewReader(srcJson)).Decode(dst)
	if err != nil {
		if err == io.EOF {
			return false, nil // return "not exist" if json empty
		}
		return false, errors.New("json decode error" + err.Error())
	}
	return true, nil
}

// ToJsonFile convert source to json and write into jsonPath file.
func ToJsonFile(jsonPath string, src interface{}) error {

	f, err := os.OpenFile(jsonPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return errors.New("json file create error" + err.Error())
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(src)
	if err != nil {
		return errors.New("json encode error" + err.Error())
	}
	return nil
}

// ToJsonIndent return source conveted to json indeneted string.
func ToJsonIndent(src interface{}) (string, error) {

	srcJson, err := json.Marshal(src)
	if err != nil {
		return "", errors.New("json marshal error" + err.Error())
	}
	var srcIndent bytes.Buffer

	err = json.Indent(&srcIndent, srcJson, "", "  ")
	if err != nil {
		return "", errors.New("json indent error" + err.Error())
	}
	return srcIndent.String(), nil
}
