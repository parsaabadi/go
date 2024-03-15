// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
Package helper is a set common helper functions
*/
package helper

import (
	"errors"
	"io"
	"os"
	"regexp"
)

// CleanPath replace special file path characters: "'`:*?><|$}{@&^;/\ by _ underscore
func CleanPath(src string) string {
	re := regexp.MustCompile("[\"'`:*?><|$}{@&^;/\\\\]")
	return re.ReplaceAllString(src, "_")
}

// SaveTo copy all from source reader into new outPath file. File truncated if already exists.
func SaveTo(outPath string, rd io.Reader) error {

	// create or truncate output file
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// copy request body into the file
	_, err = io.Copy(f, rd)
	return err
}

// return true if path exists and it is directory, return error if path is not a directory or not accessible
func IsDirExist(dirPath string) (bool, error) {
	if dirPath == "" {
		return false, nil
	}
	fi, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.New("Error: unable to access directory: " + dirPath + " : " + err.Error())
	}
	if !fi.IsDir() {
		return false, errors.New("Error: directory expected: " + dirPath)
	}
	return true, nil
}
