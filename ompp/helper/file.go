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
	"strings"
)

const InvalidFilePathChars = "\"'`:*?><|$}{@&^;%"    // invalid or dangerous file path or URL characters
const InvalidFileNameChars = "\"'`:*?><|$}{@&^;%/\\" // invalid or dangerous file name or URL characters

// replace special file name characters: "'`:*?><|$}{@&^;/\ by _ underscore
func CleanFileName(src string) string {
	return strings.Map(
		func(r rune) rune {
			if strings.ContainsRune("\"'`:*?><|$}{@&^;/\\\\", r) {
				r = '_'
			}
			return r
		},
		src)
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
