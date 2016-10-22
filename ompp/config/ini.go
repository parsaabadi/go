// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package config

import (
	"errors"
	"strconv"
	"strings"

	"go.openmpp.org/ompp/helper"
)

/*
NewIni read ini-file content into  map of (section.key)=>value.
It is very light and able to parse:
  dsn = "DSN='server'; UID='user'; PWD='pas#word';"   ; comments are # here

Section and key trimed and cannot contain comments ; or # chars inside.
Values trimed and unquoted, multi-line values not supported.
Value escaped with "double" or 'single' quotes can include spaces or ; or # chars

Example:
  ; comments can start from ; or
  # from # and empty lines are skipped

   [section test]  ; section comment
   val = no comment
   rem = ; comment only and empty value
   nul =
   dsn = "DSN='server'; UID='user'; PWD='pas#word';" ; quoted value
   t w = the "# quick #" brown 'fox ; jumps' over    ; escaped: ; and # chars
   qts = " allow ' unbalanced quotes                 ; with comment
*/
func NewIni(iniPath string, encodingName string) (map[string]string, error) {

	if iniPath == "" {
		return nil, nil // no ini-file
	}

	// read ini-file and convert to utf-8
	s, err := helper.FileToUtf8(iniPath, encodingName)
	if err != nil {
		return nil, errors.New("reading ini-file file to utf-8 failed: " + err.Error())
	}

	// parse ini-file into strings map of (section.key)=>value
	kvIni, err := loadIni(s)
	if err != nil {
		return nil, errors.New("reading ini-file failed: " + err.Error())
	}
	return kvIni, nil
}

// Parse ini-file content into strings map of (section.key)=>value
func loadIni(iniContent string) (map[string]string, error) {

	kvIni := make(map[string]string)
	var section, key, val string

	for nLine, nStart := 0, 0; nStart < len(iniContent); {

		// get current line and move to next
		nextPos := strings.IndexAny(iniContent[nStart:], "\r\n")
		if nextPos < 0 {
			nextPos = len(iniContent)
		}
		nextPos += 1 + nStart
		if nextPos > len(iniContent) {
			nextPos = len(iniContent)
		}

		line := strings.TrimSpace(iniContent[nStart:nextPos])
		nStart = nextPos
		nLine++

		// skip empty lines and ; comments and # Linux comments
		// empty line: at least k= or [] section expected, ignore shorter lines
		if len(line) < 1 || line[0] == ';' || line[0] == '#' {
			continue
		}

		// error if line too short: at least k= or [] section expected
		// error if no [section] found: only comments or empty lines can be before first section
		if len(line) < 2 {
			return nil, errors.New("line " + strconv.Itoa(nLine) + " too short")
		}
		if section == "" && line[0] != '[' {
			return nil, errors.New("line " + strconv.Itoa(nLine) + ": only comments or empty lines can be before first section")
		}

		// check if this is [section] with optional ; comments
		if line[0] == '[' {

			nEnd := strings.IndexRune(line, ']')
			nRem := strings.IndexAny(line, ";#")
			if nEnd < 2 || nRem > 0 && nRem < nEnd {
				return nil, errors.New("line " + strconv.Itoa(nLine) + ": invalid section name")
			}

			section = strings.TrimSpace(line[1:nEnd])
			continue // done with section header
		}
		if section == "" { // if no [section] found then skip until first section
			continue
		}

		// split key = and value ; with comment
		nEq := strings.IndexRune(line, '=')
		nRem := strings.IndexAny(line, ";#")
		if nEq < 1 || nRem > 0 && nRem < nEq {
			return nil, errors.New("line " + strconv.Itoa(nLine) + ": expected key=...")
		}
		key = strings.TrimSpace(line[:nEq])
		val = line[nEq+1:]

		// split value and ; optional # comment
		isQuote := false
		nRem = 0
		nQuote := 0
		var cQuote rune
		for k, c := range val {

			if c == ';' || c == '#' { // potential comment started
				nRem = k
				if !isQuote {
					break // comment outside of quotes
				}
				// else comment inside of quotes or after unbalanced quote started
			}

			if !isQuote && (c == '"' || c == '\'') || isQuote && c == cQuote { // open or close quotes
				isQuote = !isQuote
				nQuote = k
				if isQuote {
					cQuote = c // opening quote
				} else {
					cQuote = 0 // quote closed
				}
				continue
			}
		}
		if nRem > nQuote { // if comment after 'value' or after "unbalanced quotes then remove comment
			val = val[:nRem]
		}

		// append result to the map, unquote "value" if quotes balanced
		if section != "" && key != "" {
			kvIni[iniKey(section, key)] = helper.UnQuote(val)
		}
		key, val = "", "" // reset state
	}

	return kvIni, nil
}

// iniKey return ini-file key as concatenation: section.key
func iniKey(section, key string) string { return section + "." + key }
