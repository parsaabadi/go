// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package config

import (
	"errors"
	"strconv"
	"strings"

	"github.com/openmpp/go/ompp/helper"
)

/*
NewIni read ini-file content into  map of (section.key)=>value.

It is very light and able to parse:

	dsn = "DSN='server'; UID='user'; PWD='pas#word';"   ; comments are # here

Section and key are trimed and cannot contain comments ; or # chars inside.
Key and values trimed and "unquoted".
Key or value escaped with "double" or 'single' quotes can include spaces or ; or # chars
Multi-line values are NOT supported, no line continuation.

Example:

	; comments can start from ; or
	# from # and empty lines are skipped

	 [section test]  ; section comment
	 val = no comment
	 rem = ; comment only and empty value
	 nul =
	 dsn = "DSN='server'; UID='user'; PWD='pas#word';" ; quoted value
	 t w = the "# quick #" brown 'fox ; jumps' over    ; escaped: ; and # chars
	 " key "" 'quoted' here " = some value
	 qts = " allow ' unbalanced quotes                 ; with comment
*/
func NewIni(iniPath string, encodingName string) (map[string]string, error) {

	if iniPath == "" {
		return nil, nil // no ini-file
	}

	// read ini-file and convert to utf-8
	s, err := helper.FileToUtf8(iniPath, encodingName)
	if err != nil {
		return nil, errors.New("reading ini-file to utf-8 failed: " + err.Error())
	}

	// parse ini-file into strings map of (section.key)=>value
	kvIni, err := loadIni(s)
	if err != nil {
		return nil, errors.New("reading ini-file failed: " + err.Error())
	}
	return kvIni, nil
}

// iniKey return ini-file key as concatenation: section.key
func iniKey(section, key string) string { return section + "." + key }

// Parse ini-file content into strings map of (section.key)=>value
func loadIni(iniContent string) (map[string]string, error) {

    // We're trying to hack a parser extension without doing too many changes.
    // Try to parse out all line continuation characters and merge all continuated lines.
    // Then pass off the result to the existing code.

    // Value can take multiple lines with \ at the end of the line for continuation.
    // Comments are optional and can start from either semicolon or hash sign at any position of the line. 
    // You can escape comment separator by putting value in single 'apostrophes' or double "quotes".
    // No rules are mentioned regarding line continuation character occurring in comments. 

    // Complication: For values not contained inside double or single quotes, 
    // leading whitespace before a line continuation character must be truncated.

    // Another complication: We may be inside a quote block that was initiated on a previous line.
    // So we need to carry over any parity checks on quotes from preceeding lines over to current line.

    // First split input into seperate lines.
    lines := strings.Split(iniContent, "\n")

    // Use corresponding slice to mark if line is being continued.
    continuations := make([]bool, len(lines))

    // And to record parity of single and double quotes.
    singleQuoteParity := make([]int, len(lines))
    doubleQuoteParity := make([]int, len(lines))

    for ix, line := range lines {
        // Split line on first occurrence of a line continuation character. 
        line, _, isContinued := strings.Cut(line, "\\")

        // If line is not continued:
        if !isContinued {
            // Set continuations and quote parities to false and 0.
            continuations[ix] = false
            singleQuoteParity[ix] = 0
            doubleQuoteParity[ix] = 0

            // And move on to next line.
            continue
        }

        // Otherwise:
        continuations[ix] = true

        // Determine if line continuation character is inside an open quote block.
        // Count the number of occurrences of single and double quotes in prefix.
        singleQuoteParity[ix] = strings.Count(line, "\'") % 2
        doubleQuoteParity[ix] = strings.Count(line, "\"") % 2

        // If it's not the first line then account for parity of previous line.
        if ix > 0 {
            singleQuoteParity[ix] = (singleQuoteParity[ix] + singleQuoteParity[ix - 1]) % 2
            doubleQuoteParity[ix] = (doubleQuoteParity[ix] + doubleQuoteParity[ix - 1]) % 2
        }

        // If line continuation character was outside of quotation blocks then
        // remove contiguous whitespace leading the line continuation character.
        if singleQuoteParity[ix] % 2 && doubleQuoteParity[ix] % 2 {
            line = strings.TrimRight(line, "\t ")
        }

        // * If it doesn't allow us to update lines in place then create another slice *
        lines[ix] = line
    }

    // Concatenate continuated lines into single lines.


    // Finally fold the slice of lines into a single string again and pass on to the existing logic.




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

		// get key: find first = outside of "quote" or 'single quote'
		isQuote := false
		var cQuote rune
		nEq := 0
		for k, c := range line {

			if !isQuote && (c == '"' || c == '\'') || isQuote && c == cQuote { // open or close quotes
				isQuote = !isQuote
				if isQuote {
					cQuote = c // opening quote
				} else {
					cQuote = 0 // quote closed
				}
				continue
			}
			if !isQuote && c == '=' { // if outside of quote: check key=
				nEq = k
				break // found end of key=
			}
			if !isQuote && (c == ';' || c == '#') { // comment outside of quotes
				break
			}
		}
		if nEq < 1 || nEq >= len(line) {
			return nil, errors.New("line " + strconv.Itoa(nLine) + ": expected key=...")
		}

		// split key = and value ; with comment
		key = helper.UnQuote(line[:nEq])
		val = line[nEq+1:]

		// split value and ; optional # comment
		isQuote = false
		cQuote = 0
		nQuote := 0
		nRem := 0
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
