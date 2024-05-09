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


// Convert multi-line values in input string into equivalent single line values.
func JoinMultiLineValues(input string) string {

    // Split input into separate lines on line breaks.
    lines := strings.Split(input, "\n")

    // Use some auxiliary slices to keep track of line related info. 
    // Record which lines are to be continued in here.
    // All boolean entries are initialized to false.
    lineIsContinued := make([]bool, len(lines))

    // Keep track of parity of single and double quotes.
    // All integer entries are initialized to zero.
    singleQuoteCount := make([]int, len(lines))
    doubleQuoteCount := make([]int, len(lines))

    // Store updated lines here.
    updatedLines := make([]string, len(lines))

    // And store concatenated lines here.
    var concatenatedLines []string

    for ix, line := range lines {
        // Initialize quote parity counts for current line.
        // If it's the first line or if previous line is not being 
        // continued then they're already set correctly to 0.

        // If it is not the first line and the previous line is being continued
        // initialize quote parity counts to those of the previous line. 
        if ix > 0 || lineIsContinued[ix - 1] {
            singleQuoteCount[ix] = singleQuoteCount[ix - 1]
            doubleQuoteCount[ix] = doubleQuoteCount[ix - 1]
        }

        // Iterate through characters in current line.
        for _, char := range line {
            // If it's a comment starting character.
            if char == '#' || char == ';' {
                // And if we're outside a quote block.
                if singleQuoteCount[ix] % 2 == 0 && doubleQuoteCount[ix] % 2 == 0 {
                    // Then it's the start of a comment and no line continuation character
                    // was encountered before it. So line is not continued. Break out of loop.
                    break

                // And if we're inside a quote block.
                } else {
                    // Then treat the comment starting character as part 
                    // of the quote and move to the next character.
                    continue
                }

            // If it's a single quote then update single quote count.
            } else if char == '\'' {
                singleQuoteCount[ix] += 1

            // If it's a double quote then update double quote count.
            } else if char == '"' {
                doubleQuoteCount[ix] += 1

            // If it's the line continuation character then mark that 
            // line as being continued and break out of character loop.
            } else if char == '\\' {
                lineIsContinued[ix] = true
                break

            // If it's any other character then move to the next character.
            } else {
                continue
            }
        }

        // If current line is being continued and the continuation character was outside of quote
        // blocks then we must remove contiguous whitespace leading the line continuation character.
        if lineIsContinued[ix] && singleQuoteCount[ix] % 2 == 0 && doubleQuoteCount[ix] % 2 == 0 {
            // If line is being continued, it will be on the first occurrence of 
            // the line continuation character so we can just use Cut and discard the rest.
            line, _, _ = strings.Cut(line, "\\")
            // And now trim any leading and trailing whitespace from that.
            line = strings.Trim(line, "\t ")
        }

        updatedLines[ix] = line
    }

    // Concatenate continued lines into single lines.
    var accumulator string
    for ix, line := range updatedLines {
        accumulator += line
        if !lineIsContinued[ix] {
            // Append the concatenated line stored in the accumulator.
            concatenatedLines = append(concatenatedLines, accumulator)
            // Reset accumulator.
            accumulator = ""
        }
    }

    // Fold the slice of lines into a single string again and return.
    return strings.Join(concatenatedLines, "\n")
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
