// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
Package helper is a set common helper functions
*/
package helper

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// UnQuote trim spaces and remove "double" or 'single' quotes around string
func UnQuote(src string) string {
	s := strings.TrimSpace(src)
	if len(s) > 1 && (s[0] == '"' || s[0] == '\'') && s[0] == s[len(s)-1] {
		return s[1 : len(s)-1]
	}
	return s
}

// MakeDateTime return date-time string, ie: 2012-08-17 16:04:59.0148
func MakeDateTime(t time.Time) string {
	y, mm, dd := t.Date()
	h, mi, s := t.Clock()
	ms := int(time.Duration(t.Nanosecond()) / time.Millisecond)

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%04d", y, mm, dd, h, mi, s, ms)
}

// DeepCopy using gob to make a deep copy from src into dst, both src and dst expected to be a pointers
func DeepCopy(src interface{}, dst interface{}) error {
	var bt bytes.Buffer
	enc := gob.NewEncoder(&bt)
	dec := gob.NewDecoder(&bt)

	err := enc.Encode(src)
	if err != nil {
		return err
	}

	err = dec.Decode(dst)
	if err != nil {
		return err
	}
	return nil
}

// ParseKeyValue string of multiple key=value; pairs separated by semicolon.
// Key cannot be empty, value can be.
// Value can be escaped with "double" or 'single' quotes
func ParseKeyValue(src string) (map[string]string, error) {

	kv := make(map[string]string)
	var key string
	var isKey = true

	for src != "" {

		// split key= and value
		if isKey {
			// skip ; semicolon(s) and spaces in front of the key
			src = strings.TrimLeftFunc(src, func(c rune) bool {
				return c == ';' || unicode.IsSpace(c)
			})
			if src == "" {
				break // empty end of string, no more key=...
			}

			nEq := strings.IndexRune(src, '=')

			if nEq <= 0 {
				return nil, errors.New("expected key=... inside of key=value; string")
			}

			key = strings.TrimSpace(src[:nEq])
			if key == "" {
				return nil, errors.New("invalid (empty) key inside of key=value; string")
			}
			isKey = false
			src = src[nEq+1:] // key is found, skip =
			//continue
		}
		// expected begin of the value position

		// search for end of value ; semicolon, skip quoted part of value
		isQuote := false
		var cQuote rune
		for nPos, chr := range src {

			// if end of value as ; semicolon found
			if !isQuote && chr == ';' {

				// append result to the map, unquote "value" if quotes balanced
				kv[key] = UnQuote(src[:nPos])

				// value is found, skip ; semicolon and reset state
				src = src[nPos+1:]
				key = ""
				isKey = true
				break
			}

			// open or close quotes
			if !isQuote && (chr == '"' || chr == '\'') || isQuote && chr == cQuote {
				isQuote = !isQuote
				if isQuote {
					cQuote = chr // opening quote
				} else {
					cQuote = 0 // quote closed
				}
				continue
			}
		}
		// last key=value without ; semicolon at the end of line
		if !isKey && key != "" {
			kv[key] = UnQuote(src)
			break
		}
	}

	return kv, nil
}
