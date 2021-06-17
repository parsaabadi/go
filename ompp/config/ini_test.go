// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package config

import (
	"testing"
)

func TestIni(t *testing.T) {

	// load ini-file and compare content
	kvIni, err := NewIni("testdata/test.ompp.config.ini", "")
	if err != nil {
		t.Fatal(err)
	}

	checkString := func(section, key, expected string) {
		val, ok := kvIni[iniKey(section, key)]
		if !ok {
			t.Errorf("not found [%s]:%s:", section, key)
		}
		if val != expected {
			t.Errorf("[%s]%s=%s: NOT :%s:", section, key, expected, val)
		}
	}

	checkString("first test", "val", "no comment")
	checkString("first test", "rem", "")
	checkString("first test", "nul", "")
	checkString("replace", "k", "4")
	checkString("escape", "dsn", `DSN='server'; UID='user'; PWD='pas#word';`)
	checkString("escape", "t w", `the "# quick #" brown 'fox ; jumps' over`)
	checkString("escape", ` key "" 'quoted' here `, `some value`)
	checkString("escape", "qts", `" allow ' unbalanced quotes`)
	checkString("escape", "end", "")
}
