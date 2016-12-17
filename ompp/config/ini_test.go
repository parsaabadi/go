// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIni(t *testing.T) {

	iniContent := `
;
; test ini-file
;
[first test]      ; section comment
val = no comment
rem = ; comment only and empty value
nul =

[replace]
k=1
k=
k=2
k=4

[escape]
	# next line is a real reason why ini-reading created: nothing exist to support following    
dsn = "DSN='server'; UID='user'; PWD='pas#word';" ; quoted value
	# escaping test
t w = the "# quick #" brown 'fox ; jumps' over    ; escaped: ; and # chars
" key "" 'quoted' here " = some value
qts = " allow ' unbalanced quotes                 ; with comment  
end = ; last line without cr/lf end of line `

	// create test.ini file
	iniPath := filepath.Join(os.TempDir(), "test.ompp.config.ini")
	f, err := os.Create(iniPath)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(iniPath)

	if _, err := f.WriteString(iniContent); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// load ini-file and compare content
	kvIni, err := NewIni(iniPath, "")
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
