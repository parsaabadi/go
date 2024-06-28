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
		val, ok := kvIni[section+"."+key]
		if !ok {
			t.Errorf("not found [%s]:%s:", section, key)
		}
		if val != expected {
			t.Errorf("[%s]%s=%s: NOT :%s:", section, key, expected, val)
		}
	}

	checkString(`Test`, `non`, ``)
	checkString(`Test`, `rem`, ``)
	checkString(`Test`, `val`, `new value of no comments`)
	checkString(`Test`, `dsn`, `new value of UID='user'; PWD='secret';`)
	checkString(`Test`, `lst`, `new value of "the # quick" fox 'jumps # over'`)
	checkString(`Test`, `unb`, `"unbalanced quote                           ; this is not a comment: it is a value started from " quote`)

	checkString(`General`, `StartingSeed`, `16807`)
	checkString(`General`, `Subsamples`, `8`)
	checkString(`General`, `Cases`, `5000`)
	checkString(`General`, `SimulationEnd`, `100`)
	checkString(`General`, `UseSparse`, `true`)

	checkString(`multi`, `trim`, `Aname,Bname,Cname,DName`)
	checkString(`multi`, `keep`, `Multi line   text with spaces`)
	checkString(`multi`, `same`, `Multi line   text with spaces`)
	checkString(`multi`, `multi1`, `DSN='server'; UID='user'; PWD='secret';`)
	checkString(`multi`, `multi2`, `new value of "the # quick" fox "jumps # over"`)
	checkString(`multi`, `c-prog`, `C:\Program Files \Windows`)
	checkString(`multi`, `c-prog-win`, `C:\Program Files \Windows`)

	checkTheSame := func(section, key, keySame string) {
		v, ok := kvIni[section+"."+key]
		if !ok {
			t.Errorf("not found [%s]:%s:", section, key)
		}
		vSame, ok := kvIni[section+"."+key]
		if !ok {
			t.Errorf("not found [%s]:%s:", section, keySame)
		}
		if v != vSame {
			t.Errorf("NOT equal: [%s].%s and [%s].%s :%s: :%s:", section, key, section, keySame, v, vSame)
		}
	}
	checkTheSame(`multi`, `keep`, `same`)
	checkTheSame(`multi`, `c-prog`, `c-prog-win`)

	checkString(`replace`, `k`, `4`)

	checkString(`escape`, `dsn`, `DSN='server'; UID='user'; PWD='pas#word';`)
	checkString(`escape`, `t w`, `the "# quick #" brown 'fox ; jumps' over`)
	checkString(`escape`, ` key "" 'quoted' here `, `some value`)
	checkString(`escape`, `qts`, `" allow ' unbalanced quotes                 ; with comment`)

	checkString(`end`, `end`, ``)

	// check test coverage
	sk := []string{
		`Test.non`,
		`Test.rem`,
		`Test.val`,
		`Test.dsn`,
		`Test.lst`,
		`Test.unb`,
		`General.StartingSeed`,
		`General.Subsamples`,
		`General.Cases`,
		`General.SimulationEnd`,
		`General.UseSparse`,
		`multi.trim`,
		`multi.keep`,
		`multi.same`,
		`multi.multi1`,
		`multi.multi2`,
		`multi.c-prog`,
		`multi.c-prog-win`,
		`replace.k`,
		`escape.dsn`,
		`escape.t w`,
		`escape. key "" 'quoted' here `,
		`escape.qts`,
		`end.end`,
	}
	for key := range kvIni {
		isFound := false
		for k := range sk {
			if isFound = sk[k] == key; isFound {
				break
			}
		}
		if !isFound {
			t.Errorf("unexpected section.key found :%s:", key)
		}
	}
}
