// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package helper

import "testing"

func TestParseKeyValue(t *testing.T) {

	// parse key=value string compare content
	kvStr := `DSN=server;UID=user;PWD=password`

	kv, err := ParseKeyValue(kvStr)
	if err != nil {
		t.Errorf(err.Error())
	}

	checkString := func(key, expected string) {
		val, ok := kv[key]
		if !ok {
			t.Errorf("not found: %s", key)
		}
		if val != expected {
			t.Errorf("%s=%s: NOT :%s:", key, expected, val)
		}
	}
	checkString("DSN", "server")
	checkString("UID", `user`)
	checkString("PWD", `password`)

	// extra semicolons and spaces and complex 'single' and "double" quotes
	kvStr = ` ; ;DSN='server' ; ;;UID='us""''er'; PWD=' pas;word ';`

	kv, err = ParseKeyValue(kvStr)
	if err != nil {
		t.Errorf(err.Error())
	}
	checkString("DSN", "server")
	checkString("UID", `us""''er`)
	checkString("PWD", ` pas;word `)

	// empty key= at the end of line
	kvStr = `abc=`

	kv, err = ParseKeyValue(kvStr)
	if err != nil {
		t.Errorf(err.Error())
	}
	checkString("abc", "")

	// unbalanced quotes at the end of line
	kvStr = `abc="unbalanced quoutes ;    `

	kv, err = ParseKeyValue(kvStr)
	if err != nil {
		t.Errorf(err.Error())
	}
	checkString("abc", `"unbalanced quoutes ;`)
}
