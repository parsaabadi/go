// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
Package log to println messages to standard output and log file.
It is intended for progress or error logging and should not be used for profiling (it is slow).

Log can be enabled/disabled for two independent streams:
  console  => standard output stream
  log file => log file, truncated on every run, (optional) unique "stamped" name

"Stamped" file name produced by adding time-stamp and/or pid-stamp, i.e.:
  exeName.log => exeName_20120817_160459_0148.1234.log

Log message by default prefixed with date-time: 2012-08-17 16:04:59.0148 ....
It can be disabled by log setting "is no msg time" = true, i.e.:
  exeName -v -OpenM.LogNoMsgTime
*/
package log

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/helper"
)

// log options, default is log to console
var logOpts = config.LogOptions{IsConsole: true}

var (
	theLock       sync.Mutex // mutex to lock for log operations
	isFileEnabled bool       // if true then log to file enabled
	isFileCreated bool       // if true then log file created
)

// New log settings
func New(opts *config.LogOptions) {
	theLock.Lock()
	defer theLock.Unlock()

	logOpts = *opts
	isFileEnabled = logOpts.IsFile // file may be enabled but not created
	isFileCreated = false
}

// Log message to console and log file
func Log(msg ...interface{}) {
	theLock.Lock()
	defer theLock.Unlock()

	// make message string and log to console
	var m string
	if logOpts.IsNoMsgTime {
		m = fmt.Sprint(msg...)
	} else {
		m = helper.MakeDateTime(time.Now()) + " " + fmt.Sprint(msg...)
	}
	if logOpts.IsConsole {
		fmt.Println(m)
	}

	// create log file if required log to file if file log enabled
	if isFileEnabled && !isFileCreated {
		isFileCreated = createLogFile()
		isFileEnabled = isFileCreated
	}
	if isFileEnabled {
		isFileEnabled = writeToLogFile(m)
	}
}

// LogSql sql query to file
func LogSql(sql string) {
	theLock.Lock()
	defer theLock.Unlock()

	if !logOpts.IsLogSql { // exit if log sql not enabled
		return
	}

	// create log file if required log to file if file log enabled
	if isFileEnabled && !isFileCreated {
		isFileCreated = createLogFile()
		isFileEnabled = isFileCreated
	}
	if isFileEnabled {
		isFileEnabled = writeToLogFile(helper.MakeDateTime(time.Now()) + " " + sql)
	}
}

// create log file or truncate if already exist, return false on errors to disable file log
func createLogFile() bool {
	f, err := os.Create(logOpts.LogPath)
	if err != nil {
		return false
	}
	defer f.Close()
	return true
}

// write message to log file, return false on errors to disable file log
func writeToLogFile(msg string) bool {

	f, err := os.OpenFile(logOpts.LogPath, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return false
	}
	defer f.Close()

	_, err = f.WriteString(msg)
	if err == nil {
		if runtime.GOOS == "windows" { // adjust newline for windows
			_, err = f.WriteString("\r\n")
		} else {
			_, err = f.WriteString("\n")
		}
	}
	return err == nil
}
