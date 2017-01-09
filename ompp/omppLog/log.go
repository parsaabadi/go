// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
Package omppLog to println messages to standard output and log file.
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
package omppLog

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"go.openmpp.org/ompp/config"
	"go.openmpp.org/ompp/helper"
)

var (
	theLock       sync.Mutex                           // mutex to lock for log operations
	isFileEnabled bool                                 // if true then log to file enabled
	isFileCreated bool                                 // if true then log file created
	logPath       string                               // log file path
	lastYear      int                                  // if daily log then year of current daily stamp
	lastMonth     time.Month                           // if daily log then month current daily stamp
	lastDay       int                                  // if daily log then day current daily stamp
	logOpts       = config.LogOptions{IsConsole: true} // log options, default is log to console
)

// New log settings
func New(opts *config.LogOptions) {
	theLock.Lock()
	defer theLock.Unlock()

	if opts != nil {
		logOpts = *opts
	}
	isFileEnabled = logOpts.IsFile // file may be enabled but not created
	isFileCreated = false
}

// Log message to console and log file
func Log(msg ...interface{}) {
	theLock.Lock()
	defer theLock.Unlock()

	// make message string and log to console
	var m string
	now := time.Now()
	if logOpts.IsNoMsgTime {
		m = fmt.Sprint(msg...)
	} else {
		m = helper.MakeDateTime(now) + " " + fmt.Sprint(msg...)
	}
	if logOpts.IsConsole {
		fmt.Println(m)
	}

	// create log file if required log to file if file log enabled
	if isFileEnabled &&
		(!isFileCreated ||
			logOpts.IsDaily && (now.Year() != lastYear || now.Month() != lastMonth || now.Day() != lastDay)) {
		isFileCreated = createLogFile(now)
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
	now := time.Now()
	if isFileEnabled &&
		(!isFileCreated ||
			logOpts.IsDaily && (now.Year() != lastYear || now.Month() != lastMonth || now.Day() != lastDay)) {
		isFileCreated = createLogFile(now)
		isFileEnabled = isFileCreated
	}
	if isFileEnabled {
		isFileEnabled = writeToLogFile(helper.MakeDateTime(now) + " " + sql)
	}
}

// create log file or truncate if already exist, return false on errors to disable file log
func createLogFile(nowTime time.Time) bool {

	// make log file path as log settings file path with daily-stamp, if daily stamp required
	logPath = logOpts.LogPath

	if logOpts.IsDaily {
		dir, fName := filepath.Split(logPath)
		ext := filepath.Ext(fName)
		if ext != "" {
			fName = fName[:len(fName)-len(ext)]
		}
		lastYear = nowTime.Year()
		lastMonth = nowTime.Month()
		lastDay = nowTime.Day()
		logPath = filepath.Join(dir, fName+"_"+fmt.Sprintf("%04d%02d%02d", lastYear, lastMonth, lastDay)+ext)
	}

	// create log file or truncate existing
	f, err := os.Create(logPath)
	if err != nil {
		return false
	}
	defer f.Close()
	return true
}

// write message to log file, return false on errors to disable file log
func writeToLogFile(msg string) bool {

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0666)
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
