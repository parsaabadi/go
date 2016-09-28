// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
Package config to merge run options: command line arguments and ini-file content.
Command line arguments take precedence over ini-file
*/
package config

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"go.openmpp.org/ompp/helper"
)

// Standard config keys to get values from ini-file or command line arguments
const (
	OptionsFile      = "OpenM.OptionsFile"    // ini-file path
	OptionsFileShort = "ini"                  // ini-file path (short form)
	DbConnectionStr  = "OpenM.Database"       // db connection string
	DbDriverName     = "OpenM.DatabaseDriver" // db driver name, ie: SQLite, odbc, sqlite3
	ModelName        = "OpenM.ModelName"      // model name
	ModelNameShort   = "m"                    // model name (short form)
	ModelDigest      = "OpenM.ModelDigest"    // model hash digest
	SetName          = "OpenM.SetName"        // workset name
	SetNameShort     = "s"                    // workset name (short form)
	SetId            = "OpenM.SetId"          // workset id, workset is a set of model input parameters
	RunName          = "OpenM.RunName"        // model run name
	RunId            = "OpenM.RunId"          // model run id
	TaskName         = "OpenM.TaskName"       // modeling task name
	TaskId           = "OpenM.TaskId"         // modeling task id
	ParamDir         = "OpenM.ParamDir"       // path to workset parameters directory
	ParamDirShort    = "p"                    // path to workset parameters directory (short form)
)

/* Log config keys.
Log can be enabled/disabled for two independent streams:
    console  => standard output stream
    log file => log file, truncated on every run, (optional) unique "stamped" name
"Stamped" file name produced by adding time-stamp and/or pid-stamp, i.e.:
    exeName.log => exeName_20120817_160459_0148.1234.log
*/
const (
	LogToConsole      = "OpenM.LogToConsole"    // if true then log to standard output
	LogToConsoleShort = "v"                     // if true then log to standard output (short form)
	LogToFile         = "OpenM.LogToFile"       // if true then log to file
	LogFilePath       = "OpenM.LogFilePath"     // log file path, default = current/dir/exeName.log
	LogUseTs          = "OpenM.LogUseTimeStamp" // if true then use time-stamp in log file name
	LogUsePid         = "OpenM.LogUsePidStamp"  // if true then use pid-stamp in log file name
	LogNoMsgTime      = "OpenM.LogNoMsgTime"    // if true then do not prefix log messages with date-time
	LogSql            = "OpenM.LogSql"          // if true then log sql statements
)

// RunOptions is (key,value) map of command line arguments and ini-file.
// For ini-file options key is combined as section.key
type RunOptions struct {
	KeyValue        map[string]string // (key=>value) from command line arguments and ini-file
	DefaultKeyValue map[string]string // default (key=>value), if non-empty default for command line argument
	iniPath         string            // path to ini-file
}

// LogOptions for console and log file output
type LogOptions struct {
	LogPath     string // path to log file
	IsConsole   bool   // if true then log to standard output, default: true
	IsFile      bool   // if true then log to file
	IsNoMsgTime bool   // if true then do not prefix log messages with date-time
	IsLogSql    bool   // if true then log sql statements
	TimeStamp   string // log timestamp string, ie: 20120817_160459_0148
}

// fullShort is pair of full option name and short option name
type fullShort struct {
	full  string // full option name
	short string // short option name
}

// standard pairs of full and short names
var optFs = []fullShort{
	fullShort{ModelName, ModelNameShort},
	fullShort{SetName, SetNameShort},
	fullShort{ParamDir, ParamDirShort}}

// New process command-line arguments and ini-file options.
func New() (*RunOptions, *LogOptions, error) {

	runOpts := &RunOptions{
		KeyValue:        make(map[string]string),
		DefaultKeyValue: make(map[string]string),
	}
	logOpts := &LogOptions{
		IsConsole: true,
		TimeStamp: makeTimeStamp(time.Now()),
	}

	addStandardFlags(runOpts, logOpts) // add "standard" config options

	// parse command line arguments and ini-file
	flag.Parse()

	kvIni, err := helper.NewIni(runOpts.iniPath)
	if err != nil {
		return nil, nil, err
	}
	if kvIni != nil {
		runOpts.KeyValue = kvIni
	}

	// override ini-file file values with command-line arguments
	flag.Visit(func(f *flag.Flag) {
		if f.Name == OptionsFile || f.Name == OptionsFileShort {
			runOpts.KeyValue[OptionsFile] = runOpts.iniPath
			return
		}
		if f.Name == LogToConsole || f.Name == LogToConsoleShort {
			runOpts.KeyValue[LogToConsole] = strconv.FormatBool(logOpts.IsConsole)
			return
		}
		for _, fs := range optFs {
			if f.Name == fs.full || f.Name == fs.short {
				runOpts.KeyValue[fs.full] = f.Value.String()
				return
			}
		}
		runOpts.KeyValue[f.Name] = f.Value.String()
	})

	// set default (key,value) from flag defaults if not empty
	flag.VisitAll(func(f *flag.Flag) {
		if f.DefValue == "" {
			return
		}
		n := f.Name
		if n == OptionsFileShort {
			n = OptionsFile
		}
		if n == LogToConsoleShort {
			n = LogToConsole
		}
		for _, fs := range optFs {
			if n == fs.short {
				n = fs.full
			}
		}
		if runOpts.DefaultKeyValue[n] == "" {
			runOpts.DefaultKeyValue[n] = f.DefValue
		}
	})

	// adjust log settings
	adjustLogOptions(runOpts, logOpts)
	return runOpts, logOpts, nil
}

// IsExist return true if key is defined as command line argument or ini-file option
func (opts *RunOptions) IsExist(key string) bool {
	if opts == nil || opts.KeyValue == nil {
		return false
	}
	_, ok := opts.KeyValue[key]
	return ok
}

// String return value by key.
// It can be defined as command line argument or ini-file option or command line default
func (opts *RunOptions) String(key string) string {
	val, _, _ := opts.StringExist(key)
	return val
}

// StringExist return value by key and boolean flags:
// isExist=true if value defined as command line argument or ini-file option,
// isDefault=true if value defined as non-empty default for command line argument.
func (opts *RunOptions) StringExist(key string) (val string, isExist, isDefaultArg bool) {
	if opts == nil || opts.KeyValue == nil {
		return "", false, false
	}
	if val, isExist = opts.KeyValue[key]; isExist {
		return val, isExist, false
	}

	val, isDefaultArg = opts.DefaultKeyValue[key]
	return val, false, isDefaultArg
}

// Bool return boolean value by key.
// If value not defined by command line argument or ini-file option
// or cannot be converted to boolean (see strconv.ParseBool) then return false
func (opts *RunOptions) Bool(key string) bool {
	sVal, isExist, _ := opts.StringExist(key)
	if !isExist || sVal == "" {
		return false
	}
	if val, err := strconv.ParseBool(sVal); err == nil {
		return val
	}
	return false
}

// Int return integer value by key.
// If value not defined by command line argument or ini-file option
// or cannot be converted to integer then default is returned
func (opts *RunOptions) Int(key string, defaultValue int) int {
	sVal, isExist, _ := opts.StringExist(key)
	if !isExist || sVal == "" {
		return defaultValue
	}
	if val, err := strconv.Atoi(sVal); err == nil {
		return val
	}
	return defaultValue
}

// Uint64 return unsigned 64 bit integer value by key.
// If value not defined by command line argument or ini-file option
// or cannot be converted to uint64 then default is returned
func (opts *RunOptions) Uint64(key string, defaultValue uint64) uint64 {
	sVal, isExist, _ := opts.StringExist(key)
	if !isExist || sVal == "" {
		return defaultValue
	}
	if val, err := strconv.ParseUint(sVal, 0, 64); err == nil {
		return val
	}
	return defaultValue
}

// Float return 64 bit float value by key.
// If value not defined by command line argument or ini-file option
// or cannot be converted to float64 then default is returned
func (opts *RunOptions) Float(key string, defaultValue float64) float64 {
	sVal, isExist, _ := opts.StringExist(key)
	if !isExist || sVal == "" {
		return defaultValue
	}
	if val, err := strconv.ParseFloat(sVal, 64); err == nil {
		return val
	}
	return defaultValue
}

// make timestamp string, ie: 20120817_160459_0148
func makeTimeStamp(t time.Time) string {
	y, mm, dd := t.Date()
	h, mi, s := t.Clock()
	ms := int(time.Duration(t.Nanosecond()) / time.Millisecond)

	return fmt.Sprintf("%04d%02d%02d_%02d%02d%02d_%04d", y, mm, dd, h, mi, s, ms)
}

// add "standard" config options to command line arguments
func addStandardFlags(runOpts *RunOptions, logOpts *LogOptions) {

	flag.StringVar(&runOpts.iniPath, OptionsFile, "", "path to `ini-file`")
	flag.StringVar(&runOpts.iniPath, OptionsFileShort, "", "path to `ini-file` (short of "+OptionsFile+")")

	// add log options to command line arguments
	flag.BoolVar(&logOpts.IsConsole, LogToConsole, true, "if true then log to standard output")
	flag.BoolVar(&logOpts.IsConsole, LogToConsoleShort, true, "if true then log to standard output (short of "+LogToConsole+")")
	flag.BoolVar(&logOpts.IsFile, LogToFile, false, "if true then log to file")
	flag.StringVar(&logOpts.LogPath, LogFilePath, "", "path to log file")
	_ = flag.Bool(LogUseTs, false, "if true then use time-stamp in log file name")
	_ = flag.Bool(LogUsePid, false, "if true then use pid-stamp in log file name")
	flag.BoolVar(&logOpts.IsNoMsgTime, LogNoMsgTime, false, "if true then do not prefix log messages with date-time")
	flag.BoolVar(&logOpts.IsLogSql, LogSql, false, "if true then log sql statements")
}

// adjust log settings by merging command line arguments and ini-file options
// make sure if LogToFile then log file path is defined and vice versa
// make "stamped" log file name, if required, by adding time-stamp and/or pid-stamp, i.e.:
//   exeName.log => exeName_20120817_160459_0148.1234.log
func adjustLogOptions(runOpts *RunOptions, logOpts *LogOptions) {

	// if log file path is not empty then LogToFile must be true
	if logOpts.LogPath != "" || logOpts.IsFile || runOpts.Bool(LogToFile) {
		logOpts.IsFile = true
		runOpts.KeyValue[LogToFile] = strconv.FormatBool(logOpts.IsFile)
	}

	// if LogToFile is true then log file path must not be empty
	if logOpts.IsFile && logOpts.LogPath == "" {

		logOpts.LogPath = runOpts.String(LogFilePath) // use log file path from ini-file

		//use exeName.log as default
		if logOpts.LogPath == "" {
			_, exeName := filepath.Split(os.Args[0])
			ext := filepath.Ext(exeName)
			if ext != "" {
				exeName = exeName[:len(exeName)-len(ext)]
			}
			logOpts.LogPath = exeName + ".log"
		}
	}

	// update log settings from merged command line arguments and ini-file
	logOpts.IsConsole = !runOpts.IsExist(LogToConsole) || runOpts.Bool(LogToConsole)
	logOpts.IsNoMsgTime = runOpts.Bool(LogNoMsgTime)
	logOpts.IsLogSql = runOpts.Bool(LogSql)

	// update file name with time stamp and pid stamp, if required:
	// exeName.log => exeName_20120817_160459_0148.1234.log
	isTs := logOpts.IsFile && runOpts.Bool(LogUseTs)
	isPid := logOpts.IsFile && runOpts.Bool(LogUsePid)

	if isTs || isPid {

		dir, fName := filepath.Split(logOpts.LogPath)
		ext := filepath.Ext(fName)
		if ext != "" {
			fName = fName[:len(fName)-len(ext)]
		}
		if isTs {
			fName += "_" + logOpts.TimeStamp
		}
		if isPid {
			fName += "." + strconv.Itoa(os.Getpid())
		}
		logOpts.LogPath = filepath.Join(dir, fName+ext)
	}
	runOpts.KeyValue[LogFilePath] = logOpts.LogPath // update value of log file name in run options
}
