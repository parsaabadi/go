// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"io/fs"
	"path/filepath"
	"time"

	"github.com/openmpp/go/ompp/config"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

const diskScanDefaultInterval = 383 * 1000 // timeout in msec, default value of sleep interval between scanning storage use
const minDiskScanInterval = 11 * 1000      // timeout in msec, minimum value to sleep between scanning storage use

// storage space use state
type diskUseState struct {
	IsOver        bool  // if true then storage use reach the limit
	diskUseConfig       // storage use settings
	AllSize       int64 // all oms instances size
	TotalSize     int64 // total size: models/bin size + download + upload
	BinSize       int64 // total models/bin size
	DbSize        int64 // total size of all db files
	DownSize      int64 // download total size
	UpSize        int64 // upload total size
	UpdateTs      int64 // info update time (unix milliseconds)
}

// model and database file info
type dbDiskUse struct {
	Digest string // model digest
	Size   int64  // bytes, if non zero then model database file size
	ModTs  int64  // db file mod time info update time (unix milliseconds)
}

// storage usage control settings
type diskUseConfig struct {
	ScanInterval int   // timeout in msec, sleep interval between scanning storage
	Limit        int64 // bytes, this instance storage limit
	AllLimit     int64 // bytes, total storage limit for all oms instances
}

/*
scan all models directories, download and upload directories to collect storage space usage.

Other file statistics also collected, e.g. files count, SQLite db file size for each model, etc.
*/
func scanDisk(doneC <-chan bool) {
	if !theCfg.isDiskUse {
		return // storage use control disabled
	}

	// if disk use file does not updated more than 3 times of scan interval (and minimum 1 minute) then oms instance is dead
	// disk use file: disk-#-_4040-#-size-#-100-#-status-#-ok-#-2022_07_08_23_45_12_123-#-125678.json
	diskUsePtrn := filepath.Join(theCfg.jobDir, "state", "disk-#-"+theCfg.omsName+"-#-size-#-*-#-status-#-*-#-*-#-*.json")

	// path to disk.ini: storage quotas and configuration
	diskIniPath := filepath.Join(theCfg.jobDir, "disk.ini")

	duState := diskUseState{
		diskUseConfig: diskUseConfig{
			ScanInterval: diskScanDefaultInterval,
		},
	}
	dbUse := []dbDiskUse{}

	for {
		isOk, cfg := initDiskState(diskIniPath)
		if !isOk {
			if isExitSleep(diskScanDefaultInterval, doneC) { // wait for doneC or sleep
				return
			}
			continue
		}
		// clean previous state
		nowTime := time.Now()
		nowTs := nowTime.UnixMilli()

		duState = diskUseState{
			diskUseConfig: cfg,
			UpdateTs:      nowTs,
		}

		// find all disk use files and calculate total disk usage by all other oms instances
		// if disk use file does not updated more than 3 times of scan interval (and minimum 1 minute) then oms instance is dead
		minuteTs := nowTime.Add(-1 * time.Minute).UnixMilli()
		minTs := nowTime.Add(-1 * 3 * time.Duration(cfg.ScanInterval) * time.Millisecond).UnixMilli()
		if minTs > minuteTs {
			minTs = minuteTs
		}
		var nOtherSize int64 // all other oms instances disk use size

		diskUseFiles := filesByPattern(diskUsePtrn, "Error at disk use files search")

		for _, fp := range diskUseFiles {

			oms, size, _, _, ts := parseDiskUseStatePath(fp)

			if oms == "" || oms == theCfg.omsName {
				continue // skip: invalid disk use state file path or it is current instance
			}
			if ts > minTs {
				nOtherSize += size // oms instance is alive
			}
		}

		// parseDiskUseStatePath

		// for all models get database file size and mod time
		mbs := theCatalog.allModels()

		if len(dbUse) != len(mbs) {
			dbUse = make([]dbDiskUse, len(mbs)) // model catalog updated
		}

		for k := 0; k < len(mbs); k++ {

			dbUse[k].Digest = mbs[k].model.Digest
			dbUse[k].Size = 0

			if st, e := fileStat(mbs[k].dbPath); e == nil { // skip on file errors
				dbUse[k].Size = st.Size()
				dbUse[k].ModTs = st.ModTime().UnixMilli()
			}
			duState.DbSize = duState.DbSize + dbUse[k].Size
		}

		// get total size of all files in the folder and sub-folders
		doTotalSize := func(folderPath string) int64 {

			var nTotal int64

			err := filepath.Walk(folderPath, func(path string, fi fs.FileInfo, err error) error {
				if err != nil {
					omppLog.Log("Error at directory walk: ", path, " : ", err.Error())
					return err
				}
				if !fi.IsDir() {
					nTotal = nTotal + fi.Size()
				}
				return nil
			})
			if err != nil {
				// omppLog.Log("Error at directory walk: ", folderPath, " :", err.Error())
			}
			return nTotal
		}

		// total size of models/bin directory, downlod and upload
		mDir, _ := theCatalog.getModelDir()
		duState.BinSize = doTotalSize(mDir)

		if theCfg.downloadDir != "" {
			duState.DownSize = doTotalSize(theCfg.downloadDir)
		}
		if theCfg.uploadDir != "" {
			duState.UpSize = doTotalSize(theCfg.uploadDir)
		}
		duState.TotalSize = duState.BinSize + duState.DownSize + duState.UpSize
		duState.AllSize = duState.TotalSize + nOtherSize

		// check if current disk usage reach the limit
		duState.IsOver = duState.Limit > 0 && duState.TotalSize >= cfg.Limit ||
			cfg.AllLimit > 0 && duState.AllSize >= cfg.AllLimit

		// update run catalog with current storage use state and save persistent part of the state
		theRunCatalog.updateDiskUse(&duState, dbUse)
		diskUseStateWrite(&duState, dbUse)

		// wait for doneC or sleep
		if isExitSleep(time.Duration(cfg.ScanInterval), doneC) {
			break
		}
	}
}

// update run catalog with current disk use state
func (rsc *RunCatalog) updateDiskUse(duState *diskUseState, dbUse []dbDiskUse) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	rsc.DiskUseState = *duState

	// copy db file info for current models list
	if len(rsc.DbDiskUse) != len(rsc.models) {
		rsc.DbDiskUse = make([]dbDiskUse, len(rsc.models))
	}

	k := 0
	for dgst := range rsc.models {

		rsc.DbDiskUse[k] = dbDiskUse{Digest: dgst}

		for j := 0; j < len(dbUse); j++ {
			if dbUse[j].Digest == dgst {
				rsc.DbDiskUse[k] = dbUse[j]
				break
			}
		}
		k++
	}
}

// return disk use config
func (rsc *RunCatalog) getDiskConfig() diskUseConfig {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	return rsc.DiskUseState.diskUseConfig
}

// return copy of current disk use state
func (rsc *RunCatalog) getDiskUse() (diskUseState, []dbDiskUse) {

	rsc.rscLock.Lock()
	defer rsc.rscLock.Unlock()

	duState := rsc.DiskUseState

	dbUse := make([]dbDiskUse, len(rsc.DbDiskUse))
	copy(dbUse, rsc.DbDiskUse)

	return duState, dbUse
}

// read job service state and computational servers definition from job.ini
func initDiskState(diskIniPath string) (bool, diskUseConfig) {

	cfg := diskUseConfig{ScanInterval: diskScanDefaultInterval}

	// read available resources limits and computational servers configuration from job.ini
	if diskIniPath == "" || !fileExist(diskIniPath) {
		return false, cfg
	}

	opts, err := config.FromIni(diskIniPath, theCfg.codePage)
	if err != nil {
		omppLog.Log(err)
		return false, cfg
	}

	cfg.ScanInterval = 1000 * opts.Int("Common.ScanInterval", diskScanDefaultInterval)
	if cfg.ScanInterval < minDiskScanInterval {
		cfg.ScanInterval = diskScanDefaultInterval // if too low then use default
	}
	cfg.AllLimit = 1024 * 1024 * 1024 * opts.Int64("Common.AllUsersLimit", 0) // total limit in bytes for all oms instances
	if cfg.AllLimit < 0 {
		cfg.AllLimit = 0 // unlimited
	}

	// find oms instance limit defined by name
	var uGb int64

	isOk := opts.IsExist(theCfg.omsName + ".UserLimit")
	if isOk {
		uGb = opts.Int64(theCfg.omsName+".UserLimit", 0) // limit defined for current instance name
	}

	if !isOk && opts.IsExist("Common.Groups") {

		gLst := helper.ParseCsvLine(opts.String("Common.Groups"), ',')

		for k := 0; !isOk && k < len(gLst); k++ {

			if !opts.IsExist(gLst[k]+".Users") || !opts.IsExist(gLst[k]+".UserLimit") {
				continue // skip: no users in that group or no user limit defined
			}
			uLst := helper.ParseCsvLine(opts.String(gLst[k]+".Users"), ',')

			for j := 0; !isOk && j < len(uLst); j++ {
				isOk = uLst[j] == theCfg.omsName // check if instance name exists in that group
			}
			if isOk {
				uGb = opts.Int64(gLst[k]+".UserLimit", 0) // group limit applied to the current instance
			}
		}
	}

	if !isOk {
		uGb = opts.Int64("Common.UserLimit", 0) // apply limit common for any user
	}
	if uGb < 0 {
		uGb = 0
	}
	cfg.Limit = 1024 * 1024 * 1024 * uGb // bytes, storage limit for current instance name

	return true, cfg
}
