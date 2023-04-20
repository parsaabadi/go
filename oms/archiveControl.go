// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/openmpp/go/ompp/db"
	"github.com/openmpp/go/ompp/helper"
	"github.com/openmpp/go/ompp/omppLog"
)

const archiveScanInterval = 16381                  // timeout in msec, sleep interval between scanning of archive state
const archiveNewScanInterval = 23 * 60 * 60 * 1000 // timeout in msec to find new data for archive

const archiveStateFile = "archive-state.json" // archive state file name
const archiveStateMaxErr = 17                 // max number of errors during archive state write to disable archiving

const archiveRunKeepAll = "KEEP-ALL-RUNS" // special model run name: keep all model runs in database, no archiving
const archiveSetKeepAll = "KEEP-ALL-SETS" // special workset name: keep all worksets in database, no archiving

// archive catalog and state
type archiveCatalog struct {
	theLock sync.Mutex // mutex to lock for archive state operations
}

var theArchive archiveCatalog // archive catalog and state

type archiveState struct {
	IsArchive       bool             // if true the archiving is enabled: old data moved out from into archive directory
	ArchiveDays     int              // number of days before achiving model runs, input sets, downloads and uploads
	AlertDays       int              // number of days to issue archive alert to the user
	ArchiveDateTime string           // archive cut-off date-time
	AlertDateTime   string           // alert cut-off date-time
	UpdateDateTime  string           // last state update date-time
	Model           []archiveRequest // model archive requests
}

// archive request: list of model runs and worksets to be copied into text files and deleted from database
type archiveRequest struct {
	ModelDigest string          // model digest
	ModelName   string          // model name
	Run         []db.RunPub     // model runs to archive now
	Set         []db.WorksetPub // worksets to archive now
	RunAlert    []db.RunPub     // model runs to be archived soon
	SetAlert    []db.WorksetPub // worksets  to be archived soon
}

// scan model runs, worksets, downloads and uploads
// if model runs or workset is older than oms.ArchiveDays then move it from database into archive directory
// if downloads or uploads or archives is older than oms.ArchiveDays then delete it
func scanArchive(doneC <-chan bool) {
	if !theCfg.isArchive {
		return // archiving disabled
	}

	arcLst := []archiveRequest{} // archive requests for each model

	var lastNewScanTs int64 // unix msec, last time of scan for new archive request
	var nextStateTs int64   // unix msec, next time to write archive state
	tState := time.Now()    // last state update time
	stateErrCount := 0      // error count at write archive state
	isNewState := false     // if true then archive state updated
	arcDt := ""             // archive date-time cut off
	alertDt := ""           // alert about archive date-time cut off

	for {
		// remove from archive request list old models which are no longer in main model catalog
		mLst := theCatalog.allModels()
		{
			j := 0
			for k := range arcLst {

				ok := false
				for i := range mLst {
					ok = arcLst[k].ModelDigest == mLst[i].digest
					if ok {
						break
					}
				}
				if ok {
					arcLst[j] = arcLst[k]
					j++
				}
			}
			arcLst = arcLst[:j]
		}

		// scan for new items to archive
		tNow := time.Now()
		nowTs := tNow.UnixMilli()

		if lastNewScanTs+archiveNewScanInterval < nowTs {

			lastNewScanTs = nowTs
			nextStateTs = nowTs
			tState = tNow
			isNewState = true

			arcDt = helper.MakeDateTime(tNow.AddDate(0, 0, -theCfg.archiveDays))
			alertDt = helper.MakeDateTime(tNow.AddDate(0, 0, -theCfg.archiveAlertDays))

			arcLst = updateArchiveState(arcDt, alertDt, mLst, arcLst)
		}

		// create archive state file
		if isNewState && nextStateTs <= nowTs {

			isNewState := !theArchive.createArchiveState(tState, arcDt, alertDt, arcLst)
			if !isNewState {
				nextStateTs = nowTs + archiveNewScanInterval
				stateErrCount = 0
			} else {
				nextStateTs = nowTs + archiveScanInterval
				stateErrCount++
			}

			// too many errors: disable archiving after multiple errors and exits archive scan job
			if stateErrCount > archiveStateMaxErr {
				omppLog.Log("Error: archiving disabled after multiple errors at writing into: ", theCfg.archiveStatePath)
				break
			}
		}

		// wait for doneC or sleep
		if isExitSleep(archiveScanInterval, doneC) {
			return
		}
	}

	theCfg.isArchive = false
}

// update archive requests state: fins model runs and worksets for archiving
func updateArchiveState(arcDt, alertDt string, modelLst []modelBasic, arcLst []archiveRequest) []archiveRequest {

	// find model runs or worksets where update time older than archive period and add new request to archive request list
	for _, mdl := range modelLst {

		// find list of model runs to keep and list worksest to keep
		rKeep := []string{}
		for md, lst := range theCfg.archiveRunKeep {
			if md == mdl.digest {
				rKeep = lst
				break
			}
		}
		wKeep := []string{}
		for md, lst := range theCfg.archiveSetKeep {
			if md == mdl.digest {
				wKeep = lst
				break
			}
		}

		// find existing or append new archive request, clear current archive lists of runs and worksets and allert lists
		nRq := -1
		for k := range arcLst {
			if arcLst[k].ModelDigest == mdl.digest {
				nRq = k
				break
			}
		}
		if nRq < 0 || nRq >= len(arcLst) { // model not found: add new archive request

			nRq = len(arcLst)
			arcLst = append(arcLst,
				archiveRequest{
					ModelDigest: mdl.digest,
					ModelName:   mdl.name,
					Run:         []db.RunPub{},
					Set:         []db.WorksetPub{},
					RunAlert:    []db.RunPub{},
					SetAlert:    []db.WorksetPub{},
				})
		} else { // clear existing lists of model runs and worksets

			n := cap(arcLst[nRq].Run)
			if n < cap(arcLst[nRq].RunAlert) {
				n = cap(arcLst[nRq].RunAlert)
			}
			arcLst[nRq].Run = make([]db.RunPub, 0, n)
			arcLst[nRq].RunAlert = make([]db.RunPub, 0, n)

			n = cap(arcLst[nRq].Set)
			if n < cap(arcLst[nRq].SetAlert) {
				n = cap(arcLst[nRq].SetAlert)
			}
			arcLst[nRq].Set = make([]db.WorksetPub, 0, n)
			arcLst[nRq].SetAlert = make([]db.WorksetPub, 0, n)
		}

		// get list of model runs and filter it
		// to append to the list of runs to be archived now and to be archived soon (user alert list)
		rl, ok := theCatalog.RunPubList(mdl.digest)
		if ok && len(rl) > 0 {

			for k := range rl {

				// skip first (base) model run
				// skip model runs which are in runs to keep list
				ok = k == 0
				for i := 0; !ok && i < len(rKeep); i++ {
					ok = rKeep[i] == archiveRunKeepAll || rKeep[i] == rl[k].RunDigest || rKeep[i] == rl[k].RunStamp || rKeep[i] == rl[k].Name
				}
				if ok {
					continue
				}

				// append this run to request list if it is older than archive time and or alert time
				if rl[k].UpdateDateTime < arcDt {
					arcLst[nRq].Run = append(arcLst[nRq].Run, rl[k])
				} else {
					if rl[k].UpdateDateTime < alertDt {
						arcLst[nRq].RunAlert = append(arcLst[nRq].RunAlert, rl[k])
					}
				}
			}
		}

		// get list of model worksets and filter it
		// to append to the list of worksets to be archived now and to be archived soon (user alert list)
		wl, ok := theCatalog.WorksetPubList(mdl.digest)
		if ok && len(wl) > 0 {

			for k := range wl {

				// skip first (default) model workset
				// skip read-write worksets (even it is unlikely edit in progress)
				// skip model worksets which are in worksets to keep list
				ok = k == 0 || !wl[k].IsReadonly
				for i := 0; !ok && i < len(wKeep); i++ {
					ok = wKeep[i] == archiveSetKeepAll || wKeep[i] == wl[k].Name
				}
				if ok {
					continue
				}

				// append this workset to request list if it is older than archive time and or alert time
				if wl[k].UpdateDateTime < arcDt {
					arcLst[nRq].Set = append(arcLst[nRq].Set, wl[k])
				} else {
					if wl[k].UpdateDateTime < alertDt {
						arcLst[nRq].SetAlert = append(arcLst[nRq].SetAlert, wl[k])
					}
				}
			}
		}
	}

	return arcLst
}

// return empty value of archive job state
func emptyArchiveState() archiveState {

	nowDt := helper.MakeDateTime(time.Now())

	return archiveState{
		IsArchive:       theCfg.isArchive,
		ArchiveDays:     theCfg.archiveDays,
		AlertDays:       theCfg.archiveAlertDays,
		ArchiveDateTime: nowDt,
		AlertDateTime:   nowDt,
		UpdateDateTime:  nowDt,
		Model:           []archiveRequest{},
	}
}

// write archive job state into state file, return true on success
func (ac *archiveCatalog) createArchiveState(tState time.Time, arcDt, alertDt string, arcLst []archiveRequest) bool {

	if !theCfg.isArchive || theCfg.archiveStatePath == "" { // archiving disabled
		return false
	}

	// create archive state, include only models where request list is not empty
	st := archiveState{
		IsArchive:       theCfg.isArchive,
		ArchiveDays:     theCfg.archiveDays,
		AlertDays:       theCfg.archiveAlertDays,
		ArchiveDateTime: arcDt,
		AlertDateTime:   alertDt,
		UpdateDateTime:  helper.MakeDateTime(tState),
		Model:           []archiveRequest{},
	}

	for k := range arcLst {
		if len(arcLst[k].Run) > 0 || len(arcLst[k].Set) > 0 || len(arcLst[k].RunAlert) > 0 || len(arcLst[k].SetAlert) > 0 {
			st.Model = append(st.Model, arcLst[k])
		}
	}

	// lock archive state and write state into the state file
	ac.theLock.Lock()
	defer ac.theLock.Unlock()

	err := helper.ToJsonIndentFile(theCfg.archiveStatePath, &st)
	if err != nil {
		omppLog.Log(err)
		fileDeleteAndLog(true, theCfg.archiveStatePath) // on error remove file, if any file created
		return false
	}

	return true
}

// read archive job state from file, return empty state if archiving disabled
func (ac *archiveCatalog) readArchiveState() ([]byte, error) {

	// retrun empty state if archiving disabled
	if !theCfg.isArchive || theCfg.archiveStatePath == "" { // archiving disabled
		return json.Marshal(emptyArchiveState())
	}

	// lock archive state and read from state file
	ac.theLock.Lock()
	defer ac.theLock.Unlock()

	return os.ReadFile(theCfg.archiveStatePath)
}
