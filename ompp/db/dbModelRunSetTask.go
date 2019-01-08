// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

// Model run part of database: run, workset, task, profile.
//
// That portion of model database updated during model run and should not be cached.

// Model run status (run_lst table) and modeling task run status (task_run_lst table):
//   if task status = w (wait) then
//      model wait and NOT completed until other process set status to one of finals: s,x,e
//      model check if any new sets inserted into task_set and run it as they arrive
const (
	InitRunStatus     = "i" // i = initial status
	ProgressRunStatus = "p" // p = run in progress
	WaitRunStatus     = "w" // w = wait: run in progress, under external supervision
	DoneRunStatus     = "s" // s = completed successfully
	ExitRunStatus     = "x" // x = exit and not completed
	ErrorRunStatus    = "e" // e = error failure
)

// RunMeta is metadata of model run: name, status, run options, description, notes.
type RunMeta struct {
	Run      RunRow            // model run master row: run_lst
	Txt      []RunTxtRow       // run text rows: run_txt
	Opts     map[string]string // options used to run the model: run_option
	Param    []runParam        // run parameters: parameter_hid, sub-value count, run_parameter_txt table rows
	Progress []RunProgress     // run progress by sub-values: run_progress table rows
}

// RunPub is "public" model run metadata for json import-export
type RunPub struct {
	ModelName      string            // model name for that run
	ModelDigest    string            // model digest for that run
	Name           string            // run_name      VARCHAR(255) NOT NULL
	SubCount       int               // sub_count     INT          NOT NULL, -- subvalue count
	SubStarted     int               // sub_started   INT          NOT NULL, -- number of subvalues started
	SubCompleted   int               // sub_completed INT          NOT NULL, -- number of subvalues completed
	CreateDateTime string            // create_dt     VARCHAR(32)  NOT NULL, -- start date-time
	Status         string            // status        VARCHAR(1)   NOT NULL, -- run status: i=init p=progress s=success x=exit e=error(failed)
	UpdateDateTime string            // update_dt     VARCHAR(32)  NOT NULL, -- last update date-time
	Digest         string            // run_digest    VARCHAR(32)  NULL,     -- digest of the run
	Txt            []DescrNote       // run text: description and notes by language
	Opts           map[string]string // options used to run the model: run_option
	Param          []ParamRunSetPub  // run parameters: name, sub-value count and value notes by language
	Progress       []RunProgress     // run progress by sub-values: run_progress table rows
}

// ParamRunSetPub is "public" run or workset parameter metadata for json import-export
type ParamRunSetPub struct {
	Name     string     // parameter name
	SubCount int        // number of parameter sub-values
	Txt      []LangNote // parameter value notes by language
}

// RunRow is model run row: run_lst table row.
//
// Run status: i=init p=progress s=success x=exit e=error(failed).
// Run id must be different from working set id (use id_lst to get it)
type RunRow struct {
	RunId          int    // run_id        INT          NOT NULL, -- unique run id
	ModelId        int    // model_id      INT          NOT NULL
	Name           string // run_name      VARCHAR(255) NOT NULL, -- model run name
	SubCount       int    // sub_count     INT          NOT NULL, -- subvalue count
	SubStarted     int    // sub_started   INT          NOT NULL, -- number of subvalues started
	SubCompleted   int    // sub_completed INT          NOT NULL, -- number of subvalues completed
	CreateDateTime string // create_dt     VARCHAR(32)  NOT NULL, -- start date-time
	Status         string // status        VARCHAR(1)   NOT NULL, -- run status: i=init p=progress s=success x=exit e=error(failed)
	UpdateDateTime string // update_dt     VARCHAR(32)  NOT NULL, -- last update date-time
	Digest         string // run_digest    VARCHAR(32)  NULL,     -- digest of the run
}

// RunTxtRow is db row of run_txt
type RunTxtRow struct {
	RunId    int    // run_id    INT          NOT NULL
	LangCode string // lang_code VARCHAR(32)  NOT NULL
	Descr    string // descr     VARCHAR(255) NOT NULL
	Note     string // note      VARCHAR(32000)
}

// RunParam is a holder for run parameter Hid, subvalue count and run_parameter_txt rows
type runParam struct {
	ParamHid int              // parameter_hid INT NOT NULL
	SubCount int              // number of parameter sub-values
	Txt      []RunParamTxtRow // run_parameter_txt table rows
}

// RunParamTxtRow is db row of run_parameter_txt
type RunParamTxtRow struct {
	RunId    int    // run_id        INT         NOT NULL
	ParamHid int    // parameter_hid INT         NOT NULL
	LangCode string // lang_code     VARCHAR(32) NOT NULL
	Note     string // note          VARCHAR(32000)
}

// RunProgress is a "public" sub-value run_progress db row
type RunProgress struct {
	SubId          int     // sub_id         INT         NOT NULL, -- sub-value id (zero based index)
	CreateDateTime string  // create_dt      VARCHAR(32) NOT NULL, -- start date-time
	Status         string  // status         VARCHAR(1)  NOT NULL, -- run status: i=init p=progress s=success x=exit e=error(failed)
	UpdateDateTime string  // update_dt      VARCHAR(32) NOT NULL, -- last update date-time
	Count          int     // progress_count INT         NOT NULL, -- progress count: percent completed
	Value          float64 // progress_value FLOAT       NOT NULL, -- progress value: number of cases (case based) or time (time based)
}

// runProgressRow is db row of run_progress
type runProgressRow struct {
	RunId    int         // run_id         INT         NOT NULL
	Progress RunProgress // sub-value run progress
}

// WorksetMeta is a model workset metadata: name, parameters, decription, notes.
//
// Workset (working set of model input parameters):
// it can be a full set, which include all model parameters
// or subset and include only some parameters.
//
// Each model must have "default" workset.
// Default workset must include ALL model parameters (it is a full set).
// Default workset is a first workset of the model: set_id = min(set_id).
// If workset is a subset (does not include all model parameters)
// then it can be based on model run results, specified by run_id (not NULL).
//
// Workset can be editable or read-only.
// If workset is editable then you can modify input parameters or workset description, notes, etc.
// If workset is read-only then you can run the model using that workset as input.
//
// Important: working set_id must be different from run_id (use id_lst to get it)
// Important: always update parameter values inside of transaction scope
// Important: before parameter update do is_readonly = is_readonly + 1 to "lock" workset
//
// WorksetMeta is workset metadata db rows: workset_lst, workset_txt, workset_parameter, workset_parameter_txt
type WorksetMeta struct {
	Set   WorksetRow      // workset master row: workset_lst
	Txt   []WorksetTxtRow // workset text rows: workset_txt
	Param []worksetParam  // workset parameter: parameter_hid, sub-value count and workset_parameter_txt rows
}

// WorksetPub is "public" workset metadata for json import-export
type WorksetPub struct {
	ModelName      string           // model name for that workset
	ModelDigest    string           // model digest for that workset
	Name           string           // workset name: set_name VARCHAR(255) NOT NULL
	BaseRunDigest  string           // if not empty then digest of the base run
	IsReadonly     bool             // readonly flag
	UpdateDateTime string           // last update date-time
	Txt            []DescrNote      // workset text: description and notes by language
	Param          []ParamRunSetPub // workset parameters: name and text (value notes by language)
}

// WorksetRow is workset_lst table row.
type WorksetRow struct {
	SetId          int    // unique working set id
	BaseRunId      int    // if not NULL and positive then base run id (source run id)
	ModelId        int    // model_id     INT          NOT NULL
	Name           string // set_name     VARCHAR(255) NOT NULL
	IsReadonly     bool   // is_readonly  SMALLINT     NOT NULL
	UpdateDateTime string // update_dt    VARCHAR(32)  NOT NULL, -- last update date-time
}

// WorksetParam is a holder for workset parameter Hid, sub-value count and workset_parameter_txt rows
type worksetParam struct {
	ParamHid int                  // parameter_hid INT NOT NULL
	SubCount int                  // number of parameter sub-values
	Txt      []WorksetParamTxtRow // workset_parameter_txt table rows
}

// WorksetTxtRow is db row of workset_txt
type WorksetTxtRow struct {
	SetId    int    // set_id    INT          NOT NULL
	LangCode string // lang_code VARCHAR(32)  NOT NULL
	Descr    string // descr     VARCHAR(255) NOT NULL
	Note     string // note      VARCHAR(32000)
}

// WorksetParamTxtRow is workset_parameter_txt table row.
type WorksetParamTxtRow struct {
	SetId    int    // set_id        INT NOT NULL
	ParamHid int    // parameter_hid INT NOT NULL
	LangCode string // lang_code VARCHAR(32)  NOT NULL
	Note     string // note          VARCHAR(32000), -- parameter value note
}

// TaskMeta is metadata for modeling task: name, status, description, notes, task run history.
//
// Modeling task is a named set of input model inputs (of workset ids) to run the model.
// Typical use case: create multiple input sets by varying some model parameters,
// combine it under named "task" and run the model with that task name.
// As result multiple model "runs" created ("run" is input and output data of model run).
// Such run of model called "task run" and allow to study dependencies between model input and output.
//
// Task can be edited by user: new input workset ids added or some workset id(s) excluded.
// As result current task body (workset ids of the task) may be different
// from older version of it: task_set set_id's  may not be same as task_run_set set_id's.
// TaskRun and TaskRunSet is a history and result of that task run,
// but there is no guarantee of any workset in task history still exist
// or contain same input parameter values as it was at the time of task run.
// To find actual input for any particular model run and/or task run we must use run_id.
type TaskMeta struct {
	Task    TaskRow       // modeling task row: task_lst
	Txt     []TaskTxtRow  // task text rows: task_txt
	Set     []int         // task body (current list of workset id's): task_set
	TaskRun []taskRunItem // task run history: task_run_lst and task_run_set rows
}

// taskRunItem is master task_run_lst row (task run status, date-time...)
// and details as list of (run id, ste id) pairs
type taskRunItem struct {
	TaskRunRow                 // task run history row: task_run_lst
	TaskRunSet []TaskRunSetRow // task run history body: task_run_set
}

// TaskRow is db row of task_lst.
type TaskRow struct {
	TaskId  int    // task_id      INT          NOT NULL, -- unique task id
	ModelId int    // model_id     INT          NOT NULL
	Name    string // task_name    VARCHAR(255) NOT NULL
}

// TaskTxtRow is db row of task_txt
type TaskTxtRow struct {
	TaskId   int    // task_id  INT           NOT NULL
	LangCode string // lang_code VARCHAR(32)  NOT NULL
	Descr    string // descr     VARCHAR(255) NOT NULL
	Note     string // note      VARCHAR(32000)
}

// TaskRunRow is db row of task_run_lst.
// This table contains task run history and status.
//
// Task status: i=init p=progress w=wait s=success x=exit e=error(failed)
//   if task status = w (wait) then
//      model wait and NOT completed until other process set status to one of finals: s,x,e
//      model check if any new sets inserted into task_set and run it as they arrive
type TaskRunRow struct {
	TaskRunId      int    // task_run_id INT          NOT NULL, -- unique task run id
	TaskId         int    // task_id     INT          NOT NULL
	Name           string // run_name    VARCHAR(255) NOT NULL, -- task run name
	SubCount       int    // sub_count   INT          NOT NULL, -- subvalue count of task run
	CreateDateTime string // create_dt   VARCHAR(32)  NOT NULL, -- start date-time
	Status         string // status      VARCHAR(1)   NOT NULL, -- task status: i=init p=progress w=wait s=success x=exit e=error(failed)
	UpdateDateTime string // update_dt   VARCHAR(32)  NOT NULL, -- last update date-time
}

// TaskRunSetRow is db row of task_run_set.
// This table contains task run input (working set id) and output (model run id)
type TaskRunSetRow struct {
	TaskRunId int // task_run_id INT NOT NULL
	RunId     int // run_id      INT NOT NULL, -- if > 0 then result run id
	SetId     int // set_id      INT NOT NULL, -- if > 0 then input working set id
	TaskId    int // task_id     INT NOT NULL
}

// TaskPub is "public" modeling task metadata, task input worksets and task run history for json import-export
type TaskPub struct {
	TaskDefPub              // task definition: metadata and input worksets
	TaskRun    []taskRunPub // task run history: task_run_lst
}

// TaskDefPub is "public" modeling task metadata and task input worksets for json import-export
type TaskDefPub struct {
	ModelName   string      // model name for that task list
	ModelDigest string      // model digest for that task list
	Name        string      // task_name    VARCHAR(255) NOT NULL
	Txt         []DescrNote // task text: description and notes by language
	Set         []string    // task body: list of workset names
}

// taskRunPub is "public" metadata of task run history.
type taskRunPub struct {
	Name           string          // run_name    VARCHAR(255) NOT NULL, -- task run name
	SubCount       int             // sub_count   INT          NOT NULL, -- subvalue count of task run
	CreateDateTime string          // create_dt   VARCHAR(32)  NOT NULL, -- start date-time
	Status         string          // status      VARCHAR(1)   NOT NULL, -- task status: i=init p=progress w=wait s=success x=exit e=error(failed)
	UpdateDateTime string          // update_dt   VARCHAR(32)  NOT NULL, -- last update date-time
	TaskRunSet     []taskRunSetPub // task run history body: run and set pairs
}

// taskRunSetPub is "public" metadata of task run history body: run and set pairs.
// To find workset name is used, it is unique by model.
// To find model run:
// if digest not empty then use digest;
// else if status is error then by run_name, sub_count, sub_completed, status, create_dt.
type taskRunSetPub struct {
	SetName string   // workset name
	Run     struct { // "public" link to model run
		Name           string // run_name      VARCHAR(255) NOT NULL
		SubCompleted   int    // sub_completed INT          NOT NULL, -- number of subvalues completed
		CreateDateTime string // create_dt     VARCHAR(32)  NOT NULL, -- start date-time
		Status         string // status        VARCHAR(1)   NOT NULL, -- run status: i=init p=progress s=success x=exit e=error(failed)
		Digest         string // run_digest    VARCHAR(32)  NULL,     -- digest of the run
	}
}
