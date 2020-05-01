// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"sync"

	"github.com/openmpp/go/ompp/db"
	"golang.org/x/text/language"
)

// ModelCatalog is a list of the models and database connections.
// If model directory specified then model catalog include model.sqlite files from model directory.
type ModelCatalog struct {
	theLock         sync.Mutex // mutex to lock for model list operations
	isDirEnabled    bool       // if true then use sqlite files from model directory
	modelDir        string     // models bin directory, it is a root dir under which model.sqlite and model.exe expected to be located
	modelLogDir     string     // default model log directory
	isLogDirEnabled bool       // if true then default log directory exist
	lastTimeStamp   string     // most recent timestamp
	modelLst        []modelDef // list of model metadata and associated database connections
}

// list of models and database connections
var theCatalog ModelCatalog

// ModelCatalogConfig is "public" state of model catalog for json import-export
type ModelCatalogConfig struct {
	ModelDir        string // model bin directory
	ModelLogDir     string // default model log directory
	IsLogDirEnabled bool   // if true then default log directory exist
	LastTimeStamp   string // most recent timestamp
}

// modelDef is database connection and model metadata database rows
type modelDef struct {
	dbConn        *sql.DB           // database connection
	binDir        string            // database and .exe directory: directory part of models/bin/model.sqlite
	logDir        string            // model log directory
	isLogDir      bool              // if true then use model log directory for model run logs
	isMetaFull    bool              // if true then ModelMeta fully loaded else only ModelDicRow
	meta          *db.ModelMeta     // model metadata, language-neutral part, should not be nil
	isTxtMetaFull bool              // if true then ModelTxtMeta fully loaded else only []ModelTxtRow
	txtMeta       *db.ModelTxtMeta  // if not nil then language-specific model metadata
	langCodes     []string          // language codes, first is default language
	matcher       language.Matcher  // matcher to search text by language
	langMeta      *db.LangMeta      // list of languages: one list per db connection, order of languages NOT the same as language codes
	modelWord     *db.ModelWordMeta // if not nil then list of model words, order of languages NOT the same as language codes
}

// modelBasic is basic model info: name, digest, files location
type modelBasic struct {
	name     string // model name
	digest   string // model digest
	binDir   string // database and .exe directory: directory part of models/bin/model.sqlite
	logDir   string // model log directory
	isLogDir bool   // if true then use model log directory for model run logs
}

// ModelMetaFull is full model metadata: language-neutral db rows
// and language-specific rows in all languages.
type ModelMetaFull struct {
	db.ModelMeta    // model text rows: model_dic_txt
	db.ModelTxtMeta // model type text rows: type_dic_txt join to model_type_dic
}

// ModelMetaDescrNote is language-specific model metadata db rows.
// It is sliced by one single language, but it can be different single language for each row.
// It is either user prefered language, model default language, first of the row or empty "" language.
type ModelMetaDescrNote struct {
	ModelDicDescrNote                  // model text rows: model_dic_txt
	TypeTxt           []TypeDescrNote  // model type text rows: type_dic_txt join to model_type_dic
	ParamTxt          []ParamDescrNote // model parameter text rows: parameter_dic_txt join to model_parameter_dic
	TableTxt          []TableDescrNote // model output table text rows: table_dic_txt join to model_table_dic
	GroupTxt          []GroupDescrNote // model group text rows: group_txt join to group_lst
}

// ModelDicDescrNote is join of model_dic db row and model_dic_txt row
type ModelDicDescrNote struct {
	Model     db.ModelDicRow // model_dic db row
	DescrNote db.DescrNote   // from model_dic_txt
}

// TypeDescrNote is join of type_dic_txt, model_type_dic, type_dic_txt
type TypeDescrNote struct {
	Type        db.TypeDicRow       // model type row: type_dic join to model_type_dic
	DescrNote   db.DescrNote        // from type_dic_txt
	TypeEnumTxt []TypeEnumDescrNote // type enum text rows: type_enum_txt join to model_type_dic
}

// TypeEnumDescrNote is join of type_enum_lst, model_type_dic, type_enum_txt
type TypeEnumDescrNote struct {
	Enum      db.TypeEnumRow // type enum row: type_enum_lst join to model_type_dic
	DescrNote db.DescrNote   // from type_enum_txt
}

// ParamDescrNote is join of parameter_dic, model_parameter_dic, parameter_dic_txt
type ParamDescrNote struct {
	Param        db.ParamDicRow       // parameter row: parameter_dic join to model_parameter_dic table
	DescrNote    db.DescrNote         // from parameter_dic_txt
	ParamDimsTxt []ParamDimsDescrNote // parameter dimension text rows: parameter_dims_txt join to model_parameter_dic
}

// ParamDimsDescrNote is join of parameter_dims, model_parameter_dic, parameter_dims_txt
type ParamDimsDescrNote struct {
	Dim       db.ParamDimsRow // parameter dimension row: parameter_dims join to model_parameter_dic table
	DescrNote db.DescrNote    // from parameter_dims_txt
}

// TableDescrNote is join of table_dic, model_table_dic, table_dic_txt
type TableDescrNote struct {
	Table        db.TableDicRow       // output table row: table_dic join to model_table_dic
	LangCode     string               // table_dic_txt.lang_code
	TableDescr   string               // table_dic_txt.descr
	TableNote    string               // table_dic_txt.note
	ExprDescr    string               // table_dic_txt.expr_descr
	ExprNote     string               // table_dic_txt.expr_note
	TableDimsTxt []TableDimsDescrNote // output table dimension text rows: table_dims_txt join to model_table_dic
	TableAccTxt  []TableAccDescrNote  // output table accumulator text rows: table_acc_txt join to model_table_dic
	TableExprTxt []TableExprDescrNote // output table expression text rows: table_expr_txt join to model_table_dic
}

// TableDimsDescrNote is join of table_dims, model_table_dic, table_dims_txt
type TableDimsDescrNote struct {
	Dim       db.TableDimsRow // parameter dimension row: table_dims join to model_table_dic table
	DescrNote db.DescrNote    // from table_dims_txt
}

// TableAccDescrNote is join of table_acc, model_table_dic, table_acc_txt
type TableAccDescrNote struct {
	Acc       db.TableAccRow // output table accumulator row: table_acc join to model_table_dic
	DescrNote db.DescrNote   // from table_acc_txt
}

// TableExprDescrNote is join of table_expr, model_table_dic, table_expr_txt
type TableExprDescrNote struct {
	Expr      db.TableExprRow // output table expression row: table_expr join to model_table_dic
	DescrNote db.DescrNote    // from table_expr_txt
}

// GroupDescrNote is join of group_lst, group_pc and group_txt
type GroupDescrNote struct {
	Group     db.GroupMeta // parameters or output tables group rows: group_lst join to group_pc
	DescrNote db.DescrNote // from group_txt
}

// ModelLangWord is (code, label) rows from lang_word and model_word language-specific db tables.
// It is either in user prefered language or model default language.
type ModelLangWord struct {
	ModelName     string      // model name for text metadata
	ModelDigest   string      // model digest for text metadata
	LangCode      string      // language code selected for lang_word table rows
	LangWords     []codeLabel // lang_word db table rows as (code, value)
	ModelLangCode string      // language code selected for model_word table rows
	ModelWords    []codeLabel // model_word db table rows as (code, value)
}

// codeLabel is code + label pair, for example, language-specific "words" db table row.
type codeLabel struct {
	Code  string
	Label string
}
