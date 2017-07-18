// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package main

import (
	"database/sql"
	"sync"

	"golang.org/x/text/language"

	"go.openmpp.org/ompp/db"
)

// ModelCatalog is a list of the models and database connections.
// If model directory specified then model catalog include model.sqlite files from model directory.
type ModelCatalog struct {
	theLock      sync.Mutex // mutex to lock for model list operations
	isDirEnabled bool       // if true then use sqlite files from model directory
	modelDir     string     // model directory
	modelLst     []modelDef // list of model_dic and associated database connections
}

// list of models and database connections
var theCatalog ModelCatalog

// modelDef is database connection and model database rows
type modelDef struct {
	dbConn        *sql.DB          // database connection
	isMetaFull    bool             // if true then ModelMeta fully loaded else only ModelDicRow
	meta          *db.ModelMeta    // model metadata, language-neutral part, should not be nil
	isTxtMetaFull bool             // if true then ModelTxtMeta fully loaded else only []ModelTxtRow
	txtMeta       *db.ModelTxtMeta // if not nil then language-specific model metadata
	langLst       []db.LangLstRow  // model languages, first is default language
	langTags      []language.Tag   // model languages as tags
	matcher       language.Matcher // matcher to search text by language
	groupLst      *db.GroupMeta    // if not nil then parameters and table groups
}

// ModelMetaDescrNote is language-specific portion of model metadata db rows
// It is sliced by one single language, but it can be different single language for each row.
// It is either user prefered language, model default language, first of the row or empty "" language.
type ModelMetaDescrNote struct {
	ModelDicDescrNote                  // model text rows: model_dic_txt
	TypeTxt           []TypeDescrNote  // model type text rows: type_dic_txt join to model_type_dic
	ParamTxt          []ParamDescrNote // model parameter text rows: parameter_dic_txt join to model_parameter_dic
	TableTxt          []TableDescrNote // model output table text rows: table_dic_txt join to model_table_dic
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

// TableDescrNote is join of able_dic, model_table_dic, table_dic_txt
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

// GroupMeta is db rows to describe parent-child groups of model parameters and output tables.
// It is language-neutral portion of groups metadata
type GroupMeta struct {
	Group   []db.GroupLstRow // parameters or output tables group rows: group_lst
	GroupPc []db.GroupPcRow  // group parent-child relationship rows: group_pc
}

// GroupMetaDescrNote is db rows to describe parent-child groups of model parameters and output tables.
// It is sliced by one single language, but it can be different single language for each row.
// It is either user prefered language, model default language, first of the row or empty "" language.
type GroupMetaDescrNote struct {
	GroupLst []GroupDescrNote // parameters or output tables group rows: group_lst and group_txt
	GroupPc  []db.GroupPcRow  // group parent-child relationship rows: group_pc
}

// GroupDescrNote is join of group_lst and group_txt
type GroupDescrNote struct {
	Group     db.GroupLstRow // parameters or output tables group rows: group_lst
	DescrNote db.DescrNote   // from group_txt
}
