// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

// ModelMeta is model metadata db rows, language-independent portion of it.
//
// Types, parameters and output tables can be shared between different models and even between different databases.
// Use digest hash to find same type (parameter, table or model) in other database.
// As it is today language-specific part of model metadata (labels, description, notes, etc.)
// does not considered for "equality" comparison and not included in digest.
//
// For example, logical type consists of 2 enum (code, value) pairs: [(0, "false") (1, "true")] and
// even it has different labels in different databases, i.e. (1, "Truth") vs (1, "OK")
// such type(s) considered the same and should have identical digest(s).
//
// Inside of database *_hid (type_hid, parameter_hid, table_hid) is a unique id of corresponding object (primary key).
// Those _hid's are database-unique and should be used to find same type (parameter, output table) in other database.
// Also each type, parameter, output table have model-unique *_id (type_id, parameter_id, table_id)
// assigned by compiler and it is possible to find type, parameter or table by combination of (model_id, type_id).
//
// Unless otherwise specified each array is ordered by model-specific id's and binary search can be used.
// For example type array is ordered by (model_id, type_id) and type enum array by (model_id, type_id, enum_id).
type ModelMeta struct {
	Model ModelDicRow // model_dic table row
	Type  []TypeMeta  // types metadata: type name and enums
	Param []ParamMeta // parameters metadata: parameter name, type, dimensions
	Table []TableMeta // output tables metadata: table name, dimensions, accumulators, expressions
}

// maxBuiltInTypeId is max type id for openM++ built-in types, ie: int, double, logical
const maxBuiltInTypeId = 100

// TypeMeta is type metadata: type name and enums
type TypeMeta struct {
	TypeDicRow               // model type rows: type_dic join to model_type_dic
	Enum       []TypeEnumRow // type enum rows: type_enum_lst join to model_type_dic
}

// ParamMeta is parameter metadata: parameter name, type, dimensions
type ParamMeta struct {
	ParamDicRow                // model parameter row: parameter_dic join to model_parameter_dic table
	Dim         []ParamDimsRow // parameter dimension rows: parameter_dims join to model_parameter_dic table
	typeOf      *TypeMeta      // type of parameter
	sizeOf      int            // size of parameter: db row count calculated as dimension(s) size product
}

// TableMeta is output table metadata: table name, dimensions, accumulators, expressions
type TableMeta struct {
	TableDicRow                // model output table row: table_dic join to model_table_dic
	Dim         []TableDimsRow // output table dimension rows: table_dims join to model_table_dic
	Acc         []TableAccRow  // output table accumulator rows: table_acc join to model_table_dic
	Expr        []TableExprRow // output table expression rows: table_expr join to model_table_dic
	sizeOf      int            // db row count calculated as dimension(s) size product
}

// ModelTxtMeta is language-specific portion of model metadata db rows.
type ModelTxtMeta struct {
	ModelName    string            // model name for text metadata
	ModelDigest  string            // model digest for text metadata
	ModelTxt     []ModelTxtRow     // model text rows: model_dic_txt
	TypeTxt      []TypeTxtRow      // model type text rows: type_dic_txt join to model_type_dic
	TypeEnumTxt  []TypeEnumTxtRow  // type enum text rows: type_enum_txt join to model_type_dic
	ParamTxt     []ParamTxtRow     // model parameter text rows: parameter_dic_txt join to model_parameter_dic
	ParamDimsTxt []ParamDimsTxtRow // parameter dimension text rows: parameter_dims_txt join to model_parameter_dic
	TableTxt     []TableTxtRow     // model output table text rows: table_dic_txt join to model_table_dic
	TableDimsTxt []TableDimsTxtRow // output table dimension text rows: table_dims_txt join to model_table_dic
	TableAccTxt  []TableAccTxtRow  // output table accumulator text rows: table_acc_txt join to model_table_dic
	TableExprTxt []TableExprTxtRow // output table expression text rows: table_expr_txt join to model_table_dic
}

// GroupMeta is db rows to describe parent-child groups of model parameters and output tables.
type GroupMeta struct {
	ModelName   string        // model name for group metadata
	ModelDigest string        // model digest for group metadata
	GroupLst    []GroupLstRow // parameters or output tables group rows: group_lst
	GroupPc     []GroupPcRow  // group parent-child relationship rows: group_pc
	GroupTxt    []GroupTxtRow // group text rows: group_txt
}

// LangList is language and words in that language
type LangList struct {
	LangWord  []LangMeta     // languages and words in that language
	idIndex   map[int]int    // language id index
	codeIndex map[string]int // language code index
}

// LangMeta is language and words in that language
type LangMeta struct {
	LangLstRow           // lang_lst db-table row
	Word       []WordRow // lang_word db-table rows for the language
}

// ProfileMeta is rows from profile_option table.
// Profile is a named group of (key, value) options, similar to ini-file.
// Default model options has profile_name = model_name.
type ProfileMeta struct {
	Name string            // profile name
	Opts map[string]string // profile (key, value) options
}

// LangLstRow is db row of lang_lst table.
// LangId (lang_lst.lang_id) is db-unique id of the language, use lang_code to find same language in other db.
type LangLstRow struct {
	LangId   int    // lang_id   INT          NOT NULL
	LangCode string // lang_code VARCHAR(32)  NOT NULL
	Name     string // lang_name VARCHAR(255) NOT NULL
}

// WordRow is db row of lang_word table
type WordRow struct {
	LangId   int    // lang_id    INT          NOT NULL
	WordCode string // word_code  VARCHAR(255) NOT NULL
	Value    string // word_value VARCHAR(255) NOT NULL
}

// ModelDicRow is db row of model_dic table.
// ModelId (model_dic.model_id) is db-unique id of the model, use digest to find same model in other db.
type ModelDicRow struct {
	ModelId        int    // model_id         INT          NOT NULL
	Name           string // model_name       VARCHAR(255) NOT NULL
	Digest         string // model_digest     VARCHAR(32)  NOT NULL
	Type           int    // model_type       INT          NOT NULL
	Version        string // model_ver        VARCHAR(32)  NOT NULL
	CreateDateTime string // create_dt        VARCHAR(32)  NOT NULL
}

// ModelTxtRow is db row of model_dic_txt join to model_dic
type ModelTxtRow struct {
	ModelId  int    // model_id     INT          NOT NULL
	LangId   int    // lang_id      INT          NOT NULL
	LangCode string // lang_code    VARCHAR(32)  NOT NULL
	Descr    string // descr        VARCHAR(255) NOT NULL
	Note     string // note         VARCHAR(32000)
}

// TypeDicRow is db row of type_dic join to model_type_dic table.
// TypeHid (type_dic.type_hid) is db-unique id of the type, use digest to find same type in other db.
// TypeId (model_type_dic.model_type_id) is model-unique type id, assigned by model compiler.
type TypeDicRow struct {
	ModelId     int    // model_id      INT          NOT NULL
	TypeId      int    // model_type_id INT          NOT NULL
	TypeHid     int    // type_hid      INT          NOT NULL, -- unique type id
	Name        string // type_name     VARCHAR(255) NOT NULL, -- type name: int, double, etc.
	Digest      string // type_digest   VARCHAR(32)  NOT NULL
	DicId       int    // dic_id        INT NOT NULL, -- dictionary id: 0=simple 1=logical 2=classification 3=range 4=partition 5=link
	TotalEnumId int    // total_enum_id INT NOT NULL, -- if total enabled this is enum_value of total item =max+1
}

// TypeTxtRow is db row of type_dic_txt join to model_type_dic table
type TypeTxtRow struct {
	ModelId  int    // model_id      INT          NOT NULL
	TypeId   int    // model_type_id INT          NOT NULL
	LangId   int    // lang_id       INT          NOT NULL
	LangCode string // lang_code     VARCHAR(32)  NOT NULL
	Descr    string // descr         VARCHAR(255) NOT NULL
	Note     string // note          VARCHAR(32000)
}

// TypeEnumRow is db row of type_enum_lst join to model_type_dic table
type TypeEnumRow struct {
	ModelId int    // model_id      INT NOT NULL
	TypeId  int    // model_type_id INT NOT NULL
	EnumId  int    // enum_id       INT NOT NULL
	Name    string // enum_name     VARCHAR(255) NOT NULL
}

// TypeEnumTxtRow is db row of type_enum_txt join to model_type_dic table
type TypeEnumTxtRow struct {
	ModelId  int    // model_id      INT          NOT NULL
	TypeId   int    // model_type_id INT          NOT NULL
	EnumId   int    // enum_id       INT          NOT NULL
	LangId   int    // lang_id       INT          NOT NULL
	LangCode string // lang_code     VARCHAR(32)  NOT NULL
	Descr    string // descr         VARCHAR(255) NOT NULL
	Note     string // note          VARCHAR(32000)
}

// ParamDicRow is db row of parameter_dic join to model_parameter_dic table
// ParamHid (parameter_dic.parameter_hid) is db-unique id of the parameter, use digest to find same parameter in other db.
// ParamId (model_parameter_dic.model_parameter_id) is model-unique parameter id, assigned by model compiler.
type ParamDicRow struct {
	ModelId      int    // model_id           INT          NOT NULL
	ParamId      int    // model_parameter_id INT          NOT NULL
	ParamHid     int    // parameter_hid      INT          NOT NULL, -- unique parameter id
	Name         string // parameter_name     VARCHAR(255) NOT NULL
	Digest       string // parameter_digest   VARCHAR(32)  NOT NULL
	DbRunTable   string // db_run_table       VARCHAR(64)  NOT NULL
	DbSetTable   string // db_set_table       VARCHAR(64)  NOT NULL
	Rank         int    // parameter_rank     INT          NOT NULL
	TypeId       int    // model_type_id      INT          NOT NULL
	IsHidden     bool   // is_hidden          SMALLINT     NOT NULL
	NumCumulated int    // num_cumulated      INT          NOT NULL
}

// ParamTxtRow is db row of parameter_dic_txt join to model_parameter_dic table
type ParamTxtRow struct {
	ModelId  int    // model_id           INT          NOT NULL
	ParamId  int    // model_parameter_id INT          NOT NULL
	LangId   int    // lang_id            INT          NOT NULL
	LangCode string // lang_code          VARCHAR(32)  NOT NULL
	Descr    string // descr              VARCHAR(255) NOT NULL
	Note     string // note               VARCHAR(32000)
}

// ParamDimsRow is db row of parameter_dims join to model_parameter_dic table
type ParamDimsRow struct {
	ModelId int       // model_id           INT        NOT NULL
	ParamId int       // model_parameter_id INT        NOT NULL
	DimId   int       // dim_id             INT        NOT NULL
	Name    string    // dim_name           VARCHAR(8) NOT NULL
	TypeId  int       // model_type_id      INT        NOT NULL
	typeOf  *TypeMeta // type of dimension
	sizeOf  int       // dimension size as enum count, zero if type is simple
}

// ParamDimsTxtRow is db row of parameter_dims_txt join to model_parameter_dic table
type ParamDimsTxtRow struct {
	ModelId  int    // model_id           INT          NOT NULL
	ParamId  int    // model_parameter_id INT          NOT NULL
	DimId    int    // dim_id             INT          NOT NULL
	LangId   int    // lang_id            INT          NOT NULL
	LangCode string // lang_code          VARCHAR(32)  NOT NULL
	Descr    string // descr              VARCHAR(255) NOT NULL
	Note     string // note               VARCHAR(32000)
}

// TableDicRow is db row of table_dic join to model_table_dic table
// TableHid (table_dic.table_hid) is db-unique id of the output table, use digest to find same table in other db.
// TableId (model_table_dic.model_table_id) is model-unique output table id, assigned by model compiler.
type TableDicRow struct {
	ModelId     int    // model_id       INT          NOT NULL
	TableId     int    // model_table_id INT          NOT NULL
	TableHid    int    // table_hid      INT          NOT NULL, -- unique table id
	Name        string // table_name     VARCHAR(255) NOT NULL
	Digest      string // table_digest   VARCHAR(32)  NOT NULL
	DbExprTable string // db_expr_table  VARCHAR(64)  NOT NULL
	DbAccTable  string // db_acc_table   VARCHAR(64)  NOT NULL
	IsUser      bool   // is_user        SMALLINT     NOT NULL
	Rank        int    // table_rank     INT          NOT NULL
	IsSparse    bool   // is_sparse      SMALLINT     NOT NULL
	ExprPos     int    // expr_dim_pos   INT          NOT NULL
}

// TableTxtRow is db row of table_dic_txt join to model_table_dic table
type TableTxtRow struct {
	ModelId   int    // model_id       INT          NOT NULL
	TableId   int    // model_table_id INT          NOT NULL
	LangId    int    // lang_id        INT          NOT NULL
	LangCode  string // lang_code      VARCHAR(32)  NOT NULL
	Descr     string // descr          VARCHAR(255) NOT NULL
	Note      string // note           VARCHAR(32000)
	ExprDescr string // expr_descr     VARCHAR(255) NOT NULL
	ExprNote  string // expr_note      VARCHAR(32000)
}

// TableDimsRow is db row of table_dims join to model_table_dic table
type TableDimsRow struct {
	ModelId int       // model_id       INT        NOT NULL
	TableId int       // model_table_id INT        NOT NULL
	DimId   int       // dim_id         INT        NOT NULL
	Name    string    // dim_name       VARCHAR(8) NOT NULL
	TypeId  int       // model_type_id  INT        NOT NULL
	IsTotal bool      // is_total       SMALLINT   NOT NULL
	DimSize int       // dim_size       INT        NOT NULL
	typeOf  *TypeMeta // type of dimension
}

// TableDimsTxtRow is db row of table_dims_txt join to model_table_dic table
type TableDimsTxtRow struct {
	ModelId  int    // model_id       INT          NOT NULL
	TableId  int    // model_table_id INT          NOT NULL
	DimId    int    // dim_id         INT          NOT NULL
	LangId   int    // lang_id        INT          NOT NULL
	LangCode string // lang_code      VARCHAR(32)  NOT NULL
	Descr    string // descr          VARCHAR(255) NOT NULL
	Note     string // note           VARCHAR(32000)
}

// TableAccRow is db row of table_acc join to model_table_dic table
type TableAccRow struct {
	ModelId int    // model_id       INT          NOT NULL
	TableId int    // model_table_id INT          NOT NULL
	AccId   int    // acc_id         INT          NOT NULL
	Name    string // acc_name       VARCHAR(8)   NOT NULL
	AccExpr string // acc_expr       VARCHAR(255) NOT NULL
}

// TableAccTxtRow is db row of table_acc_txt join to model_table_dic table
type TableAccTxtRow struct {
	ModelId  int    // model_id       INT          NOT NULL
	TableId  int    // model_table_id INT          NOT NULL
	AccId    int    // acc_id         INT          NOT NULL
	LangId   int    // lang_id        INT          NOT NULL
	LangCode string // lang_code      VARCHAR(32)  NOT NULL
	Descr    string // descr          VARCHAR(255) NOT NULL
	Note     string // note           VARCHAR(32000)
}

// TableExprRow is db row of table_expr join to model_table_dic table
type TableExprRow struct {
	ModelId  int    // model_id       INT           NOT NULL
	TableId  int    // model_table_id INT           NOT NULL
	ExprId   int    // expr_id        INT           NOT NULL
	Name     string // expr_name      VARCHAR(8)    NOT NULL
	Decimals int    // expr_decimals  INT           NOT NULL
	SrcExpr  string // expr_src       VARCHAR(255)  NOT NULL
	ExprSql  string // expr_sql       VARCHAR(2048) NOT NULL
}

// TableExprTxtRow is db row of table_expr_txt join to model_table_dic table
type TableExprTxtRow struct {
	ModelId  int    // model_id       INT          NOT NULL
	TableId  int    // model_table_id INT          NOT NULL
	ExprId   int    // expr_id        INT           NOT NULL
	LangId   int    // lang_id        INT          NOT NULL
	LangCode string // lang_code      VARCHAR(32)  NOT NULL
	Descr    string // descr          VARCHAR(255) NOT NULL
	Note     string // note           VARCHAR(32000)
}

// GroupLstRow is db row of group_lst table
type GroupLstRow struct {
	ModelId  int    // model_id     INT          NOT NULL
	GroupId  int    // group_id     INT          NOT NULL
	IsParam  bool   // is_parameter SMALLINT     NOT NULL, -- if <> 0 then parameter group else output table group
	Name     string // group_name   VARCHAR(255) NOT NULL
	IsHidden bool   // is_hidden    SMALLINT     NOT NULL
}

// GroupPcRow is db row of group_pc table
type GroupPcRow struct {
	ModelId      int // model_id       INT NOT NULL
	GroupId      int // group_id       INT NOT NULL
	ChildPos     int // child_pos      INT NOT NULL
	ChildGroupId int // child_group_id INT NULL, -- if not NULL then id of child group
	ChildLeafId  int // leaf_id        INT NULL, -- if not NULL then id of parameter or output table
}

// GroupTxtRow is db row of group_txt table
type GroupTxtRow struct {
	ModelId  int    // model_id  INT          NOT NULL
	GroupId  int    // group_id  INT          NOT NULL
	LangId   int    // lang_id   INT          NOT NULL
	LangCode string // lang_code VARCHAR(32)  NOT NULL
	Descr    string // descr     VARCHAR(255) NOT NULL
	Note     string // note      VARCHAR(32000)
}
