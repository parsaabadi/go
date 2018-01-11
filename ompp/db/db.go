// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

/*
Package db to support openM++ model database operations.

Database may contain multiple openM++ models and consists of two parts:
(a) metadata to describe models: data types, input parameters, output tables, etc.
(b) actual modeling data itself: database tables for input parameters and output tables.

To run the model we need to have a set of model input parameters, called "input working set" or "workset".
User can "open" workset in read-write mode and modify model input parameters.
To use that set as model run input user must "close" it by marking workset as read-only.
After model run user can again open workset as read-write and continue input editing.
Each workset has a name (unique inside of the model) and set id (database unique positive int).

Result of model run stored in output tables and also include copy of all input parameters used to run the model.
That pair of input and output data called "run" and identified by run id (database unique positive int).
*/
package db

import (
	"bytes"
	"container/list"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"

	_ "github.com/alexbrainman/odbc"
	_ "github.com/mattn/go-sqlite3"
	"go.openmpp.org/ompp/helper"
	"go.openmpp.org/ompp/omppLog"
)

// Database connection values
const (
	SQLiteDbDriver  = "SQLite"  // default db driver name
	SQLiteTimeout   = 86400     // default SQLite busy timeout
	Sqlite3DbDriver = "sqlite3" // SQLite db driver name
	OdbcDbDriver    = "odbc"    // ODBC db driver name
)

// Open database connection.
//
// Default driver name: "SQLite" and connection string is compatible with model connection, ie:
//     Database=modelName.sqlite; Timeout=86400; OpenMode=ReadWrite;
// Otherwise it is expected to be driver-specific connection string, ie:
//     DSN=ms2014; UID=sa; PWD=secret;
//     file:m1.sqlite?mode=rw&_busy_timeout=86400000
// If isFacetRequired is true then database facet determined
func Open(dbConnStr, dbDriver string, isFacetRequired bool) (*sql.DB, Facet, error) {

	// convert default SQLite connection string into sqlite3 format
	// delete existing sqlite file if required
	facet := DefaultFacet
	if dbDriver == "" || dbDriver == SQLiteDbDriver {
		var err error
		if dbConnStr, dbDriver, err = prepareSqlite(dbConnStr); err != nil {
			return nil, DefaultFacet, err
		}
	}
	if dbDriver == Sqlite3DbDriver { // at this SQLite pseudo name replaced by "sqlite3" db-driver name
		facet = SqliteFacet
	}

	// open database connection
	omppLog.LogSql("Connect to " + dbDriver)

	dbConn, err := sql.Open(dbDriver, dbConnStr)
	if err != nil {
		return nil, DefaultFacet, err
	}

	// determine db facet if requered and not defined by driver (example: odbc)
	if isFacetRequired && facet == DefaultFacet {
		facet = detectFacet(dbConn)
	}
	if isFacetRequired {
		omppLog.LogSql(facet.String())
	}

	return dbConn, facet, nil
}

// IfEmptyMakeDefault return SQLite connection string and driver name based on model name:
//   Database=modelName.sqlite; Timeout=86400; OpenMode=ReadWrite;
func IfEmptyMakeDefault(modelName, dbConnStr, dbDriver string) (string, string) {
	if dbDriver == "" {
		dbDriver = SQLiteDbDriver
	}
	if dbDriver == SQLiteDbDriver && (dbConnStr == "" && modelName != "") {
		dbConnStr = MakeSqliteDefault(modelName + ".sqlite")
	}
	return dbConnStr, dbDriver
}

// MakeSqliteDefault return default SQLite connection string based on model.sqlite file path:
//   Database=model.sqlite; Timeout=86400; OpenMode=ReadWrite;
func MakeSqliteDefault(modelSqlitePath string) string {
	return "Database=" + modelSqlitePath + "; Timeout=" + strconv.Itoa(SQLiteTimeout) + "; OpenMode=ReadWrite;"
}

// Convert SQLite connection string into "sqlite3" format and delete existing db.slite file if required.
//
// Following parameters allowed for SQLite database connection:
//   Database - (required) database file path or URI
//   Timeout - (optional) table lock "busy" timeout in seconds, default=0
//   OpenMode - (optional) database file open mode: ReadOnly, ReadWrite, Create, default=ReadOnly
//   DeleteExisting - (optional) if true then delete existing database file, default: false
func prepareSqlite(dbConnStr string) (string, string, error) {

	// parse SQLite connection string
	kv, err := helper.ParseKeyValue(dbConnStr)
	if err != nil {
		return "", "", err
	}

	// check SQLite connection string parts
	dbPath := kv["Database"]
	if dbPath == "" {
		return "", "", errors.New("SQLIte database file path cannot be empty")
	}

	m := kv["OpenMode"]
	switch strings.ToLower(m) {
	case "", "readonly":
		m = "ro"
	case "readwrite":
		m = "rw"
	case "create":
		m = "rwc"
	default:
		return "", "", errors.New("SQLIte invalid OpenMode=" + m)
	}

	// check if file exist:
	// sqlite3 driver does create new file if not exist, it should return an error
	if m == "ro" || m == "rw" {
		if _, err := os.Stat(dbPath); err != nil {
			return "", "", errors.New("SQLIte file not exist (or not accessible) " + dbPath)
		}
	}

	s := kv["Timeout"]
	var t int
	if s != "" {
		if t, err = strconv.Atoi(s); err != nil {
			return "", "", err
		}
	}

	// if required delete source file
	s = kv["DeleteExisting"]
	if s != "" {
		var isDel bool
		if isDel, err = strconv.ParseBool(s); err != nil {
			return "", "", err
		}
		if isDel {
			_ = os.Remove(dbPath) // ignore file delete errors, assume file not exist
		}
	}

	// make sqlite3 connection string
	s3Conn := "file:" + dbPath + "?mode=" + m
	if t != 0 {
		s3Conn += "&_busy_timeout=" + strconv.Itoa(1000*t)
	}

	return s3Conn, Sqlite3DbDriver, nil
}

// make sql quoted string, ie: 'O''Brien'
func toQuoted(src string) string {
	var bt bytes.Buffer
	bt.WriteRune('\'')
	bt.WriteString(strings.Replace(src, "'", "''", -1))
	bt.WriteRune('\'')
	return bt.String()
}

// return "NULL" if string '' empty or return sql quoted string, ie: 'O''Brien'
func toQuotedOrNull(src string) string {
	if src == "" {
		return "NULL"
	}
	return toQuoted(src)
}

// convert boolean to sql value: true=1, false=0
func toBoolStr(isValue bool) string {
	if isValue {
		return "1"
	}
	return "0"
}

// SelectFirst select first db row and pass it to cvt() for row.Scan()
func SelectFirst(dbConn *sql.DB, query string, cvt func(row *sql.Row) error) error {
	if dbConn == nil {
		return errors.New("invalid database connection")
	}
	omppLog.LogSql(query)
	return cvt(dbConn.QueryRow(query))
}

// SelectRows select db rows and pass each to cvt() for rows.Scan()
func SelectRows(dbConn *sql.DB, query string, cvt func(rows *sql.Rows) error) error {

	if dbConn == nil {
		return errors.New("invalid database transaction")
	}
	omppLog.LogSql(query)

	rows, err := dbConn.Query(query) // query db rows
	if err != nil {
		return err
	}
	defer rows.Close()

	// process each row
	for rows.Next() {
		if err = cvt(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// SelectToList select db rows into list using cvt to convert (scan) each db row into struct.
//
// It selects "page size" number of rows starting from row number == offset (zero based).
// If page size <= 0 then all rows returned.
func SelectToList(
	dbConn *sql.DB, query string, layout ReadPageLayout, cvt func(rows *sql.Rows) (interface{}, error)) (*list.List, *ReadPageLayout, error) {

	if dbConn == nil {
		return nil, nil, errors.New("invalid database connection")
	}

	// query db rows
	omppLog.LogSql(query)

	rows, err := dbConn.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// convert each row and append to the result list
	nStart := layout.Offset
	if nStart < 0 {
		nStart = 0
	}
	nSize := layout.Size
	if nSize < 0 {
		nSize = 0
	}
	var nRow int64

	rs := list.New()
	for rows.Next() {
		nRow++
		if nSize > 0 && nRow > nStart+nSize {
			break
		}
		// convert and add row to the page
		if layout.IsLastPage || !layout.IsLastPage && nRow > nStart {
			r, err := cvt(rows)
			if err != nil {
				return nil, nil, err
			}
			rs.PushBack(r)
		}
		// if last page mode then
		// 	 if page size limited then keep page rows
		//   else keep only one row before page start offset
		if layout.IsLastPage &&
			(nSize > 0 && int64(rs.Len()) > nSize || nSize <= 0 && nRow <= nStart+1 && rs.Len() > 1) {
			rs.Remove(rs.Front())
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, nil, err
	}

	// actual start row offset, size of result and last page flag
	lt := ReadPageLayout{
		Offset:     nRow - int64(rs.Len()),
		Size:       int64(rs.Len()),
		IsLastPage: nSize <= 0 || nSize > 0 && nRow <= nStart+nSize,
	}
	if !lt.IsLastPage && lt.Offset > 0 {
		lt.Offset--
	}

	// return result if last page mode then:
	//   if actual offset < input start offset then remove extra rows from top of the page
	//   do such remove only if last row > input start offset
	//   otherwise keep entire page (if input offset too far and below last row)
	if layout.IsLastPage && lt.IsLastPage && nSize > 0 && lt.Offset < nStart && nRow > nStart {

		for lt.Offset < nStart && rs.Len() > 1 {
			rs.Remove(rs.Front())
			lt.Offset++
		}
		lt.Size = int64(rs.Len())
	}
	return rs, &lt, nil
}

// Update execute sql query outside of transaction scope (on different connection)
func Update(dbConn *sql.DB, query string) error {
	if dbConn == nil {
		return errors.New("invalid database connection")
	}
	omppLog.LogSql(query)

	_, err := dbConn.Exec(query)
	return err
}

// TrxSelectRows select db rows in transaction scope and pass each to cvt() for rows.Scan()
func TrxSelectRows(dbTrx *sql.Tx, query string, cvt func(rows *sql.Rows) error) error {

	if dbTrx == nil {
		return errors.New("invalid database transaction")
	}
	omppLog.LogSql(query)

	rows, err := dbTrx.Query(query) // query db rows
	if err != nil {
		return err
	}
	defer rows.Close()

	// process each row
	for rows.Next() {
		if err = cvt(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// TrxSelectFirst select first db row in transaction scope and pass it to cvt() for row.Scan()
func TrxSelectFirst(dbTrx *sql.Tx, query string, cvt func(row *sql.Row) error) error {
	if dbTrx == nil {
		return errors.New("invalid database transaction")
	}
	omppLog.LogSql(query)
	return cvt(dbTrx.QueryRow(query))
}

// TrxUpdate execute sql query in transaction scope
func TrxUpdate(dbTrx *sql.Tx, query string) error {
	if dbTrx == nil {
		return errors.New("invalid database transaction")
	}
	omppLog.LogSql(query)

	_, err := dbTrx.Exec(query)
	return err
}

// TrxUpdateStatement execute sql statement in transaction scope until put() return true
func TrxUpdateStatement(dbTrx *sql.Tx, query string, put func() (bool, []interface{}, error)) error {

	if dbTrx == nil {
		return errors.New("invalid database transaction")
	}
	omppLog.LogSql(query)

	// prepare statement in transaction scope
	stmt, err := dbTrx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// until put() return next row values execute statement
	for {
		isNext, r, err := put()
		if err != nil {
			return err
		}
		if !isNext {
			break
		}
		_, err = stmt.Exec(r...)
		if err != nil {
			return err
		}
	}
	return nil
}

// detectFacet obtains db facet by quiering sql server.
// It may not be always reliable and even not true facet.
// It is better to use driver information to determine db facet.
func detectFacet(dbConn *sql.DB) Facet {

	facet := DefaultFacet

	// check is it PostgreSQL
	// check is it MySQL (not reliable) or MariaDB
	// odbc driver bug (?): PostgreSQL 9.2 + odbc 9.5.400 fails forever after first query failed
	// that means PostgreSQL facet detection must be first
	_ = SelectRows(dbConn,
		"SELECT LOWER(VERSION())",
		func(rows *sql.Rows) error {
			var s sql.NullString
			if err := rows.Scan(&s); err != nil {
				return err
			}
			if s.Valid {
				v := s.String
				if strings.Contains(v, "postgresql") {
					facet = PgSqlFacet
				}
				if facet == DefaultFacet &&
					(strings.Contains(v, "mysql") || strings.Contains(v, "mariadb") || strings.HasPrefix(v, "5.")) {
					facet = MySqlFacet
				}
			}
			return nil
		})
	if facet != DefaultFacet {
		return facet
	}

	// check is it SQLite
	_ = SelectRows(dbConn,
		"SELECT COUNT(*) FROM sqlite_master",
		func(rows *sql.Rows) error {
			var n sql.NullInt64
			if err := rows.Scan(&n); err != nil {
				return err
			}
			if n.Valid {
				facet = SqliteFacet
			}
			return nil
		})
	if facet != DefaultFacet {
		return facet
	}

	// check is it MS SQL
	_ = SelectRows(dbConn,
		"SELECT LOWER(@@VERSION)",
		func(rows *sql.Rows) error {
			var s sql.NullString
			if err := rows.Scan(&s); err != nil {
				return err
			}
			if s.Valid {
				facet = MsSqlFacet
			}
			return nil
		})
	if facet != DefaultFacet {
		return facet
	}

	// check is it Oracle
	_ = SelectRows(dbConn,
		"SELECT LOWER(product) FROM product_component_version",
		func(rows *sql.Rows) error {
			var s sql.NullString
			if err := rows.Scan(&s); err != nil {
				return err
			}
			if s.Valid {
				facet = OracleFacet
			}
			return nil
		})
	if facet != DefaultFacet {
		return facet
	}

	// check is it IBM DB2
	_ = SelectRows(dbConn,
		"SELECT COUNT(*) FROM SYSIBMADM.ENV_PROD_INFO",
		func(rows *sql.Rows) error {
			var n sql.NullInt64
			if err := rows.Scan(&n); err != nil {
				return err
			}
			if n.Valid {
				facet = Db2Facet
			}
			return nil
		})
	return facet
}
