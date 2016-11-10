// Copyright (c) 2016 OpenM++
// This code is licensed under the MIT license (see LICENSE.txt for details)

package db

import "strconv"

// Facet is type to define database provider and driver facets, ie: name of bigint type
type Facet uint8

const (
	DefaultFacet Facet = iota // common default db facet
	SqliteFacet               // SQLite db facet
	PgSqlFacet                // PostgreSQL db facet
	MySqlFacet                // MySQL and MariaDB facet
	MsSqlFacet                // MS SQL db facet
	OracleFacet               // Oracle db facet
	Db2Facet                  // DB2 db facet
)

// String is default printable value of db facet, Stringer implementation
func (facet Facet) String() string {
	switch facet {
	case DefaultFacet:
		return "Default db facet"
	case SqliteFacet:
		return "Sqlite db facet"
	case PgSqlFacet:
		return "PostgreSQL db facet"
	case MySqlFacet:
		return "MySQL db facet"
	case MsSqlFacet:
		return "MS SQL db facet"
	case OracleFacet:
		return "Oracle db facet"
	case Db2Facet:
		return "DB2 db facet"
	}
	return "Unknown db facet"
}

// bigintType return type name for BIGINT sql type
func (facet Facet) bigintType() string {
	if facet == OracleFacet {
		return "NUMBER(19)"
	}
	return "BIGINT"
}

// floatType return type name for FLOAT standard sql type
func (facet Facet) floatType() string {
	if facet == OracleFacet {
		return "BINARY_DOUBLE"
	}
	return "FLOAT"
}

// textType return column type DDL for long VARCHAR columns, use it for len > 255.
func (facet Facet) textType(len int) string {
	switch facet {
	case MsSqlFacet:
		if len > 4000 {
			return "TEXT"
		}
	case OracleFacet:
		if len > 2000 {
			return "CLOB"
		}
	}
	return "VARCHAR(" + strconv.Itoa(len) + ")"
}

// maxTableNameSize return max length of db table or view name.
func (facet Facet) maxTableNameSize() int {
	switch facet {
	case PgSqlFacet:
		return 63
	case OracleFacet:
		return 30
	}
	return 64
}

// createTableIfNotExist return sql statement to create table if not exists
func (facet Facet) createTableIfNotExist(tableName string, bodySql string) string {

	switch facet {
	case SqliteFacet, PgSqlFacet, MySqlFacet:
		return "CREATE TABLE IF NOT EXISTS " + tableName + " " + bodySql
	case MsSqlFacet:
		return "IF NOT EXISTS" +
			" (SELECT * FROM INFORMATION_SCHEMA.TABLES T WHERE T.TABLE_NAME = " + toQuoted(tableName) + ") " +
			" CREATE TABLE " + tableName + " " + bodySql
	}
	return "CREATE TABLE " + tableName + " " + bodySql
}
