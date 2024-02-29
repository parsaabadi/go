#!/usr/bin/env bash
#
# Linux: compress model database by copy the model into a new database
#
# It does:
#   use sqlite3 to create new empty model db: modelName.db
#   use dbcopy to copy from old modelName.sqlite into new modelName.db
#   delete old database file modelName.sqlite
#   rename modelName.sqlite into modelName.db
#
#
# Environment:
#   sqlite3   - must be in the $PATH
#   OM_ROOT   - openM++ root folder, default: ../..
#               must exist:
#                 $OM_ROOT/bin/dbcopy
#                 $OM_ROOT/sql/create_db.sql
#                 $OM_ROOT/sql/insert_default.sql
#                 $OM_ROOT/sql/sqlite/optional_meta_views_sqlite.sql
# Arguments:
#        $1 - path to model database
#        $2 - (optional) model name, default: database file name
#        $3 - (optional) model digest
#

set -e

self_dir=$(dirname "$0")

db_path="$1"
m_name="$2"
m_digest="$3"

# check if database file exist

if [ -z "${db_path}" ] ;
then
  echo "ERROR: invalid (empty) path to SQLite database file"
  exit 1
fi
if [ ! -f "${db_path}" ] ;
then
  echo "ERROR: SQLite database file not found (or invalid): ${db_path}"
  exit 1
fi

db_file=$(basename "${db_path}")
db_dir=$(dirname "${db_path}")
db_stem="${db_file%.*}"

echo "Model db : ${db_file}"

if [ -z "${db_dir}" ] ;
then
  echo "ERROR: invalid (empty) model database directory"
  exit 1
fi

# if model name not supplied as argument then use db file stem: file name without extension

[ -z "${m_name}" ] && m_name="${db_stem}"

if [ -z "${m_name}" ] ;
then
  echo "ERROR: invalid model name: ${db_stem}"
  exit 1
fi

echo "Model    : ${m_name}"

# model digest is optional

m_arg="-m ${m_name}"

if [ -n "${m_digest}" ] ;
then
  m_arg="-dbcopy.ModelDigest ${m_digest}"

  echo "Digest   : ${m_digest}"
fi

# check OM_ROOT, following must exist:
#   $OM_ROOT/bin/dbcopy
#   $OM_ROOT/sql/create_db.sql
#   $OM_ROOT/sql/insert_default.sql
#   $OM_ROOT/sql/sqlite/optional_meta_views_sqlite.sql

[ -z "${OM_ROOT}" ] && OM_ROOT=$(dirname "$self_dir")

echo "OM_ROOT  : ${OM_ROOT}"

dbcopy_exe="${OM_ROOT}"/bin/dbcopy

if [ ! -x "${dbcopy_exe}" ] ;
then
  echo "ERROR: dbcopy utility not found (or invalid): ${dbcopy_exe}"
  exit 1
fi
if [ ! -f "${OM_ROOT}/sql/create_db.sql" ] || [ ! -f "${OM_ROOT}/sql/insert_default.sql" ] || [ ! -f "${OM_ROOT}/sql/sqlite/optional_meta_views_sqlite.sql" ] ;
then
  echo "ERROR: SQL script(s) not found at: ${OM_ROOT}/sql"
  exit 1
fi

# check if sqlite3 exe exists

SQLITE_EXE=sqlite3

if ! command -v "${SQLITE_EXE}" ;
then
  echo "ERROR: $SQLITE_EXE utility not found"
  exit 1
fi

# create new modelName.db empty file
#

new_db="${db_dir}/${db_stem}.db"

if [ -f "${new_db}" ] ;
then
  if ! rm "${new_db}" ;
  then
    echo "ERROR: fail to delete: ${new_db}"
    exit 1
  fi
fi

do_sql_script()
{
  if ! "${SQLITE_EXE}" "$1" < "$2" ;
  then
    echo "ERROR at: $SQLITE_EXE $1 < $2"
    exit 1
  fi
}

do_sql_script "${new_db}" "${OM_ROOT}/sql/create_db.sql"
do_sql_script "${new_db}" "${OM_ROOT}/sql/insert_default.sql"
do_sql_script "${new_db}" "${OM_ROOT}/sql/sqlite/optional_meta_views_sqlite.sql"

# source database: 
# report run status and lock all worksets
#

do_sql_cmd()
{
  if ! echo "$2" | "${SQLITE_EXE}" "$1" ;
  then
    echo "ERROR at: $2"
    exit 1
  fi
}

echo Source model run status count:
do_sql_cmd "${db_path}" "SELECT status, COUNT(*) FROM run_lst GROUP BY status ORDER BY 1;" 

echo Update source input scenario: set read-only
do_sql_cmd "${db_path}" "UPDATE workset_lst SET is_readonly = 1;"

echo Source input scenario count:
do_sql_cmd "${db_path}" "SELECT COUNT(*) FROM workset_lst;"

# copy model into new database
#

if ! "${dbcopy_exe}" ${m_arg} -dbcopy.To db2db -dbcopy.ToSqlite "${new_db}"  -dbcopy.FromSqlite "${db_path}" ;
then
  echo "ERROR at: ${dbcopy_exe} ${m_arg} -dbcopy.To db2db -dbcopy.ToSqlite ${new_db}  -dbcopy.FromSqlite ${db_path}"
  exit 1
fi

# report copy results
#

echo Results model run status:
do_sql_cmd "${new_db}" "SELECT status, COUNT(*) FROM run_lst GROUP BY status ORDER BY 1;"

echo Results input scenario count:
do_sql_cmd "${new_db}" "SELECT COUNT(*) FROM workset_lst;"

# final copy
#

if ! rm "${db_path}" ;
then
  echo "ERROR: fail to delete: ${db_path}"
  exit 1
fi
if ! mv "${new_db}" "${db_path}" ;
then
  echo "ERROR: fail to rename: ${new_db} -> ${db_path}"
  exit 1
fi

echo "Done."
