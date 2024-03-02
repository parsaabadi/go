@echo off
REM test of db cleanup on Windows
REM it is a test only and does nothing

set db_path=%1
set m_name=%2
set m_digest=%3

IF "%db_path%" == "" (
  @echo "ERROR: invalid (empty) path to SQLite database file"
  EXIT 1
)

@echo Model db : %db_path%
@echo Model    : %m_name%
@echo Digest   : %m_digest%

ping 127.0.0.1 -n 5 >nul

@echo Done.

