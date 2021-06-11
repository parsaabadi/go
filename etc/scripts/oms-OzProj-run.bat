@echo off

rem additional environment from template:
rem
rem OM_T_MODEL_NAME
rem OM_T_EXE_STEM
rem OM_T_WORK_DIR
rem OM_T_BIN_DIR

set OM_%OM_T_MODEL_NAME%=../..
cd .\..\%OM_T_MODEL_NAME%\ompp\bin

..\..\..\bin\%OM_T_EXE_STEM% -OpenM.Database Database="..\..\..\bin\%OM_T_MODEL_NAME%.sqlite;OpenMode=ReadWrite;Timeout=86400;" %*
