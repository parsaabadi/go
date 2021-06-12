#!/usr/bin/env bash
#
# invoked by oms with template arguments:
#
# ../../etc/scripts/oms-OzProj-run.sh {{.ModelName}} {{.ExeStem}} {{.Dir}} {{.BinDir}} ....more....

set -e 

OM_T_MODEL_NAME=$1
OM_T_EXE_STEM=$2
OM_T_WORK_DIR=$3
OM_T_BIN_DIR=$4
shift 4

export OM_${OM_T_MODEL_NAME}=../..
cd ../${OM_T_MODEL_NAME}/ompp/bin

../../../bin/${OM_T_EXE_STEM} -OpenM.Database "Database=../../../bin/${OM_T_MODEL_NAME}.sqlite;OpenMode=ReadWrite;Timeout=86400;" $@
