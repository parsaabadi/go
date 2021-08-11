#!/usr/bin/env bash
#
# invoked by oms with template arguments:
#
# ../../etc/scripts/oms-OzProj-run.sh {{.ModelName}} {{.ExeStem}} {{.Dir}} {{.BinDir}} {{.DbPath}} ....more....

set -e 

OM_T_MODEL_NAME=$1
OM_T_EXE_STEM=$2
OM_T_WORK_DIR=$3
OM_T_BIN_DIR=$4
OM_T_DB_PATH=$5
shift 5

export OM_${OM_T_MODEL_NAME}=../..
cd ../${OM_T_MODEL_NAME}/ompp/bin

../../../bin/${OM_T_EXE_STEM} -OpenM.Database "Database=${OM_T_DB_PATH};OpenMode=ReadWrite;Timeout=86400;" $@
