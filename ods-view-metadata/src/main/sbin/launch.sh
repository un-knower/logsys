#!/bin/bash

#
# example:  sh ./launch.sh --taskId=task1 --disableGenerateDDLAndDML=false --disableExecuteDDL=false --disableExecuteDML=false
#

cd `dirname $0`
pwd=`pwd`

source ../bin/envFn.sh
load_args $*

export pidFile=${pwd}/../logs/ods-view-metadata.pid
export logFile=/data/logs/ods-view-metadata/ods-view-metadata.log
export gcFile=/data/logs/ods-view-metadata/gc_ods-view-metadata.log


echo "pid file: ${pidFile}"
echo "log file: ${logFile}"
echo "gc file: ${gcFile}"

../bin/ods_view_metadata.sh $@

