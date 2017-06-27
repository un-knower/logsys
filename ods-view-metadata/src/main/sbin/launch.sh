#!/bin/bash

#
# example:  sh ./launch.sh --taskId=task1 --disableGenerateDDLAndDML=false --disableExecuteDDL=false --disableExecuteDML=false
#

cd `dirname $0`
pwd=`pwd`

source ../bin/envFn.sh
load_args $*

sh ../bin/ods_view_metadata.sh $@

