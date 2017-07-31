#!/bin/bash

#
# example :  sh commit_tmp_file.sh --taskFile=/tmp/tmp_log_origin.data.txt
#

cd `dirname $0`
pwd=`pwd`

source ../bin/envFn.sh
load_args $@

if [ -z $taskFile ]; then
    taskFile="/tmp/tmp_log_origin.list"
    hadoop fs -ls /data_warehouse/ods_origin.db/tmp_log_origin > $taskFile
fi

for file in `cat $taskFile|awk '{print $8}'`
do
    c=`hadoop fs -count $file|awk '{print $3}'`
    if [ "$c" != "0" ]; then
        fileName=`echo $file|awk -F '/' '{print $5}'`
        appId=`echo $fileName|awk -F '_' '{print $1}'`
        time=`echo $fileName|awk -F '_' '{print $2}'`
        day=${time:0:8}
        hour=${time:8:2}
        targetFile="/data_warehouse/ods_origin.db/log_origin/key_appId=$appId/key_day=$day/key_hour=$hour/$fileName"
        echo "move file: $file -> $targetFile"
        hadoop fs -mv $file $targetFile
    else
        echo "skip file: $file"
    fi
done

