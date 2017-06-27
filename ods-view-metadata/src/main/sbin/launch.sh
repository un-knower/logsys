#!/bin/bash

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
echo "groupId: ${groupId}"

case "$cmd" in
    start)

        if [ -f ${pidFile} ]; then
          pid=$(cat ${pidFile})
          pid=`ps -ef|grep $pid|grep -v grep|awk '{print $2}'`
          if [ -n "$pid" ]; then
            echo "task always running! pid=$pid"
            exit
          fi
        fi

        set -x
        nohup ../bin/ods_view_metadata.sh $@ >> ${logFile} 2>&1 &
        set +x
    ;;
    stop)
        if [ -f $pidFile ]; then
            pidV=`cat $pidFile`
            pid=`ps -ef|grep $pidV|grep -v grep|awk '{print $2}'`
            if [ -n "$pid" ]; then
                kill $pid
                ret=$?
                echo "kill process $pid,ret=$ret"
            else
                echo "process not exists.pid:$pidV"
            fi

        else
            echo "pid file $pidFile not exists."
        fi
    ;;
    *)
        echo invalid cmd $cmd
    ;;
esac
