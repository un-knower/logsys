#!/bin/bash

#################################################################
# whaleytv_wui2.0: boikgpokn78sb95kjhfrendoj8ilnoi7
#   ./launch_msgproc.sh start --appId=boikgpokn78sb95kjhfrendoj8ilnoi7
#   ./launch_msgproc.sh stop --appId=boikgpokn78sb95kjhfrendoj8ilnoi7
#################################################################

cd `dirname $0`
pwd=`pwd`

source ../bin/envFn.sh

cmd=$1
shift

load_args $*

if [ -z "$appId" ]; then
    echo "appId required."
    exit 1
fi

export pidFile=${pwd}/../logs/${appId}.pid
export logFile=/data/logs/forest/msgproc_${appId}.log
export gcFile=/data/logs/forest/gc_msgproc_${appId}.log


echo "pid file: ${pidFile}"
echo "log file: ${logFile}"
echo "gc file: ${gcFile}"

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

        topics="^log-raw-${appId}$"
        set -x
        nohup ../bin/launch_executor.sh MsgProcExecutor \
            --f MsgBatchManager.xml,settings.properties \
            --c prop.KafkaMsgSource.topics=$topics \
            >> ${logFile} 2>&1 &
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
