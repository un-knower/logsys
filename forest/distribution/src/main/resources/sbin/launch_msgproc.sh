#!/bin/bash

cd `dirname $0`
pwd=`pwd`

source ../bin/envFn.sh

cmd=$1
shift

load_args $*

if [ "$cmd" != "start" ] && [ "$cmd" != "stop" ]; then
    echo "invalid cmd: $cmd"
    exit 1
fi

if [ -z "$taskName" ]; then
    echo "taskName required."
    exit 1
fi

if [ -z "$topicRegex" ] && [ "$cmd" == "start" ] ; then
    case "$taskName" in
        medusa)
            topicRegex='^log-raw-boikgpokn78sb95ktmsc1bnk.*$'
        ;;
        whaleytv)
            topicRegex='^log-raw-boikgpokn78sb95kjhfrendo.*$'
        ;;
        whaleyvr_orca)
            topicRegex='(^log-raw-boikgpokn78sb95kbqei6cc9.*$)|(^log-raw-boikgpokn78sb95kicggqhbk.*$)'
        ;;
        eagle_mobilehelper)
            topicRegex='(^log-raw-boikgpokn78sb95k7id7n8eb.*$)|(^log-raw-boikgpokn78sb95kjtihcg26.*$)'
        ;;
        crawler)
            topicRegex='^log-raw-boikgpokn78sb95kkls3bhmt.*$'
        ;;
        whaley_other)
            topicRegex='^log-raw-boikgpokn78sb95k0+$'
        ;;
        *)
            echo invalid taskName $taskName
            exit 1
    ;;
    esac
fi


if [ -z "$groupId" ]; then
    groupId="forest-dist-${taskName}"
fi
if [ -n "$taskId" ]; then
    taskName="${taskName}${taskId}"
fi

export pidFile=${pwd}/../logs/${taskName}.pid
export logFile=/data/logs/forest-dist/msgproc_${taskName}.log
export gcFile=/data/logs/forest-dist/gc_msgproc_${taskName}.log


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
        nohup ../bin/launch_executor.sh MsgProcExecutor \
            --f MsgBatchManager.xml,settings.properties \
            --c prop.KafkaMsgSource.topicRegex=$topicRegex \
            --c prop.kafka-consumer.group.id=${groupId}
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
