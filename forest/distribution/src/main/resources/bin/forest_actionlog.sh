#!/usr/bin/env bash

cd `dirname $0`

source ./envFn.sh
source ./dateFn.sh
load_args $*

pwd=`pwd`
pid_file=$pwd/../logs/forest_actionlog.pid

cmd=$cmd

case $1 in
    start)

        classpath='../conf'
        for file in ../lib/*.jar
        do
              #echo add jar file: $file
              classpath="$classpath:$file"
        done

        #echo $classpath

        log4j_file_out_path=$pwd/../logs/forest_actionlog.log
        touch ${log4j_file_out_path}
        touch $pwd/../logs/forest_actionlog_gc.log
        echo "log file: ${log4j_file_out_path}"

        nohup java -Xmx512m -Xms256m -server -XX:+UseParNewGC -XX:+UseConcMarkSweepGC \
         -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark \
         -XX:+DisableExplicitGC -Djava.awt.headless=true \
         -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDateStamps \
         -Xloggc:$pwd/../logs/forest_actionlog_gc.log \
         -Dlog4j.file.out.path="${log4j_file_out_path}" \
         -Dlog4j.configuration=file:$pwd/../conf/log4j.properties \
         -classpath $classpath  \
         cn.whaley.bi.logsys.forest.MainObj MsgProcExecutor $@ \
         &



        if [ $? -eq 0 ]
        then
            pid=$!
            if /bin/echo -n $pid > "$pid_file"
            then
                sleep 1
                echo "forest_actionlog task started.pid $pid"
            else
                echo "failure to write pid file $pid_file"
                exit 1
            fi
        else
            echo "server start failure"
            exit 1
        fi

        ;;
    stop)

        if [ -f $pid_file ]; then
            pid=`cat $pid_file`
            kill $pid
            echo "kill process $pid"
        else
            echo "pid file $pid_file not exists."
        fi

        ;;
    *)
        echo "Usage: $0 {start|stop}" >&2
esac