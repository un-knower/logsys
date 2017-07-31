#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`

source ./envFn.sh
source ./dateFn.sh


path='../conf'
for file in ../lib/*.jar
do
    path="$path:$file"
done
if [ -n "$CLASSPATH" ]; then
   classpath="$path:$CLASSPATH"
else
   classpath="$path"
fi

export pidFile=${pwd}/../logs/launch_tmpfile_committer.pid
export logFile=/data/logs/forest-dist/launch_tmpfile_committer.log
export gcFile=/data/logs/forest-dist/gc_launch_tmpfile_committer.log


javaXmx=${javaXmx:-4096m}
javaXms=${javaXms:-1024m}
java -Xmx${javaXmx} -Xms${javaXms} -server -XX:+UseParNewGC -XX:+UseConcMarkSweepGC \
 -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark \
 -XX:+DisableExplicitGC -Djava.awt.headless=true \
 -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDateStamps \
 -Xloggc:$gcFile \
 -Dlog4j.logFile=$logFile \
 -Dlog4j.configuration=file:$pwd/../conf/log4j.properties \
 -classpath $classpath  \
 cn.whaley.bi.logsys.forest.sinker.HdfsMsgSink /data_warehouse/ods_origin.db/tmp_log_origin/* \
  /data_warehouse/ods_origin.db/log_origin \
  org.apache.hadoop.io.compress.GzipCodec \
  &

if [ $? -eq 0 ]
then
    pid=$!
    if /bin/echo -n $pid > "$pidFile"
    then
        sleep 1
        echo "task started. pid $pid."
    else
        echo "failure to write pid file $pidFile"
        exit 1
    fi
else
    echo "server start failure"
    exit 1
fi


