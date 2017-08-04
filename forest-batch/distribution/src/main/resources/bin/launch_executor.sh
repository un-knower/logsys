#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`

source ./envFn.sh
source ./dateFn.sh

executor=$1
shift
load_args $*

if [ -z "$executor" ]; then
    echo "executor required!"
fi
if [ -z "${pidFile}" ]; then
    echo "pidFile required!"
fi
if [ -z "$logFile" ]; then
    echo "logFile required!"
fi
if [ -z "$gcFile" ]; then
    echo "gcFile required!"
fi


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
 cn.whaley.bi.logsys.forest.MainObj $executor $@ \
 &

if [ $? -eq 0 ]
then
    pid=$!
    if /bin/echo -n $pid > "$pidFile"
    then
        sleep 1
        echo "task started. pid $pid."
        echo "cn.whaley.bi.logsys.forest.MainObj ${executor} $@"
    else
        echo "failure to write pid file $pidFile"
        exit 1
    fi
else
    echo "server start failure"
    exit 1
fi