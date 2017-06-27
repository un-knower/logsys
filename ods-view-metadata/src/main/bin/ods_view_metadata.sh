#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`

source ./envFn.sh

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
if [ -f "/data/apps/azkaban/share/libs/phoenix-4.10.0-HBase-1.2.jar" ]; then
    classpath="$classpath:/data/apps/azkaban/share/libs/phoenix-4.10.0-HBase-1.2.jar"
fi
if [ -f "/data/apps/azkaban/share/libs/hive-jdbc-2.1.1.jar" ]; then
    classpath="$classpath:/data/apps/azkaban/share/libs/hive-jdbc.jar"
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
 cn.whaley.bi.logsys.metadata.Application $@
