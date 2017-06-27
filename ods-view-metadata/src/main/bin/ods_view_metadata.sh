#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`

source ./envFn.sh

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
if [ -f "/data/apps/azkaban/share/libs/phoenix-4.10.0-HBase-1.2-thin-client.jar" ]; then
    classpath="$classpath:/data/apps/azkaban/share/libs/phoenix-4.10.0-HBase-1.2-thin-client.jar"
fi
if [ -f "/data/apps/azkaban/share/libs/hive-jdbc.jar" ]; then
    classpath="$classpath:/data/apps/azkaban/share/libs/hive-jdbc.jar"
fi

javaXmx=${javaXmx:-4096m}
javaXms=${javaXms:-1024m}
set -x
java -Xmx${javaXmx} -Xms${javaXms} -server -XX:+UseParNewGC -XX:+UseConcMarkSweepGC \
 -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark \
 -XX:+DisableExplicitGC -Djava.awt.headless=true \
 -classpath $classpath  \
 cn.whaley.bi.logsys.metadata.Application $@
