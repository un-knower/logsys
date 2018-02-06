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

if [ -f "/data/apps/azkaban/share/libs/hive-jdbc.jar" ]; then
    classpath="$classpath:/data/apps/azkaban/share/libs/hive-jdbc.jar"
fi

javaXmx=${javaXmx:-4096m}
javaXms=${javaXms:-1024m}
set -x
 scala  -cp $classpath  cn.whaley.bi.logsys.metadata.Application $@
