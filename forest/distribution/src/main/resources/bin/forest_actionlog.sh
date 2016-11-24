#!/usr/bin/env bash

cd `dirname $0`

source ./envFn.sh
source ./dateFn.sh
load_args $*

pwd=`pwd`
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
