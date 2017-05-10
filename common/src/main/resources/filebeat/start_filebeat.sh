#!/usr/bin/env bash

if [ $# -ne 1 ]
then
  echo "usage:
  ./star_filebeat.sh configFileName(without .yml suffix)
example:
  ./star_filebeat.sh filebeat_log-raw-boikgpokn78sb95kjhfrendo";
  exit 1;
fi

configFileName=$1

FileBeatHome="$( cd "$( dirname "$0"  )" && cd .. && pwd  )"

pro_num=`ps -ef|grep $configFileName.yml |grep -v grep|wc -l`
echo "pro_num is $pro_num"
if [ ${pro_num} -gt 0 ]; then
  echo "filebeat config [configFileName] is already in use,you need to check and kill it first."
  exit 1
else
  nohup $FileBeatHome/filebeat -c $FileBeatHome/conf/$configFileName.yml >> /data/logs/filebeat/$configFileName.log 2>&1 &
fi
