#!/usr/bin/env bash

if [ $# -ne 1 ]
then
  echo "usage:
  ./startFilebeat.sh configFileName
example:
  ./startFilebeat.sh filebeat_log-raw-boikgpokn78sb95kjhfrendo.yml";
  exit 1;
fi

configFileName=$1
logFileName=${configFileName%.*}

FileBeatHome="$( cd "$( dirname "$0"  )" && cd .. && pwd  )"

pro_num=`ps -ef|grep $configFileName |grep -v grep|grep -v $0 | wc -l`
if [ ${pro_num} -gt 0 ]; then
  echo "filebeat config [$configFileName] is already in use,you need to check and kill it first."
  ps -ef|grep $configFileName |grep -v grep|grep -v $0
  exit 1
else
  nohup $FileBeatHome/filebeat -c $FileBeatHome/conf/$configFileName >> /data/logs/filebeat_v5/$logFileName.log 2>&1 &
fi
