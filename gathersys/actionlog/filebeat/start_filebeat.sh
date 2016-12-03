#!/usr/bin/env bash

#----------------------help----------------------
current_bin_path="`dirname "$0"`"
usage="Usage: start_filebeat.sh appname,applist like: "
# if no args specified, show usage
if [ $# -lt 1 ]; then
  echo ${usage}
  cat ${current_bin_path}/applist| while read appname_var
  do
    echo "${appname_var}"
  done

  exit 1
fi

appname=$1
echo "current_bin_path is ${current_bin_path}"
cd ${current_bin_path}
nohup ./filebeat -c filebeat_${appname}.yml -e > nohup_${appname}.log 2>&1 &
