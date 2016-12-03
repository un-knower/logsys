#!/usr/bin/env bash

#----------------------help----------------------
current_bin_path="`dirname "$0"`"
usage="Usage: start_logstash.sh app_name "
# if no args specified, show usage
if [ $# -lt 1 ]; then
  echo ${usage}
  exit 1
fi

app_name=$1
LOGSTASH_HOME="$( cd "$( dirname "$0"  )" && cd .. && pwd  )"
echo ${LOGSTASH_HOME}
cd ${LOGSTASH_HOME}
nohup ${LOGSTASH_HOME}/bin/logstash -f ${LOGSTASH_HOME}/conf/logstash_${app_name}.conf --auto-reload -w 2 -b 200 > $LOGSTASH_HOME/logs/logstash_${app_name}.log 2>&1 &
