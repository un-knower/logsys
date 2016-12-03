#!/usr/bin/env bash

#----------------------help----------------------
usage="Usage: sh auto_gen_logstash_config.sh app_name app_port app_topic"
# if no args specified, show usage
if [ $# -lt 3 ]; then
  echo ${usage}
  exit 1
fi

app_name=$1
app_port=$2
app_topic=$3
echo "app_name:${app_name},app_port:${app_port},app_topic:${app_topic}"

current_bin_path="`dirname "$0"`"

echo "update appname"
new_app_filename=${current_bin_path}/logstash_${app_name}.conf
cp ${current_bin_path}/logstash_template.conf ${new_app_filename}
sed -i "s/app_name/${app_name}/g" ${new_app_filename}

echo "update port"
sed -i "s/app_port/${app_port}/g" ${new_app_filename}

echo "update topic"
sed -i "s/app_topic/${app_topic}/g" ${new_app_filename}

echo "check if port is used"
port_count=`netstat -nltp|grep ${app_port}|wc -l`
if [ ${port_count} -gt 0 ]; then
  echo "${app_port} is already used,please changed another port"
  exit 1
fi
