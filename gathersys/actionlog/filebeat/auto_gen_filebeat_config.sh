#!/usr/bin/env bash

#----------------------help----------------------
usage="Usage: sh auto_gen_filebeat_config.sh appname"
# if no args specified, show usage
if [ $# -lt 1 ]; then
  echo ${usage}
  exit 1
fi

appname=$1
current_bin_path="`dirname "$0"`"

echo "update appname"
new_app_filename=${current_bin_path}/filebeat_${appname}.yml
cp ${current_bin_path}/filebeat_template.yml ${new_app_filename}
sed -i "s/app_name/${appname}/g" ${new_app_filename}

echo "update port"
port_file=${current_bin_path}/ports
last_app_port=`head -1 ${port_file}`
let new_port=$last_app_port+1
sed -i "s/app_port/${new_port}/g" ${new_app_filename}

#update ports file
echo ${new_port} > ${port_file}

echo "check if port is used"
port_count=`netstat -nltp|grep ${new_port}|wc -l`
if [ ${port_count} -gt 0 ]; then
  echo "${new_port} is already used,please changed another port"
  exit 1
fi

