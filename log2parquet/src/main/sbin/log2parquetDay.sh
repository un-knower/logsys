#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`
echo "${pwd}"
service_bin_home="../sbin"

usage() { echo "Usage: $0 [-p <string> -d <string>]" 1>&2; exit 1; }

while getopts ":d:h:" o; do
    case "${o}" in
        p)
            p=${OPTARG}
            ;;
        d)
            d=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${d}" ]; then
    usage
fi

echo "day = ${d}"

#日期减去一天，获得新的day
newDate=`date -d "${d} -1 day" +"%Y%m%d"`
echo "newDate:${newDate}"

echo "path:${path},newDate:${newDate}"
inputPath=${path}/key_day=${newDate}
echo "${inputPath}"
sh ${service_bin_home}/submit.sh cn.whaley.bi.logsys.log2parquet.MainObj MsgProcExecutor --f MsgBatchManagerV3.xml,settings.properties --c inputPath=${inputPath}
