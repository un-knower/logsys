#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`
echo "${pwd}"
service_bin_home="../sbin"

usage() { echo "Usage: $0 [-d <string:day> -h <string:hour>  -f <string:taskFlag>]" 1>&2; exit 1; }

while getopts ":d:h:f:" o; do
    case "${o}" in
        d)
            d=${OPTARG}
            ;;
        h)
            h=${OPTARG}
            ;;
        f)
            f=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${d}" ] || [ -z "${h}" ] ||  [ -z "${f}" ]; then
    usage
fi


#日期减去一个小时，获得新的日期
newDate=`date -d "${d} ${h} -1 hour" +"%Y%m%d"`
newHour=`date -d "${d} ${h} -1 hour" +"%H"`
inputPath=/data_warehouse/ods_origin.db/log_origin/key_day=${newDate}/key_hour=${newHour}
##微鲸电视主程序
echo "inputPath:${inputPath},newDate:${newDate},taskFlag:${f}"
sh ${service_bin_home}/submit_log2parquet.sh cn.whaley.bi.logsys.log2parquet.MainObj MsgProcExecutor --f MsgBatchManagerV3.xml,settings.properties --c inputPath=${inputPath} --c startDate=${newDate} --c startHour=${newHour} --c taskFlag=${f}
