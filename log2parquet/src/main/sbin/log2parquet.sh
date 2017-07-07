#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`
echo "${pwd}"
service_bin_home="../sbin"

ARGS=`getopt -o p:d:m -l path:,startDate:,startHour: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"
while true
do
    case "$1" in
        -p|--path)
            path=$2;
            shift 2;;
		-d|--startDate)
            startDate=$2;
            shift 2;;
        -m|--startHour)
            startHour=$2;
            shift 2;;
        --)
            shift;
            break;;
        *)
            exit 1
            ;;
    esac
done

echo "path:${path},startDate:${startDate},startHour:${startHour}"
inputPath=${path}/key_day=${startDate}/key_hour=${startHour}
echo "${inputPath}"
sh ${service_bin_home}/submit.sh cn.whaley.bi.logsys.log2parquet.MainObj MsgProcExecutor --f MsgBatchManagerV3.xml,settings.properties --c inputPath=${inputPath}
