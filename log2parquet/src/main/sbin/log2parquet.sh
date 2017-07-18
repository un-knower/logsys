#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`
echo "${pwd}"
service_bin_home="../sbin"

ARGS=`getopt -o p:d:m:j:t:f -l path:,startDate:,startHour:,isJsonDirDelete:,isTmpDirDelete:,taskFlag: -- "$@"`

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
        -j|--isJsonDirDelete)
            isJsonDirDelete=$2;
            shift 2;;
        -t|--isTmpDirDelete)
            isTmpDirDelete=$2;
            shift 2;;
        -f|--taskFlag)
            taskFlag=$2;
            shift 2;;
        --)
            shift;
            break;;
        *)
            exit 1
            ;;
    esac
done

newDate=`date -d "${startDate} ${startHour} -1 hour" +"%Y%m%d"`
newHour=`date -d "${startDate} ${startHour} -1 hour" +"%H"`

echo "path:${path},newDate:${newDate},newHour:${newHour},isJsonDirDelete:${isJsonDirDelete},isTmpDirDelete:${isTmpDirDelete},taskFlag:${taskFlag}"
inputPath=${path}/key_day=${newDate}/key_hour=${newHour}
echo "${inputPath}"
sh ${service_bin_home}/submit.sh cn.whaley.bi.logsys.log2parquet.MainObj MsgProcExecutor --f MsgBatchManagerV3.xml,settings.properties --c inputPath=${inputPath} --c isJsonDirDelete=${isJsonDirDelete} --c isTmpDirDelete=${isTmpDirDelete} --c taskFlag=${taskFlag}
