#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`
echo "${pwd}"

ARGS=`getopt -o m:t:s:e:b:h:a --long mainClass:,tableName:,startDate:,endDate:,startHour:,endHour:,appId: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -m|--mainClass)
            mainClass=$2;
            shift 2;;
        -t|--tableName)
            tableName=$2;
            shift 2;;
		-s|--startDate)
            startDate=$2;
            shift 2;;
        -e|--endDate)
            endDate=$2;
            shift 2;;
        -b|--startHour)
            startHour=$2;
            shift 2;;
        -h|--endHour)
            endHour=$2;
            shift 2;;
        -a|--appId)
            appId=$2;
            shift 2;;
        --)
            shift;
            break;;

        *)
            exit 1
            ;;
    esac
done

endTime=`date -d "$startDate $startHour -1 hour" +"%Y%m%d%H"`
startTime=`date -d "$endDate $endHour -1 hour" +"%Y%m%d%H"`

while [[ ${startTime}  -ge  ${endTime} ]]
   do
    echo "execute time ... is ${startTime}"
    startDate=${startTime:0:8}
    startHour=${startTime:8:2}
    inputPath=/data_warehouse/ods_origin.db/log_origin/key_day=${startDate}/key_hour=${startHour}
    echo "inputPath:${inputPath},startDate:${startDate},startHour:${startHour},taskFlag:${taskFlag}"
    sh  ./submit_log2parquet.sh ${mainClass} MsgProcExecutor --f MsgBatchManagerV3.xml,settings.properties --c inputPath=${inputPath} --c startDate=${startDate} --c startHour=${startHour} --c tableName=${tableName} --c appId=${appId}
    if [ $? -ne 0 ];then
            echo "log2parquet ${startTime} is fail ..."
            exit 1
    fi
    startTime=`date -d "${startDate} ${startHour} -1 hour" +"%Y%m%d%H"`
done



