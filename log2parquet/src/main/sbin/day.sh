#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`
echo "${pwd}"

ARGS=`getopt -o f:t:s:e:b:h:a --long filterContext:,tableName:,startDate:,endDate:,startHour:,endHour:,appId: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -f|--filterContext)
            filterContext=$2;
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

   sh  ./submit_batch.sh  --mainClass cn.whaley.bi.logsys.batchforest.MainObj --appId ${appId} --startDate ${startDate} --startHour ${startHour} --filterContext ${filterContext}
    if [ $? -ne 0 ];then
            echo "batch_forest ${startTime} is fail ..."
            exit 1
    fi
    inputPath=/data_warehouse/ods_origin.db/log_origin/key_day=${startDate}/key_hour=${startHour}
    sh  ./submit_log2parquet.sh  cn.whaley.bi.logsys.log2parquet.MainObj  MsgProcExecutor --f MsgBatchManagerV3.xml,settings.properties --c inputPath=${inputPath} --c startDate=${startDate} --c startHour=${startHour} --c tableName=${tableName} --c appId=${appId}
    ##sh ./log2parquet.sh --mainClass cn.whaley.bi.logsys.log2parquet.MainObj --startDate ${startDate} --startHour ${startHour} --endDate ${startDate} --endHour ${startHour}  --realLogType ${realLogType} --appId ${appId}
    if [ $? -ne 0 ];then
            echo "log2parquet ${startTime} is fail ..."
            exit 1
    fi

    startTime=`date -d "${startDate} ${startHour} -1 hour" +"%Y%m%d%H"`
done



