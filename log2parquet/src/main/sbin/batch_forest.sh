#!/bin/bash

cd `dirname $0`
pwd=`pwd`

ARGS=`getopt -o m:a:s:e:b:h:f --long mainClass:,appId:,startDate:,endDate:,startHour:,endHour:,filterContext: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -m|--mainClass)
            mainClass=$2;
            shift 2;;
        -a|--appId)
            appId=$2;
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
        -f|--filterContext)
            filterContext=$2;
            shift 2;;
        --)
            shift;
            break;;

        *)
            exit 1
            ;;
    esac
done

startTime=`date -d "$startDate $startHour -1 hour" +"%Y%m%d%H"`
endTime=`date -d "$endDate $endHour -1 hour" +"%Y%m%d%H"`

echo "filterContext ... ${filterContext}"

while [[ ${startTime}  -le  ${endTime} ]]
   do
    echo "execute time ... is ${startTime}"
    startDate=${startTime:0:8}
    startHour=${startTime:8:2}
    sh  ./submit_batch.sh  --mainClass ${mainClass} --appId ${appId} --startDate ${startDate} --startHour ${startHour} --filterContext ${filterContext}
    if [ $? -ne 0 ];then
            echo "batch forest ${startTime} is fail ..."
            exit 1
    fi
    startTime=`date -d "${startDate} ${startHour} 1 hour" +"%Y%m%d%H"`
done


