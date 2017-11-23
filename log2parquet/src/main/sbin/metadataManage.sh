#!/bin/bash

cd `dirname $0`
pwd=`pwd`

ARGS=`getopt -o u:p:i:d:t:c:a:r:s:e:b:h --long username:,password:,path:,dbName:,tabPrefix:,productCode:,appCode:,realLogType:,startDate:,endDate:,startHour:,endHour: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -u|--username)
            username=$2;
            shift 2;;
        -p|--password)
            password=$2;
            shift 2;;
        -i|--path)
            path=$2;
            shift 2;;
        -d|--dbName)
            dbName=$2;
            shift 2;;
        -t|--tabPrefix)
            tabPrefix=$2;
            shift 2;;
        -c|--productCode)
            productCode=$2;
            shift 2;;
        -a|--appCode)
            appCode=$2;
            shift 2;;
        -r|--realLogType)
            realLogType=$2;
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



while [[ ${startTime}  -le  ${endTime} ]]
   do
    echo "execute time ... is ${startTime}"
    startDate=${startTime:0:8}
    startHour=${startTime:8:2}
    sh ./curl.sh metadataManage metadataManage ${username} ${password} path ${path} dbName ${dbName} tabPrefix ${tabPrefix} productCode ${productCode} appCode ${appCode} realLogType ${realLogType} keyDay ${startDate} keyHour ${startHour}
    if [ $? -ne 0 ];then
            echo "batch forest ${startTime} is fail ..."
            exit 1
    fi
    startTime=`date -d "${startDate} ${startHour} 1 hour" +"%Y%m%d%H"`
done


