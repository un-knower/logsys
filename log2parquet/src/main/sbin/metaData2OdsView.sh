#!/bin/bash

cd `dirname $0`
pwd=`pwd`

ARGS=`getopt -o m:t:s:e:b:h --long mainClass:,taskFlag:,startDate:,endDate:,startHour:,endHour: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -m|--mainClass)
            mainClass=$2;
            shift 2;;
        -t|--taskFlag)
            taskFlag=$2;
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

dependenceDir=/data/apps/azkaban/forest_batch

for file in ${dependenceDir}/lib/*.jar
do
     if [ -n "$jarFiles" ]; then
         jarFiles="$jarFiles;$file"
     else
         jarFiles="$file"
     fi
done

mainjar="../lib/log2parquet-1.0-SNAPSHOT.jar"

jarFiles="$jarFiles;$mainjar"

echo "jarFiles ... ${jarFiles}"

inputPath="/data_warehouse/ods_view.db/"

while [[ ${startTime}  -le  ${endTime} ]]
   do
    echo "execute time ... is ${startTime}"
    startDate=${startTime:0:8}
    startHour=${startTime:8:2}
    #java -classpath $jarFiles  ${mainClass} ${inputPath} ${startDate}  ${startHour}
    sh  ./submit_metaData2OdsView.sh ${mainClass}  ${inputPath} ${startDate}  ${startHour}  ${taskFlag}  settings.properties
    if [ $? -ne 0 ];then
            echo "batch forest ${startTime} is fail ..."
            exit 1
    fi
    startTime=`date -d "${startDate} ${startHour} 1 hour" +"%Y%m%d%H"`
done


