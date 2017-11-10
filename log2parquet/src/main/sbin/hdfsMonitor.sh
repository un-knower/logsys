#!/bin/bash

cd `dirname $0`
pwd=`pwd`

ARGS=`getopt -o s:e:b:h --long startDate:,endDate:,startHour:,endHour: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
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
        flag=1
        time=1
        while (( $time > 0 ))
        do
          startCn=`hadoop fs -ls /run_log/ods_origin_logupload/${startTime}_bigdata-extsvr-log*_start | wc -l`
          endCn=`hadoop fs -ls /run_log/ods_origin_logupload/${startTime}_bigdata-extsvr-log*_end | wc -l`
          echo "startCn ... ${startCn}"
          echo "endCn ... ${endCn}"
          if (( $startCn != $endCn )) || (( $startCn == 0 )) ; then
              flag=0
           fi
          if (( $flag != 1 )) ;then
           sleep 300s
           time=$(($time - 1))
          else
           time=0
          fi
        done

         if (( $flag != 1 )) ;then
          echo "${startTime} fail ... startCn -> ${startCn},  endCn->${endCn}"
          exit 1
         fi

        startTime=`date -d "${startDate} ${startHour} 1 hour" +"%Y%m%d%H"`
done


