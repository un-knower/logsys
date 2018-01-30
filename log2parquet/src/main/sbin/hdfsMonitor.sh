#!/bin/bash

cd `dirname $0`
pwd=`pwd`

ARGS=`getopt -o s:e:b:h: --long startDate:,endDate:,startHour:,endHour: -- "$@"`

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

endTime=`date -d "$startDate $startHour -1 hour" +"%Y%m%d%H"`
startTime=`date -d "$endDate $endHour -1 hour" +"%Y%m%d%H"`

while [[ ${startTime}  -ge  ${endTime} ]]
do
        echo "execute time ... is ${startTime}"
        startDate=${startTime:0:8}
        startHour=${startTime:8:2}
        echo "path is ... /run_log/ods_origin_logupload/${startTime}_bigdata-extsvr-log*_start "
        echo "path is ... /run_log/ods_origin_logupload/${startTime}_bigdata-extsvr-log*_end "
        time=20
        while (( $time > 0 ))
        do
         flag=1
          echo "time is ... $time"
          startCn=`hadoop fs -ls /run_log/ods_origin_logupload/${startTime}_bigdata-extsvr-log*_start | wc -l`
          endCn=`hadoop fs -ls /run_log/ods_origin_logupload/${startTime}_bigdata-extsvr-log*_end | wc -l`
          echo "startCn ... ${startCn}"
          echo "endCn ... ${endCn}"
          if (( $startCn != $endCn )) || (( $startCn == 0 )) ; then
              flag=0
          fi
          if (( $flag != 1 )) ;then
           sleep 60s
           time=$(($time - 1))
          else
           time=0
          fi
        done

         if (( startCn != 14 )) ;then
                  curl -d '{"sub":"hdfs 文件监控", "content":"时间 ${startTime} 。start 文件数:${startCn} , end 文件数:${endCn}","sendto":"peng.tao@whaley.cn,guo.hao@whaley.cn,lian.kai@whaley.cn"}' -H "Content-Type: application/json" -X POST http://10.19.15.127:5006/mail/api/v1.0/send
         fi

        # if (( $flag != 1 )) ;then
        #  echo "${startTime} fail ... startCn -> ${startCn},  endCn->${endCn}"
        #  exit 1
        # fi

        startTime=`date -d "${startDate} ${startHour} -1 hour" +"%Y%m%d%H"`
done


