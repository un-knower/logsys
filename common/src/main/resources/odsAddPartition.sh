#!/bin/bash


ARGS=`getopt -o a:d:h: --long appId:,day:,hour: -- "$@"`
if [ $? != 0 ]; then
    echo "parse args error"
    exit 1
fi

HIVE_HOME=/opt/hive
HADOOP_HOME=/opt/hadoop
appId=""
day=""
hour=""

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -a|--appId)
            appId=$2;
            shift 2;;
        -d|--day)
            day=$2;
            shift 2;;
        -h|--hour)
            hour=$2;
            shift 2;;
        --)
            shift;
            break;;

        *)
            echo "usage:
    ./odsAddPartition.sh --appId key_appId --day key_day --hour key_hour
  or
    ./odsAddPartition.sh --day key_day --hour key_hour
  or
    ./odsAddPartition.sh --day key_day

example:
    ./odsAddPartition.sh --appId boikgpokn78sb95kjhfrendoj8ilnoi7 --day 20170510 --hour 08
  or
    ./odsAddPartition.sh --day 20170510 --hour 08
  or
    ./odsAddPartition.sh --day 20170510"
            exit 1
            ;;
    esac
done

hiveSql="ALTER TABLE ods_origin.log_origin ADD IF NOT EXISTS "

if [ -n "$appId" -a -n "$day" -a -n "$hour" ]; then
    $HADOOP_HOME/bin/hadoop fs -test -e "/data_warehouse/ods_origin.db/log_origin/key_appId=$appId/key_day=$day/key_hour=$hour"
    if [ $? -eq 0 ]; then
        hiveSql=" PARTITION(key_appId='$appId',key_day='$day',key_hour='$hour') LOCATION '/data_warehouse/ods_origin.db/log_origin/key_appId=$appId/key_day=$day/key_hour=$hour' ";
    fi
elif [ -z "$appId" -a -n "$day" -a -n "$hour" ]; then
    for appIdLocation in `$HADOOP_HOME/bin/hadoop fs -ls "/data_warehouse/ods_origin.db/log_origin/key_appId=*/key_day=$day/key_hour=$hour" | awk '{print $8}'`
    do
        appId=${appIdLocation:51:32};
        hiveSql=$hiveSql" PARTITION(key_appId='$appId',key_day='$day',key_hour='$hour') LOCATION '/data_warehouse/ods_origin.db/log_origin/key_appId=$appId/key_day=$day/key_hour=$hour' ";

    done
elif [ -z "$appId" -a -n "$day" -a -z "$hour" ]; then
    for appIdLocation in `$HADOOP_HOME/bin/hadoop fs -ls -d  "/data_warehouse/ods_origin.db/log_origin/key_appId=*/key_day=$day/key_hour=*" | awk '{print $8}'`
    do
        appId=${appIdLocation:51:32};
        hour=${appIdLocation:110:2};

        hiveSql=$hiveSql" PARTITION(key_appId='$appId',key_day='$day',key_hour='$hour') LOCATION '/data_warehouse/ods_origin.db/log_origin/key_appId=$appId/key_day=$day/key_hour=$hour' ";

    done
else
    echo "usage:
    ./odsAddPartition.sh --appId key_appId --day key_day --hour key_hour
  or
    ./odsAddPartition.sh --day key_day --hour key_hour
  or
    ./odsAddPartition.sh --day key_day

example:
    ./odsAddPartition.sh --appId boikgpokn78sb95kjhfrendoj8ilnoi7 --day 20170510 --hour 08
  or
    ./odsAddPartition.sh --day 20170510 --hour 08
  or
    ./odsAddPartition.sh --day 20170510";
    exit 1;
fi

if [ ${#hiveSql} -gt 52 ]; then
    HIVE_HOME=/opt/hive

    $HIVE_HOME/bin/hive -e "$hiveSql"

fi