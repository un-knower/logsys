#!/usr/bin/env bash
####################################################################################
#
# usage:
#   ./odsAddPartition.sh appId day hour
#   day format yyyyMMdd
#   hour format HH
#
####################################################################################

if [ $# -ne 3 ]
then
  echo "usage:
  ./odsAddPartition.sh appId day hour
example:
  ./odsAddPartition.sh boikgpokn78sb95kjhfrendoj8ilnoi7 20170510 08";
  exit 1;
fi

appId=$1
day=$2
hour=$3

HIVE_HOME=/opt/hive

$HIVE_HOME/bin/hive -e "ALTER TABLE log_origin
ADD PARTITION(key_appId='$appId',key_day='$day',key_hour='$hour')
LOCATION '/data_warehouse/ods_origin.db/log_origin/key_appId=$appId/key_day=$day/key_hour=$hour'"