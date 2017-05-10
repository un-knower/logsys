#!/usr/bin/env bash
####################################################################################
#
# usage:
#   ./odsAddPartition.sh appId day hour
#   day format yyyyMMdd
#   hour format HH
#
####################################################################################
appId=$1
day=$2
hour=$3

HIVE_HOME=/opt/hive

$HIVE_HOME/bin/hive -e "ALTER TABLE log_origin
ADD PARTITION(key_appId='$appId',key_day='$day',key_hour='$hour')
LOCATION '/data_warehouse/ods_origin.db/log_origin/key_appId=$appId/key_day=$day/key_hour=$hour'"