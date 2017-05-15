#!/usr/bin/env bash
####################################################################################
#
# usage:
#   ./odsCreateTable.sh
#
####################################################################################
HIVE_HOME=/opt/hive

$HIVE_HOME/bin/hive -e "CREATE EXTERNAL TABLE ods_origin.log_origin(
     \`_sync\` map<string,string>,
     msgId string,
     msgVersion string,
     msgSite string,
     msgSource string,
     msgFormat string,
     msgSignFlag int,
     logId string,
     logVersion string,
     logTime bigint,
     logSignFlag int,
     appId string,
     logBody map<string,string>
)
COMMENT 'ods origin data hive table'
PARTITIONED BY (key_appId string,key_day string,key_hour string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.row.decode.require' = 'true'
  ,'serialization.row.decode.allow_unquoted_backslash' = 'true'
)
STORED AS TEXTFILE
LOCATION '/data_warehouse/ods_origin.db/log_origin'"