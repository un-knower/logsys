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

#创建metadata库app_id_info表
$HIVE_HOME/bin/hive -e "drop table if exists metadata.app_id_info;
CREATE TABLE metadata.app_id_info (
  row_key string,
  app_id string  ,
  org_code string ,
  product_code string ,
  app_code string ,
  remark string ,
  topic_name string  ,
  topic_partition int

)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,app_info:app_id,app_info:org_code,app_info:product_code,
app_info:app_code,app_info:remark,topic_info:topic_name,topic_info:topic_partition')
TBLPROPERTIES ('hbase.table.name' = 'metadata.app_id_info');"