package cn.whaley.bi.logsys.log2parquet.constant

/**
  * Created by michael on 2017/6/14.
  */
object Constants {
  val ODS_ORIGIN_HDFS_INPUT_PATH: String = "/data_warehouse/ods_origin.db/log_origin"
  val ODS_VIEW_HDFS_OUTPUT_PATH: String = "/data_warehouse/ods_view.db/t_log"
  val PATH_KEY_APPID="key_appId"
  val PATH_KEY_DAY="key_day"
  val PATH_KEY_HOUR="key_hour"
  val PATH_PRODUCT_CODE="productCode"
  val PATH_APP_CODE="appCode"
  val PATH_LOG_TYPE="logType"
  val PATH_EVENT_ID="eventId"
/**
  * /data_warehouse/ods_view.db/t_log
/productCode=..
/appCode=..
/logType=..
/eventId=..
/key_day=${yyyymmdd}
/key_hour=${HH}



  /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95k0000000000000000
/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kbqei6cc98dc5mlsr

  hadoop fs -ls /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kbqei6cc98dc5mlsr/key_day=20170614/key_hour=13
  * */

}
