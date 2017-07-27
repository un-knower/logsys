package cn.whaley.bi.logsys.log2parquet.constant

/**
  * Created by michael on 2017/6/14.
  */
object Constants {
  val ODS_ORIGIN_HDFS_INPUT_PATH: String = "/data_warehouse/ods_origin.db/log_origin"
  val ODS_VIEW_HDFS_OUTPUT_PATH: String = "/data_warehouse/ods_view.db"
  val DATA_WAREHOUSE: String = "/data_warehouse"
  val PATH_KEY_APPID="key_appId"
  val PATH_KEY_DAY="key_day"
  val PATH_KEY_HOUR="key_hour"
  val PATH_PRODUCT_CODE="productCode"
  val PATH_APP_CODE="appCode"
  val PATH_LOG_TYPE="logType"
  val PATH_EVENT_ID="eventId"
  val ODS_VIEW_HDFS_OUTPUT_PATH_TMP="/log/default/parquet/ods_view"
  val ODS_VIEW_HDFS_OUTPUT_PATH_TMP_ERROR="/log/default/parquet/ods_view/error"



  val STRING_PERIOD="."
  val STRIKE_THROUGH="-"
  val UNDER_LINE="_"
  val EMPTY_STRING=""

  val MEDUSA2X_APP_ID="boikgpokn78sb95ktmsc1bnken8tuboa"
  val MEDUSA3X_APP_ID="boikgpokn78sb95ktmsc1bnkechpgj9l"
  val CRASH_KEY = "p&i#Hl5!gAo*#dwSa9sfluynvaoOKD3"
}
