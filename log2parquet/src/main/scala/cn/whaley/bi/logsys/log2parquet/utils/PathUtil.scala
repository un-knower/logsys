package cn.whaley.bi.logsys.log2parquet.utils

import java.io.File
import cn.whaley.bi.logsys.log2parquet.constant.Constants

/**
  * Created by michael on 2017/6/14.
  */
object PathUtil {

  /*/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kbqei6cc98dc5mlsr/key_day=20170614/key_hour=13/boikgpokn78sb95kbqei6cc98dc5mlsr_2017061413_raw_5_14348.json.gz*/
  def getOdsOriginPath(appID: String, day: String, hour: String): String = {
    val path = Constants.ODS_ORIGIN_HDFS_INPUT_PATH + File.separator + Constants.PATH_KEY_APPID + "=" + appID + File.separator + Constants.PATH_KEY_DAY + "=" + day + File.separator + Constants.PATH_KEY_HOUR + "=" + hour
    path
  }

  /**
    * 通过传入的appId获得productCode和appCode值，完成路径拼接
    * 路径模版：
    * /data_warehouse/ods_view.db/t_log/productCode=../appCode=../logType=../eventId=../key_day=${yyyymmdd}/key_hour=${HH}
    */
  def getOdsViewPath(appID: String, day: String, hour: String, logType: String, eventId: String): String = {
    //todo get productCode and appCode from phoenix according appID field
    val productCode="test"
    val appCode="test"
    val path = Constants.ODS_VIEW_HDFS_OUTPUT_PATH + File.separator + Constants.PATH_PRODUCT_CODE + "=" + productCode + File.separator + Constants.PATH_APP_CODE + "=" + appCode +
      File.separator + Constants.PATH_LOG_TYPE + "=" + logType + File.separator + Constants.PATH_EVENT_ID + "=" + eventId +
      File.separator + Constants.PATH_KEY_DAY + "=" + day + File.separator + Constants.PATH_KEY_HOUR + "=" + hour
    path
  }

  def tableNameStandard(originalName:String):String={
    originalName.replace("-","_").replace(".","_")
  }

}
