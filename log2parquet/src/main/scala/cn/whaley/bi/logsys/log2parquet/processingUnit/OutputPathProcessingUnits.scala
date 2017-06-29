package cn.whaley.bi.logsys.log2parquet.processingUnit

import java.io.File

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.{Constants, LogKeys}
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.{MsgBatchManagerV3, ProcessResult, ProcessResultCode}
import com.alibaba.fastjson.JSONObject


/**
  * Created by michael on 2017/6/22.
  *
  * 对每条日志解析出outputPath，并用此字段(outputPathTemplate,jsonObject.toString) 返回RDD，然后依据outputPath进行聚合操作
  *
  */
class OutputPathProcessingUnits extends LogProcessorTraitV2 with LogTrait {

  /**
    * 初始化
    */
  def init(confManager: ConfManager): Unit = {

  }


  /**
    * 对每条日志解析出outputPath
    */
  def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      val appId = jsonObject.getString(LogKeys.LOG_APP_ID)
      var outputPathTemplate = MsgBatchManagerV3.appId2OutputPathTemplateMapBroadCast.value.get(appId).get
      //boikgpokn78sb95ktmsc1bnkechpgj9l->log_medusa_main3x_${log_type}_${event_id}/key_day=${key_day}/key_hour=${key_hour}

      if (outputPathTemplate.nonEmpty) {
        if (outputPathTemplate.contains("${log_type}") && jsonObject.containsKey(LogKeys.LOG_BODY_LOG_TYPE)) {
          val logType = jsonObject.getString(LogKeys.LOG_BODY_LOG_TYPE)
          outputPathTemplate = outputPathTemplate.replace("${log_type}", logType)
        } else {
          outputPathTemplate = outputPathTemplate.replace("_${log_type}", "")
        }

        if (outputPathTemplate.contains("${event_id}") && jsonObject.containsKey(LogKeys.LOG_BODY_REAL_LOG_TYPE)) {
          val realLogType = jsonObject.getString(LogKeys.LOG_BODY_REAL_LOG_TYPE)
          outputPathTemplate = outputPathTemplate.replace("${event_id}", realLogType)
        } else {
          outputPathTemplate = outputPathTemplate.replace("_${event_id}", "")
        }

        if (outputPathTemplate.contains("${key_day}") && jsonObject.containsKey(LogKeys.LOG_KEY_DAY)) {
          val key_day = jsonObject.getString(LogKeys.LOG_KEY_DAY)
          outputPathTemplate = outputPathTemplate.replace("${key_day}", key_day)
        } else {
          outputPathTemplate = outputPathTemplate.replace("key_day=${key_day}", "")
        }

        if (outputPathTemplate.contains("${key_hour}") && jsonObject.containsKey(LogKeys.LOG_KEY_HOUR)) {
          val key_hour = jsonObject.getString(LogKeys.LOG_KEY_HOUR)
          outputPathTemplate = outputPathTemplate.replace("${key_hour}", key_hour)
        } else {
          outputPathTemplate = outputPathTemplate.replace("key_hour=${key_hour}", "")
        }
        jsonObject.put(LogKeys.LOG_OUTPUT_PATH, Constants.ODS_VIEW_HDFS_OUTPUT_PATH+File.separator+outputPathTemplate)
      }
      new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }
}


