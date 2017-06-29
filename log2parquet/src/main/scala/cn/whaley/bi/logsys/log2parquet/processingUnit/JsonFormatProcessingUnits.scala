package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.{ProcessResult, ProcessResultCode, URLParser}
import com.alibaba.fastjson.JSONObject


/**
  * Created by michael on 2017/6/22.
  *
  * 解析日志消息体,平展化logBody，生成"_msg"的json结构体
  *
  */
class JsonFormatProcessingUnits extends LogProcessorTraitV2 with LogTrait {


  /**
    * 初始化
    */
  def init(confManager: ConfManager): Unit = {

  }

  /**
    * 解析日志消息体,平展化logBody，生成"_msg"的json结构体
    */
  def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      //保留
     /* val appId = jsonObject.getString(LogKeys.LOG_APP_ID)
      val logId = jsonObject.getString(LogKeys.LOG_LOG_ID)
      val logVersion = jsonObject.getString(LogKeys.LOG_LOG_VERSION)
      val logTime = jsonObject.getLongValue(LogKeys.LOG_LOG_TIME)
      val sync = jsonObject.getJSONObject(LogKeys.LOG_LOG_SYNC)*/






    /*  jsonObject.put(LogKeys.LOG_APP_ID, appId)
      jsonObject.put(LogKeys.LOG_LOG_ID, logId)
      jsonObject.put(LogKeys.LOG_LOG_VERSION, logVersion)
      jsonObject.put(LogKeys.LOG_LOG_TIME, logTime)
      jsonObject.put(LogKeys.LOG_LOG_SYNC, sync)*/

      //将被展开
      val logBody = jsonObject.getJSONObject(LogKeys.LOG_BODY)
      val logBodyKeySetIterator = logBody.keySet().iterator()
      while (logBodyKeySetIterator.hasNext) {
        val key = logBodyKeySetIterator.next()
        val value = logBody.get(key)
        jsonObject.put(key, value)
      }
      jsonObject.remove(LogKeys.LOG_BODY)


      //新建key为_msg的的json结构体，将如下字段放入json结构体中
      val logSignFlag = jsonObject.getIntValue(LogKeys.LOG_SIGN_FLAG)
      val msgSource = jsonObject.getString(LogKeys.LOG_MSG_SOURCE)
      val msgVersion = jsonObject.getString(LogKeys.LOG_MSG_VERSION)
      val msgSite = jsonObject.getString(LogKeys.LOG_MSG_SITE)
      val msgSignFlag = jsonObject.getIntValue(LogKeys.LOG_MSG_SIGN_FLAG)
      val msgId = jsonObject.getString(LogKeys.LOG_MSG_ID)
      val msgFormat = jsonObject.getString(LogKeys.LOG_MSG_FORMAT)

      val newJsonObjectMsg = new JSONObject()
      newJsonObjectMsg.put(LogKeys.LOG_SIGN_FLAG, logSignFlag)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_SOURCE, msgSource)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_VERSION, msgVersion)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_SITE, msgSite)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_SIGN_FLAG, msgSignFlag)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_ID, msgId)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_FORMAT, msgFormat)
      jsonObject.put(LogKeys.LOG_MSG_MSG, newJsonObjectMsg)

      jsonObject.remove(LogKeys.LOG_SIGN_FLAG)
      jsonObject.remove(LogKeys.LOG_MSG_SOURCE)
      jsonObject.remove(LogKeys.LOG_MSG_VERSION)
      jsonObject.remove(LogKeys.LOG_MSG_SITE)
      jsonObject.remove(LogKeys.LOG_MSG_SIGN_FLAG)
      jsonObject.remove(LogKeys.LOG_MSG_ID)
      jsonObject.remove(LogKeys.LOG_MSG_FORMAT)
      new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }

 /* def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      //保留
      val appId = jsonObject.getString(LogKeys.LOG_APP_ID)
      val logId = jsonObject.getString(LogKeys.LOG_LOG_ID)
      val logVersion = jsonObject.getString(LogKeys.LOG_LOG_VERSION)
      val logTime = jsonObject.getLongValue(LogKeys.LOG_LOG_TIME)
      val sync = jsonObject.getJSONObject(LogKeys.LOG_LOG_SYNC)


      //将被展开
      val logBody = jsonObject.getJSONObject(LogKeys.LOG_BODY)

      //新建key为_msg的的json结构体，将如下字段放入json结构体中
      val logSignFlag = jsonObject.getIntValue(LogKeys.LOG_SIGN_FLAG)
      val msgSource = jsonObject.getString(LogKeys.LOG_MSG_SOURCE)
      val msgVersion = jsonObject.getString(LogKeys.LOG_MSG_VERSION)
      val msgSite = jsonObject.getString(LogKeys.LOG_MSG_SITE)
      val msgSignFlag = jsonObject.getIntValue(LogKeys.LOG_MSG_SIGN_FLAG)
      val msgId = jsonObject.getString(LogKeys.LOG_MSG_ID)
      val msgFormat = jsonObject.getString(LogKeys.LOG_MSG_FORMAT)


      //create new jsonObject
      val newJsonObject = new JSONObject()

      newJsonObject.put(LogKeys.LOG_APP_ID, appId)
      newJsonObject.put(LogKeys.LOG_LOG_ID, logId)
      newJsonObject.put(LogKeys.LOG_LOG_VERSION, logVersion)
      newJsonObject.put(LogKeys.LOG_LOG_TIME, logTime)
      newJsonObject.put(LogKeys.LOG_LOG_SYNC, sync)

      val logBodyKeySetIterator = logBody.keySet().iterator()
      while (logBodyKeySetIterator.hasNext) {
        val key = logBodyKeySetIterator.next()
        val value = logBody.get(key)
        newJsonObject.put(key, value)
      }

      val newJsonObjectMsg = new JSONObject()
      newJsonObjectMsg.put(LogKeys.LOG_SIGN_FLAG, logSignFlag)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_SOURCE, msgSource)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_VERSION, msgVersion)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_SITE, msgSite)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_SIGN_FLAG, msgSignFlag)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_ID, msgId)
      newJsonObjectMsg.put(LogKeys.LOG_MSG_FORMAT, msgFormat)

      newJsonObject.put(LogKeys.LOG_MSG_MSG, newJsonObjectMsg)

      //logTime字段转化为day,hour字段 TODO
      new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }*/
}



