package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.{ProcessResult, ProcessResultCode}
import com.alibaba.fastjson.JSONObject


/**
  * Created by michael on 2017/6/22.
  *
  *将日志字段的string转为期望的类型
  *
  */
class FormatLogValueProcessingUnits extends LogProcessorTraitV2 with LogTrait {
  private val longTypeKeyList = List(LogKeys.ACCOUNT_ID, LogKeys.DURATION, LogKeys.SPEED, LogKeys.SIZE, LogKeys.PRE_MEMORY, LogKeys.POST_MEMORY)

  /**
    * 初始化
    */
  def init(confManager: ConfManager): Unit = {

  }

  private def formatLogValue(json:JSONObject, key:String):Unit = {
    if(longTypeKeyList.contains(key)){
      try {
        val valueStr = json.getString(key)
        if(valueStr != null){
          val trimStr = valueStr.trim()
          if(trimStr != ""){
            json.put(key,trimStr.toInt)
          }else json.put(key,0)
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
          json.put(key, 0)
        }
      }
    }
  }
  /**
    * 解析出realLogType，并校验realLogType是否有效
    *
    * @return
    */
  def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      val it=jsonObject.keySet().iterator()
      while (it.hasNext){
        val key=it.next()
        formatLogValue(jsonObject,key)
      }
      new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }
}


