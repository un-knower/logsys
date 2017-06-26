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
  *解析出realLogType，并校验realLogType是否有效
  *
  */
class RealLogTypeProcessingUnits extends LogProcessorTraitV2 with LogTrait {
  private val regexLogType = "^[\\w\\-]{2,100}$".r

  /**
    * 初始化
    */
  def init(confManager: ConfManager): Unit = {

  }

  def getStringValue(json:JSONObject,key:String,defValue:String=""):String={
    val v=json.getString(key)
    if(v==null) defValue else v
  }

  def isValidLogType(logType:String):Boolean = {
    if(logType != null && logType.nonEmpty) {
      regexLogType findFirstIn logType match {
        case Some(_) => true
        case None => false
      }
    }else false
  }
  /**
    * 解析出realLogType，并校验realLogType是否有效
    *
    * @return
    */
  def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      if(jsonObject.containsKey(LogKeys.LOG_LOG_TYPE)){
        val logType=jsonObject.getString(LogKeys.LOG_LOG_TYPE)
        val realLogType=if(LogKeys.LOG_EVENT.equalsIgnoreCase(logType)){
          getStringValue(jsonObject,LogKeys.LOG_EVENT_ID)
        }else if(LogKeys.LOG_START_END.equalsIgnoreCase(logType)){
          getStringValue(jsonObject,LogKeys.LOG_ACTION_ID)
        }else{
          logType
        }

        if(isValidLogType(realLogType)){
          new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
        }else{
          //do nothing
          new ProcessResult(this.name, ProcessResultCode.discard, "", Some(jsonObject))
        }

      }else{
        //do nothing
        new ProcessResult(this.name, ProcessResultCode.skipped, "", Some(jsonObject))
      }
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }
}


