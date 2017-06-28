package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.utils.DateFormatUtils
import cn.whaley.bi.logsys.log2parquet.{ProcessResult, ProcessResultCode}
import com.alibaba.fastjson.JSONObject


/**
  * Created by michael on 2017/6/22.
  *
  */
class LogTimeProcessingUnits extends LogProcessorTraitV2 with LogTrait {
  /**
    * 初始化
    */
  def init(confManager: ConfManager): Unit = {

  }

  /**
    * 通过logTime字段解析出day和hour字段，后期在拼接输出路径的时候使用
    *
    * @return
    */
  def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      if(jsonObject.containsKey(LogKeys.LOG_LOG_TIME)){
        val logTime=jsonObject.getLong(LogKeys.LOG_LOG_TIME)
        val day=DateFormatUtils.readFormat.format(logTime)
        val hour=DateFormatUtils.readFormatHour.format(logTime)
        jsonObject.put(LogKeys.LOG_KEY_DAY,day)
        jsonObject.put(LogKeys.LOG_KEY_HOUR,hour)
        new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
      }else{
        //没有logTime字段，丢弃这条日志
        new ProcessResult(this.name, ProcessResultCode.discard, "no logTime field", Some(jsonObject))
      }
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }
}


