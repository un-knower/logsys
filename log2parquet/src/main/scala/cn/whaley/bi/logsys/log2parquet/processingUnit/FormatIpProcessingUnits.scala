package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.{ProcessResult, ProcessResultCode}
import com.alibaba.fastjson.JSONObject

/**
  * Created by guohao on 2017/9/14.
  *
  *根据remoteIp，forwardedIp 获取realIP 值
  *
  */
class FormatIpProcessingUnits extends LogProcessorTraitV2 with LogTrait {
  val regexIp = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}".r
  /**
    * 初始化
    */
  def init(confManager: ConfManager): Unit = {

  }
  /**
    * 根据remoteIp，forwardedIp 获取realIP 值
    * @return
    */
  def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      val remoteIp = jsonObject.getString(LogKeys.REMOTE_IP)
      val forwardedIp = jsonObject.getString(LogKeys.FORWARDED_IP)
      if(forwardedIp != null){
        regexIp findFirstIn forwardedIp match {
          case Some(ip) => jsonObject.put(LogKeys.REAL_IP,ip)
          case None => jsonObject.put(LogKeys.REAL_IP,remoteIp)
        }
      }else jsonObject.put(LogKeys.REAL_IP,remoteIp)
      new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }
}


