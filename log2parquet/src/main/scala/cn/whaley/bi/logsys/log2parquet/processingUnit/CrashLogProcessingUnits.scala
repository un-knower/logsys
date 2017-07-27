package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.{Constants, LogKeys}
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.{ProcessResult, ProcessResultCode}
import com.alibaba.fastjson.JSONObject
import org.apache.commons.codec.digest.DigestUtils


/**
  * Created by michael on 2017/7/27.
  *  crash日志处理器
  */
class CrashLogProcessingUnits extends LogProcessorTraitV2 with LogTrait {

  /**
    * 初始化
    */
  def init(confManager: ConfManager): Unit = {
  }

  /**
    * crash日志处理器
    */
  def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      var status=ProcessResultCode.processed
      if (jsonObject.containsKey(LogKeys.LOG_BODY_STACK_TRACE)) {
        if (jsonObject.containsKey(LogKeys.LOG_APP_ID) && jsonObject.getString(LogKeys.LOG_APP_ID).equalsIgnoreCase(Constants.MEDUSA3X_APP_ID)) {
          //medusa3x的crash日志
          val crashKey = jsonObject.getString("CRASH_KEY")
          val createDate = jsonObject.getString("USER_CRASH_DATE")
          val mac = jsonObject.getString("MAC")
          val productCode = jsonObject.getString("PRODUCT_CODE")
          val verificationStr = Constants.CRASH_KEY + crashKey + createDate + mac + productCode
          val verificationKey = DigestUtils.md5Hex(verificationStr)
          val md5 = jsonObject.getString("MD5")
          if (md5 == verificationKey) {
            val stackTraceStr = jsonObject.getString("STACK_TRACE")
            val stackTraceMd5 = DigestUtils.md5Hex(stackTraceStr)
            jsonObject.put("STACK_TRACE_MD5", stackTraceMd5)
          } else {
            status=ProcessResultCode.signFailure
          }
        }else{
          //非medusa3x的crash日志
          val stackTraceStr = jsonObject.getString("STACK_TRACE")
          val stackTraceMd5 = DigestUtils.md5Hex(stackTraceStr)
          jsonObject.put("STACK_TRACE_MD5",stackTraceMd5)
        }
      }
      new ProcessResult(this.name, status, "", Some(jsonObject))
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }
}



