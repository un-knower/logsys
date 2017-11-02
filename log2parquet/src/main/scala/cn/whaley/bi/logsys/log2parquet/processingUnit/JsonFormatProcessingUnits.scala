package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.{Constants, LogKeys}
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.{ProcessResult, ProcessResultCode}
import com.alibaba.fastjson.JSONObject

import scala.collection.JavaConversions._


/**
  * Created by michael on 2017/6/22.
  *
  * 解析日志消息体,平展化logBody，生成"_msg"的json结构体
  *
  */
class JsonFormatProcessingUnits extends LogProcessorTraitV2 with LogTrait {

  private val REMOTE_IP = "svr_remote_addr"
  private val FORWARDED_IP = "svr_forwarded_for"
  private val REAL_IP = "realIP"
  private val IS_YUNOS = "isYunos"
  private val regexIp = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}".r

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
      //展开logBody
      val logBody = jsonObject.getJSONObject(LogKeys.LOG_BODY)
      //处理 params
      if(logBody.containsKey(LogKeys.LOG_PARAMS)){
        logBody.get(LogKeys.LOG_PARAMS) match {
          // 处理params非json格式的情形
          case paramsInfo:String => {
            if (paramsInfo.contains(",")) {
              val paramSplit = paramsInfo.split(",")
              if (paramSplit.nonEmpty) {
                paramSplit.foreach(parameterInfo => {
                  if (parameterInfo.contains("=")) {
                    val parameterSplit = parameterInfo.trim.split("=")
                    if (parameterSplit.length == 2) {
                      logBody.put(parameterSplit(0), parameterSplit(1))
                    }
                  }
                })
              }
            }
          }
          case paramsInfo:JSONObject => {
            paramsInfo.keys.foreach(i => {
              logBody.put(i, paramsInfo.get(i))
            })
          }
          case _ => System.err.println(s"ERROR: invalid attribute[${LogKeys.LOG_PARAMS}] string: ${logBody.toJSONString}")
        }
        logBody.remove(LogKeys.LOG_PARAMS)
      }

      jsonObject.asInstanceOf[java.util.Map[String, Object]].putAll(logBody.asInstanceOf[java.util.Map[String, Object]])
      jsonObject.remove(LogKeys.LOG_BODY)

      //realIp处理
      setUserRealIP(jsonObject)
      //yunos处理
      setYunos(jsonObject)
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

  def setUserRealIP(json:JSONObject):Unit = {
    val remoteIp = json.getString(REMOTE_IP)
    val forwardedIp = json.getString(FORWARDED_IP)
    if(forwardedIp != null){
      regexIp findFirstIn forwardedIp match {
        case Some(ip) => json.put(REAL_IP,ip)
        case None => json.put(REAL_IP,remoteIp)
      }
    }else json.put(REAL_IP,remoteIp)
  }
  //判断是否为yunos版本,1代表是,0代表不是,-1和2代表未知无法判断
  def setYunos(json:JSONObject):Unit = {
    val sysPlatform = json.getString(LogKeys.SYS_PLATFORM)
    val firmwareVersion = json.getString(LogKeys.FIRMWARE_VERSION)
    if (sysPlatform == "") {
      if (firmwareVersion != "" && firmwareVersion.length > 5) {
        val yunosFlag = firmwareVersion.substring(1, 3)
        if (yunosFlag == "01") json.put(IS_YUNOS, 1)
        else if (yunosFlag == "02") json.put(IS_YUNOS, 0)
        else json.put(IS_YUNOS, 2)
      } else json.put(IS_YUNOS, -1)
    } else {
      if (sysPlatform == "yunos") {
        json.put(IS_YUNOS, 1)
      } else if (sysPlatform == "nonyunos") {
        json.put(IS_YUNOS, 0)
      } else json.put(IS_YUNOS, 2)
    }
  }
}



