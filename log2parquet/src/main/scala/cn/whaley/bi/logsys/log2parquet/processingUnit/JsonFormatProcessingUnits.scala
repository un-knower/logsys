package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys._
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
  private val whiteList = List("XX1501VIPTEST00244", "PA1633A2000101A200", "XX1501VIPTEST00245", "PA1633A2000102A200", "XX1501VIPTEST00246", "PA1633A2000091A200", "XX1501VIPTEST00247", "PA1633A2000097A200", "XX1501VIPTEST00248", "PA1633A2000085A200", "XX1501VIPTEST00250", "PA1633A2000083A200", "XX1501VIPTEST00251", "PA1633A2000055A200", "XX1501VIPTEST00252", "PA1633A2000031A200", "XX1501VIPTEST00253", "PA1633A2000072A200", "XX1501VIPTEST00254", "PA1633A2000058A200", "XX1501VIPTEST00255", "PA1633A2000051A200", "XX1501VIPTEST00256", "PA1633A2000073A200", "XX1501VIPTEST00257", "PA1633A2000034A200", "XX1501VIPTEST00258", "PA1633A2000037A200", "XX1501VIPTEST00259", "PA1633A2000041A200", "XX1501VIPTEST00260", "PA1633A2000082A200", "XX1501VIPTEST00261", "PA1633A2000075A200", "XX1501VIPTEST00262", "PA1633A2000094A200", "XX1501VIPTEST00263", "PA1633A2000026A200", "XX1501VIPTEST00264", "PA1633A2000086A200", "XX1501VIPTEST00265", "PA1633A2000106A200", "XX1501VIPTEST00268", "PA1633A2000103A200", "XX1501VIPTEST00283", "PA1633A2000088A200")

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
      if (logBody.containsKey(LogKeys.LOG_PARAMS)) {
        logBody.get(LogKeys.LOG_PARAMS) match {
          // 处理params非json格式的情形
          case paramsInfo: String => {
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
          case paramsInfo: JSONObject => {
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


      //add at 20171114 for move old log2parquet logic to this script
      val appId = jsonObject.getString(LogKeys.LOG_APP_ID)
      //增加两个必填字段，确保历史程序可以正确执行
      if (appId == MEDUSA_3X_MAIN_APPID) {
        if (!jsonObject.containsKey("userId")) {
          jsonObject.put("userId", "")
        }
        if (!jsonObject.containsKey("deviceId")) {
          jsonObject.put("deviceId", "")
        }
      }

      //add logic for whaley
      if (appId.startsWith(WHALEY_TV_PRODUCT)) {
        logProcess(jsonObject)
      }
      //add at 20171114 for move old log2parquet logic to this script

      new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }

  def setUserRealIP(json: JSONObject): Unit = {
    val remoteIp = json.getString(REMOTE_IP)
    val forwardedIp = json.getString(FORWARDED_IP)
    if (forwardedIp != null) {
      regexIp findFirstIn forwardedIp match {
        case Some(ip) => json.put(REAL_IP, ip)
        case None => json.put(REAL_IP, remoteIp)
      }
    } else json.put(REAL_IP, remoteIp)
  }


  def logProcess(json: JSONObject): Unit = {
    val logType = getStringValue(json, LOG_TYPE)
    val productSN = getStringValue(json, PRODUCT_SN)
    var productLine = getStringValue(json, PRODUCT_LINE)
    val firmwareVersion = getStringValue(json, FIRMWARE_VERSION)
    val sysPlatform = getStringValue(json, SYS_PLATFORM)

    if (productSN != "" && productSN.startsWith("P") || productSN.startsWith("B") ||
      productSN.startsWith("A") || orcaWhiteList(productSN)) {
      productLine = "orca"
      json.put(PRODUCT_LINE, "orca")
    } else if (productLine == null || productLine.isEmpty) {
      productLine = "helios"
      json.put(PRODUCT_LINE, "helios")
    }

    //判断是否为yunos版本,1代表是,0代表不是,-1和2代表未知无法判断
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

    val event = getStringValue(json, EVENT)
    //修正OTA6中体育直播日志的bug，将logType由collect修正为live
    if (logType == "collect" && (event == "live" || event == "past")) {
      json.put(LOG_TYPE, "live")
    } else if (logType == "pageview" && json.containsKey("from")) {
      val source = getStringValue(json, "from")
      json.put("source", source)
      json.remove("from")
    } else if (logType == ""
      && json.containsKey("jsonlog")
      && getStringValue(json, "jsonlog").contains("playqos")
    ) {
      json.put(LOG_TYPE, "playqos")
    }
  }

  def getStringValue(json: JSONObject, key: String, defValue: String = ""): String = {
    val v = json.getString(key)
    if (v == null) defValue else v
  }

  /**
    * @deprecated
    * @return 临时白名单:虎鲸测试机
    */
  def orcaWhiteList(productsn: String) = {
    var flag = false
    for (i <- whiteList) {
      if (!flag) {
        if (productsn == i) flag = true
      }
    }
    flag
  }


}



