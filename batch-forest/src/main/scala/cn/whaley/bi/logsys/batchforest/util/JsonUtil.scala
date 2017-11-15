package cn.whaley.bi.logsys.batchforest.util

import cn.whaley.bi.logsys.batchforest.process.PostProcess.LOG
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  * Created by guohao on 2017/11/15.
  */
object JsonUtil {

  /**
    * 解析字符串为JSONObject对象
    * @param str
    * @return ，如果失败返回None
    */
   def parseObject(str: String): Option[JSONObject] = {
    try {
      Some(JSON.parseObject(str))
    } catch {
      case e: Throwable => {
        LOG.error("", e)
        None
      }
    }
  }

  /**
    * 解析字符串为JSONArray对象
    * @param str
    * @return 如果失败返回None
    */
   def parseArray(str: String): Option[JSONArray] = {
    try {
      Some(JSON.parseArray(str))
    } catch {
      case e: Throwable => {
        LOG.debug("invalid jsonarray str:" + str, e)
        None
      }
    }
  }

}
