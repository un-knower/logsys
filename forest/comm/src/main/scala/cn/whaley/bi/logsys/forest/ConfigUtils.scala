package cn.whaley.bi.logsys.forest

/**
  * Created by lituo on 2018/1/4.
  */

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.dianping.lion.{EnvZooKeeperConfig, Environment}
import com.dianping.lion.client.ConfigCache
import com.dianping.lion.client.LionException
import java.util


object ConfigUtils {
  private var configCache: ConfigCache = null

  def getInteger(key: String, defaultValue: Integer): Integer = {
    var result = defaultValue
    try {
      val value = get(key)
      if (value == null) return result
      result = value.toInt
    } catch {
      case e: Exception =>

    }
    result
  }

  def getStringListFromJson(key: String): util.List[String] = {
    val result = new util.ArrayList[String]
    try {
      val value = get(key)
      if (value == null) return result
      val jsonArray = JSON.parseArray(value)
      var i = 0
      while ( {
        i < jsonArray.size
      }) {
        result.add(jsonArray.getString(i))

        {
          i += 1; i - 1
        }
      }
    } catch {
      case e: Exception =>

    }
    result
  }

  def get(key: String): String = {
    if (null == configCache && !initailize) null
    else try
      configCache.getProperty(key)
    catch {
      case var2: LionException =>
        null
    }
  }

  def getMachineId: String = {
    if (null == configCache) initailize
    Environment.getEnv + "-" + Environment.getMachineId
  }

  private def initailize = try {
    configCache = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress)
    true
  } catch {
    case var1: LionException =>
      false
  }
}

class ConfigUtils private() {
}
