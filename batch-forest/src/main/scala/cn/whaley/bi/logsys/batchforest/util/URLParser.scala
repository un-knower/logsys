package cn.whaley.bi.logsys.batchforest.util

import java.net.URLDecoder

import com.alibaba.fastjson.{JSONArray, JSONObject}

import scala.collection.JavaConversions._


/**
  * Created by fj on 16/11/13.
  */
class URLParser {


}


object URLParser {

  /**
    *
    * @param location    第一个？之前的部分
    * @param queryString 第一个？之后的部分
    */
  case class HttpURL(location: String, queryString: String)

  /**
    * 将Http地址解析成HttpURL对象
    *
    * @param url
    * @param needDecode 是否需要进行decode
    * @return
    */
  def parseHttpURL(url: String, needDecode: Boolean = true): HttpURL = {

    val decodedUrl =
      if (needDecode) {
        try {
          URLDecoder.decode(url, "utf-8")
        } catch {
          case _: Throwable => {
            url
          }
        }
      } else url

    val indexQuery = decodedUrl.indexOf('?')

    if (indexQuery >= 0) {
      HttpURL(decodedUrl.substring(0, indexQuery), decodedUrl.substring(indexQuery + 1))
    } else {
      HttpURL(url, "")
    }
  }

  /**
    * 解析http查询字符串为一个JSON对象
    *
    * @param queryString
    * @return 如果为空或长度为0，则返回一个空的JSONObject对象实例
    */
  def parseHttpQueryString(queryString: String): JSONObject = {
    val js = new JSONObject()
    if (queryString != null && queryString.nonEmpty) {
      val kvs = queryString.split("\\&")
      kvs.foreach(e => {
        val kv = e.split("=")
        if (kv.length == 2) {
          try {
            val value = kv(1)
            if (value.contains("%")) {
              putJsonValue(js, kv(0), URLDecoder.decode(kv(1), "utf-8"))
            } else putJsonValue(js, kv(0), kv(1))
          } catch {
            case _: Exception => {
              putJsonValue(js, kv(0), "")
            }
          }
        } else if(kv.length == 1) putJsonValue(js, kv(0), "")
      })
    }
    js
  }

  /**
    * json object put key 时判断是否存在，存在则以英文逗号拼接起来，否则直接放入
    *
    * @param jsObj 目标jsonobject
    * @param key   键
    * @param value 值
    */
  def putJsonValue(jsObj: JSONObject, key: String, value: String): Unit = {
    if (jsObj.containsKey(key)) {
      val existsValue = jsObj.getString(key)
      jsObj.put(key, s"$existsValue,$value")
    } else jsObj.put(key, value)
  }


}
