package cn.whaley.bi.logsys.batchforest.process

import cn.whaley.bi.logsys.batchforest.StringDecoder
import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}
import com.alibaba.fastjson.JSONObject

/**
  * Created by guohao on 2017/9/19.
  */
object LogFormat extends NameTrait with LogTrait{
  val decoder = new StringDecoder()
  /**
    * 消息解码，从字节数组解析为归集层消息体格式
    * @return
    */
  def decode(str: String) = {
    try {

      Some(decoder.decodeToString(str))
    } catch {
      case e: Throwable => {
        LOG.error(s"解码异常"+e.getMessage)
        None
      }
    }

  }

  /**
    * 解析归集层msgBody为日志消息体
    * 在原来的处理方式有logtime字段，
    * @param line
    * @return
    */
  def verificationFormat(line: JSONObject) ={
    if (line.getString("msgSource") == "ngx_log" && line.getString("msgFormat") == "json") {
      val msgBody = line.getJSONObject("msgBody")
      //msgBody为空返回None
      if(msgBody.keySet().size()==0 || !msgBody.isInstanceOf[JSONObject]){
        None
      }else{
        val body = msgBody.getJSONObject("body")
        if(body == null){
          None
        }else{
          Some(line)
        }
      }
    }else{
      None
    }
  }



  /**
    * 转移并展开属性值
    * 如果fromProp为空,则不进任何处理,直接返回to
    * 如果fromProp为JSONObject,如果toKey为空,则展开fromProp到to,否则展开到toProp
    * 如果fromProp为非JSONObject,则要求toKey不能为空,且将toProp设置为fromProp
    * @param from
    * @param fromKey
    * @param to
    * @param toKey
    */
  def translateProp(from: JSONObject, fromKey: String, to: JSONObject, toKey: String): JSONObject ={
    val fromProp = from.get(fromKey)
    //    from.remove(fromKey)

    //如果fromProp为空，则不做任何处理
    if(fromProp == null || (fromProp.isInstanceOf[String] && fromProp.asInstanceOf[String].isEmpty)){
      return to
    }
    if (fromProp.isInstanceOf[JSONObject]) {
      if (toKey != null && !toKey.isEmpty) {
        val toProp = if (to.get(toKey) != null && to.get(toKey).isInstanceOf[JSONObject]) {
          to.getJSONObject(toKey)
        } else {
          new JSONObject()
        }
        toProp.asInstanceOf[java.util.Map[String, Object]].putAll(fromProp.asInstanceOf[JSONObject])
        to.put(toKey, toProp)
      }else{
        to.asInstanceOf[java.util.Map[String, Object]].putAll(fromProp.asInstanceOf[JSONObject])
      }

    }else{
      assert(toKey != null && !toKey.isEmpty)
      to.put(toKey, fromProp)
    }
    to
  }

  def handleTime(log:JSONObject): JSONObject ={
    val logTime = log.getLong("logTime")
    val time = if(log.getJSONObject("logBody").containsKey("happenTime")){
      val happenTime = try{
        log.getJSONObject("logBody").getLong("happenTime")
      }catch {
        case ex: Throwable => {
          null
        }
      }
      if(happenTime !=null && ((logTime - happenTime)<= 10 * 60 * 1000)){
        happenTime
      }else{
        logTime
      }
    }else{
      logTime
    }
    log.put("logTime",time)
    log
  }


}
