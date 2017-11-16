package cn.whaley.bi.logsys.batchforest.process

import cn.whaley.bi.logsys.batchforest.process.LogFormat.translateProp
import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}
import cn.whaley.bi.logsys.batchforest.util.{JsonUtil, MyAccumulator, StringUtil}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.codec.digest.DigestUtils

/**
  * Created by guohao on 2017/11/15.
  */
object EagleProcess extends NameTrait with LogTrait{
  def handleMessage(message:JSONObject)
               (implicit myAccumulator:MyAccumulator=new MyAccumulator):Seq[Option[JSONObject]]={

    val msgBody = message.getJSONObject("msgBody")
    val body = msgBody.getJSONObject("body")
    //md5校验
    if(!checkMd5(body)){
      myAccumulator.add("eagleMd5Exc")
      return Array(None)
    }
    val msgId = message.getString("msgId")
    message.remove("msgBody")

    //修正
    val md5 = body.getString("}d5")
    if(md5 != null ){
      body.put("md5",md5)
      body.remove("}d5")
    }
    msgBody.remove("body")
    //baseInfo展开
    val baseInfo = if(body.get("baseInfo").isInstanceOf[String]){
      JsonUtil.parseObject(body.get("baseInfo").asInstanceOf[String])
    }else{
      Some(body.get("baseInfo").asInstanceOf[JSONObject])
    }
    var apkVersion = ""
    if(!baseInfo.isEmpty && baseInfo.get != null  ){
      //baseInfo 合并到body
      apkVersion = baseInfo.get.getString("apkVersion")
      body.remove("baseInfo")
      body.asInstanceOf[java.util.Map[String,Object]].putAll(baseInfo.get)
    }
    //logs展开
    val logs =
      if (body.get("logs").isInstanceOf[String]) {
        val logs = if("2.1.7".equals(apkVersion.trim)){
          strFix(body.getString("logs"))
        }else{
          body.getString("logs")
        }
        JsonUtil.parseArray(logs)
      } else {
        Some(body.get("logs").asInstanceOf[JSONArray])
      }

    body.remove("logs")
    if(!logs.isEmpty  && logs.get != null  && logs.get.size() !=0){
      //logs 非空{}
      val size = logs.get.size()
      for(i<-0 to size -1) yield {
        //将body中的属性合并到log中
        try{
          val log = logs.get.getJSONObject(i)
          log.asInstanceOf[java.util.Map[String,Object]].putAll(body)
          //将log中的属性合并到msgBody中,一定要创建一个新的logBody而不能采用引用
          val logBody = JSON.parseObject(msgBody.toJSONString)
          logBody.asInstanceOf[java.util.Map[String,Object]].putAll(log)
          //平展eventProp，currentPageProp，dynamicBasicData
          flatJson(logBody,"eventProp")
          flatJson(logBody,"currentPageProp")
          flatJson(logBody,"dynamicBasicData")
          //
          val entity = JSON.parseObject(message.toJSONString)
          val logId =msgId +StringUtil.fixLeftLen(Integer.toHexString(i),'0',4)
          entity.put("logId",logId)
          entity.put("logBody",logBody)
          //提升字段
          translateProp(entity.getJSONObject("logBody"),"appId",entity,"appId")
          translateProp(entity.getJSONObject("logBody"),"logVersion",entity,"logVersion")
          translateProp(entity.getJSONObject("logBody"),"logSignFlag",entity,"logSignFlag")
          translateProp(entity.getJSONObject("logBody"),"logTime",entity,"logTime")
          myAccumulator.add("handleEagleOutRecord")
          Some(entity)
        }catch {
          case e:Exception=>{
            LOG.info(s"logs ...... ${logs.get} ")
            LOG.error(e.getMessage)
            myAccumulator.add("handleEagleExc")
            return Array(None)
          }
        }
      }
      //处理展开后的日志
    }else{
      myAccumulator.add("handleEagleExc")
      return Array(None)
    }

  }

  /**
    * 平展dynamicBasicData、currentPageProp、eventProp
    * @param logBody
    * @param key
    */
  def flatJson(logBody:JSONObject,key:String): Unit ={

    val value =
      if (logBody.get(key).isInstanceOf[String]) {
        JsonUtil.parseObject(logBody.get(key).asInstanceOf[String])
      } else {
        Some(logBody.get(key).asInstanceOf[JSONObject])
      }
    logBody.remove(key)
    if(value.get != null){
      logBody.asInstanceOf[java.util.Map[String,Object]].putAll(value.get)
    }


  }

  /**
    * 处理打点中的bug
    *
    * @param line
    * @return
    */
  def strFix(line: String): String = {
    if (line.contains("letter_search_click") ) {
      val oldStr =""",\"searchResultType\":"""
      val newStr =""",\"searchResultType\":\""""
      val result = line
        .replace(oldStr,newStr) //217版本存在的bug，后续版本已修复
      return result
    }
    else return line
  }

  def checkMd5(message:JSONObject): Boolean ={
    val key = "naudfur894509@#$8ehu$hfuhuwerj9475jafh70!-748fhgt6"
    val baseInfo = message.get("baseInfo")
    val logs = message.getString("logs")
    val ts = message.getString("ts")
    DigestUtils.md5Hex(baseInfo + logs + ts + key) == message.getString("md5")
  }


}
