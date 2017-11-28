package cn.whaley.bi.logsys.metadataManage.util

import cn.whaley.bi.logsys.metadataManage.entity.{AppLogKeyFieldDescEntity, BlackTableInfoEntity, LogFileFieldDescEntity, LogFileKeyFieldValueEntity}
import com.alibaba.fastjson.{JSON, JSONObject}

import scalaj.http.{Http, HttpOptions}

/**
  * Created by guohao on 2017/11/7.
  */
class PhoenixUtil(metadataService:String="http://bigdata-appsvr-130-5:8084",readTimeOut:Int=900000) {

  def getJSONString[T](entities: Seq[T]): String = {
    "[" + entities.map(entity => JSON.toJSONString(entity, false)).mkString(",") + "]"
  }
  /**
    * 查询所有applog_key_field_desc记录
    */
  def getAllAppLogKeyFieldDesc() = {
    val response = Http(metadataService + "/metadata/applog_key_field_desc/all")
      .option(HttpOptions.readTimeout(readTimeOut))
      .method("GET").asString
    if (!response.isSuccess) {
      throw new RuntimeException(response.body)
    }
    JSON.parseArray(response.body,classOf[AppLogKeyFieldDescEntity]).iterator()
  }

  def getAllBlackTableDesc() = {
    val response = Http(metadataService + "/metadata/black_table_info/all")
      .option(HttpOptions.readTimeout(readTimeOut))
      .method("GET").asString
    if (!response.isSuccess) {
      throw new RuntimeException(response.body)
    }
    JSON.parseArray(response.body,classOf[BlackTableInfoEntity]).iterator()
  }

  /**
    * 插入一批logfile_key_field_value记录,如果taskId不为空,则在操作之前删除taskId相关的原有记录
    */
  def putLogFileKeyFieldValue(taskId: String, entities: Seq[LogFileKeyFieldValueEntity]): JSONObject = {
    val body = getJSONString(entities)
    val response = Http(metadataService + "/metadata/logfile_key_field_value")
      .option(HttpOptions.readTimeout(readTimeOut))
      .method("PUT")
      .header("content-type", "application/json")
      .put(body).asString
    if (!response.isSuccess) {
      throw new RuntimeException(response.body)
    }
    JSON.parseObject(response.body)
  }

  /**
    * 插入一批logfile_field_desc记录,如果taskId不为空,则在操作之前删除taskId相关的原有记录
    */
  def putLogFileFieldDesc(taskId: String, entities: Seq[LogFileFieldDescEntity]): JSONObject = {
    val body = getJSONString(entities)
    val response = Http(metadataService + "/metadata/logfile_field_desc")
      .option(HttpOptions.readTimeout(readTimeOut))
      .method("PUT")
      .header("Content-Type","application/json")
      .put(body).asString
    if (!response.isSuccess) {
      throw new RuntimeException(response.body)
    }
    JSON.parseObject(response.body)
  }


  /**
    *
    * @param taskId
    * @param deleteOld drop partition 是否执行 true 执行
    * @param isDebug
    * @return
    */
  def postTaskId2MetaModel(taskId: String,deleteOld:String,isDebug:Boolean=false): JSONObject = {
//    assert(taskFlag!=null&&taskFlag.length==3)
//    var reallyTaskFlag=""
//    if(isDebug){
//      reallyTaskFlag=taskFlag.substring(0,2)+"0"
//    }else{
//      reallyTaskFlag=taskFlag
//    }
    println(metadataService + s"/metadata/processTask/${taskId}/${deleteOld}")
    val response = Http(metadataService + s"/metadata/processTask/${taskId}/${deleteOld}")
      .option(HttpOptions.readTimeout(readTimeOut))
      .method("POST")
      .header("Content-Type","text/plain")
      .postData("")
      .asString
    if (!response.isSuccess) {
      throw new RuntimeException(response.body)
    }
    JSON.parseObject(response.body)
  }


}

object PhoenixUtil{

  def main(args: Array[String]): Unit = {
    val phoenixUtil = new PhoenixUtil()
    val list = phoenixUtil.getAllAppLogKeyFieldDesc()
    while (list.hasNext){
      println(list.next().toString)
    }

  }
}
