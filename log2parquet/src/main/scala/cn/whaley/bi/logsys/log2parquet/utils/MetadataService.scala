package cn.whaley.bi.logsys.log2parquet.utils

import cn.whaley.bi.logsys.metadata.entity._
import com.alibaba.fastjson.{JSONObject, JSON, JSONArray}
import scala.collection.JavaConversions._

import scalaj.http.{HttpOptions, Http}

/**
 *
 * @param metadataServer 元数据服务器地址
 */
class MetadataService(metadataServer: String, readTimeOut: Int = 900000) {

    def getJSONString[T](entities: Seq[T]): String = {
        "[" + entities.map(entity => JSON.toJSONString(entity, false)).mkString(",") + "]"
    }

    /**
     * 查询所有applog_key_field_desc记录
     */
    def getAllAppLogKeyFieldDesc(): List[AppLogKeyFieldDescEntity] = {
        val response = Http(metadataServer + "/metadata/applog_key_field_desc/all")
            .option(HttpOptions.readTimeout(readTimeOut))
            .method("GET").asString
        if (!response.isSuccess) {
            throw new RuntimeException(response.body)
        }
        JSON.parseArray(response.body, classOf[AppLogKeyFieldDescEntity]).toList
    }

    /**
     * 查询所有applog_special_field_desc记录
     */
    def getAllAppLogSpecialFieldDesc(): List[AppLogSpecialFieldDescEntity] = {
        val response = Http(metadataServer + "/metadata/applog_special_field_desc/all")
            .option(HttpOptions.readTimeout(readTimeOut))
            .method("GET").asString
        if (!response.isSuccess) {
            throw new RuntimeException(response.body)
        }
        JSON.parseArray(response.body, classOf[AppLogSpecialFieldDescEntity]).toList
    }

    /**
     * 插入一批logfile_key_field_value记录,如果taskId不为空,则在操作之前删除taskId相关的原有记录
     */
    def putLogFileKeyFieldValue(taskId: String, entities: Seq[LogFileKeyFieldValueEntity]): JSONObject = {
        if (taskId != null && taskId.trim.length > 0) {
            val response = Http(metadataServer + "/metadata/logfile_key_field_value/" + taskId)
                .option(HttpOptions.readTimeout(readTimeOut))
                .method("DELETE").asString
            if (!response.isSuccess) {
                throw new RuntimeException(response.body)
            }
        }
        val body = getJSONString(entities)
        val response = Http(metadataServer + "/metadata/logfile_key_field_value")
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
        if (taskId != null && taskId.trim.length > 0) {
            val response = Http(metadataServer + "/metadata/logfile_field_desc/" + taskId)
                .option(HttpOptions.readTimeout(readTimeOut))
                .method("DELETE").asString
            if (!response.isSuccess) {
                throw new RuntimeException(response.body)
            }
        }
        val body = getJSONString(entities)
        val response = Http(metadataServer + "/metadata/logfile_field_desc")
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
     * 插入一批logfile_task_info记录
     */
    def putLogFileTaskInfo(entities: Seq[LogFileTaskInfoEntity]): JSONObject = {
        val body = getJSONString(entities)
        val response = Http(metadataServer + "/metadata/logfile_task_info")
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
    * taskFlag:111
    * 第一段表示期望生成DDL和DML
    * 第二段表示执行成DDL
    * 第三段表示执行成DML
    *
    * */
  def postTaskId2MetaModel(taskId: String,taskFlag:String,isDebug:Boolean=false): JSONObject = {
    assert(taskFlag!=null&&taskFlag.length==3)
    var reallyTaskFlag=""
    if(isDebug){
        reallyTaskFlag=taskFlag.substring(0,2)+"0"
    }else{
        reallyTaskFlag=taskFlag
    }
    println(metadataServer + s"/metadata/processTask/${taskId}/${reallyTaskFlag}")
    val response = Http(metadataServer + s"/metadata/processTask/${taskId}/${reallyTaskFlag}")
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
