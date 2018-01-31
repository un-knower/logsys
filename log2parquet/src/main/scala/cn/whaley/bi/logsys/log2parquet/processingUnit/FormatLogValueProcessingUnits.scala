package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.utils.MetaDataUtils
import cn.whaley.bi.logsys.log2parquet.{ProcessResult, ProcessResultCode}
import cn.whaley.bi.logsys.metadata.entity.LogFieldTypeInfoEntity
import com.alibaba.fastjson.JSONObject

import scala.collection.mutable


/**
  * Created by michael on 2017/6/22.
  *
  *将日志字段的string转为期望的类型
  *
  */
class FormatLogValueProcessingUnits extends LogProcessorTraitV2 with LogTrait {
  val fieldTypeMap = mutable.Map[String,String]()
  /**
    * 初始化
    */
  def init(confManager: ConfManager): Unit = {
    val metadataService = confManager.getConf("metadataService")
    val timeout = confManager.getConfOrElseValue("metadata", "readTimeout", "300000").toInt
    val metaDataUtils: MetaDataUtils = new MetaDataUtils(metadataService, timeout)
    //获取字段类型转换的所有信息
    val logFieldTypeInfoEntitys = metaDataUtils.metadataService().getAllLogFieldTypeInfo()
    getFieldTypeMap(logFieldTypeInfoEntitys)
  }

  def getFieldTypeMap(logFieldTypeInfoEntitys : List[LogFieldTypeInfoEntity]): Unit ={
    logFieldTypeInfoEntitys.foreach(entity=>{
      val fieldName = entity.getFieldName
      val typeFlag = entity.getTypeFlag
      fieldTypeMap.put(fieldName,typeFlag)
    })

  }

  /**
    * 转换成Long
    * @param key
    * @param json
    */
  def switchLong(key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      if(isValid(value)){
        json.put(key,value.toLong)
      }else{
        json.put(key,0)
      }
    }catch {
      case e:Exception=>{
        json.put(key,0)
      }
    }
  }

  /**
    * 转换成double
    * @param key
    * @param json
    */
  private def switchDouble(key:String,json:JSONObject):Unit = {
    try {
      val value = json.getString(key).trim
      if(isValid(value)){
        json.put(key,value.toDouble)
      }else{
        json.put(key,0.0)
      }
    }catch {
      case e:Exception=>{
        json.put(key,0.0)
      }
    }
  }

  def isValid(s:String)={
    val regex = "^([0-9]+)|([0-9]+.[0-9]+)$"
    s.matches(regex)
  }

  /**
    *字段类型转换
    * @return
    */

  def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      val it=jsonObject.keySet().iterator()
      while (it.hasNext){
        val key=it.next()
        //判断是否在类型转换名单中
        if(fieldTypeMap.keySet.contains(key.trim)){
          val value = jsonObject.getString(key).trim
          //字段类型标识 1:String 2:Long 3:Double
          val typeFlag = fieldTypeMap.get(key.trim).get
          typeFlag match {
            case "1" => jsonObject.put(key,value)
            case "2" => switchLong(key,jsonObject)
            case "3" => switchDouble(key,jsonObject)
            case _ =>
          }
        }
      }
      new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }

}


