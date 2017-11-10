package cn.whaley.bi.logsys.metadataManage

import java.util.Date

import cn.whaley.bi.logsys.metadataManage.common.ParamKey
import cn.whaley.bi.logsys.metadataManage.entity.{LogFileFieldDescEntity, LogFileKeyFieldValueEntity}
import cn.whaley.bi.logsys.metadataManage.util.{IdGenerator, ParquetHiveUtils, PhoenixUtil}
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by guohao on 2017/11/8.
  */
class MsgManager {
  val LOG = LoggerFactory.getLogger(this.getClass)
  def generateMetaDataToTable(phoenixUtil: PhoenixUtil, pathSchema:Array[(String,scala.collection.mutable.Map[String,String])],taskFlag:String): Unit = {
    //生成taskId
    val generator = IdGenerator.defaultInstance
    val taskId = generator.nextId().replace("/", "")
    println(s"taskId ... $taskId")
    //生成metadata.logfile_key_field_value表数据
    val fieldValueEntityArrayBuffer = generateFieldValueEntityArrayBuffer(taskId, pathSchema)

    println("fieldValueEntityArrayBuffer.length:" + fieldValueEntityArrayBuffer.length)
    LOG.info("fieldValueEntityArrayBuffer.length:" + fieldValueEntityArrayBuffer.length)
    println("-------batchPostFieldValue start at "+new Date())
    batchPostFieldValue(taskId,phoenixUtil, fieldValueEntityArrayBuffer)
    println("-------batchPostFieldValue end at "+new Date())

    //获得不同的输出路径
    val distinctOutput = pathSchema.map(e => {
      e._1
    }).distinct


    //获取parquet信息
    //生成metadata.logfile_field_desc表数据
    val fieldDescEntityArrayBuffer = generateFieldDescEntityArrayBuffer(taskId, distinctOutput)
    println("fieldDescEntityArrayBuffer.length:" + fieldDescEntityArrayBuffer.length)
    LOG.info("fieldDescEntityArrayBuffer.length:" + fieldDescEntityArrayBuffer.length)
    println("-------batchPostFileFieldDesc start at "+new Date())

    println("-------batchPostFileFieldDesc start at "+new Date())
    batchPostFileFieldDesc(taskId,phoenixUtil, fieldDescEntityArrayBuffer)
    println("-------batchPostFileFieldDesc end at "+new Date())

    //发送taskId给元数据模块
    println("-------发送taskId给元数据模块 start at "+new Date())
    val responseTaskIdResponse = phoenixUtil.postTaskId2MetaModel(taskId, taskFlag)
    LOG.info(s"responseTaskIdResponse : " + responseTaskIdResponse.toJSONString)
    println(s"----responseTaskIdResponse : " + responseTaskIdResponse.toJSONString)
    println("-------发送taskId给元数据模块 end at "+new Date())
  }


  /**
    * 生成FieldValueEntity的ArrayBuffer
    *
    * 对应的表内容信息如下：
    * 每条记录通用字段
    * logPath         ...key_hour=08
    * appId           boikgpokn78sb95ktmsc1bnkechpgj9l
    *
    * 每个fieldName对应一条记录
    *
    * fieldName       fieldValue
    * -----------------------
    * db_name         ods_view
    * product_code    medusa
    * app_code        main3x
    * logType         event
    * eventId         medusa_player_sdk_inner_outer_auth_parse
    * key_day         20170614
    * key_hour        08
    * tab_prefix -> log
    *
    * path_file_value_map的结构体为(输出路径 -> Map[fieldName->fieldValue](appId是需要排除的))
    *
    * (ods_view.db/log_medusa_main3x_start_end_ad_vod_tencent_sdk_pre_play/key_day=20170614/key_hour=13,
    * Map(product_code -> medusa, key_hour -> 13, key_day -> 20170614,
    * appId -> boikgpokn78sb95ktmsc1bnkechpgj9l, tab_prefix -> log,
    * actionId -> ad-vod-tencent-sdk-pre-play,
    * db_name -> ods_view, logType -> start_end, app_code -> main3x))
    */
  def generateFieldValueEntityArrayBuffer(taskId: String, path_file_value_map: Array[(String, scala.collection.mutable.Map[String, String])]): ArrayBuffer[LogFileKeyFieldValueEntity] = {
    val arrayBuffer = new ArrayBuffer[LogFileKeyFieldValueEntity]()
    //每条记录的粒度为fieldName，fieldValue
    path_file_value_map.foreach(e => {
      val outputPath = e._1
      val file_value_map = e._2
      val appId = file_value_map.get(ParamKey.APPID).getOrElse("noAppId")
      file_value_map.filter(e => !e._1.contains(ParamKey.APPID)).foreach(field => {
        val logFileKeyFieldValueEntity = new LogFileKeyFieldValueEntity()
        logFileKeyFieldValueEntity.setAppId(appId)
        logFileKeyFieldValueEntity.setLogPath(outputPath)
        logFileKeyFieldValueEntity.setTaskId(taskId)
        logFileKeyFieldValueEntity.setIsDeleted(false)
        logFileKeyFieldValueEntity.setCreateTime(new Date())
        logFileKeyFieldValueEntity.setUpdateTime(new Date())
        logFileKeyFieldValueEntity.setFieldName(field._1)
        logFileKeyFieldValueEntity.setFieldValue(field._2)
        arrayBuffer.+=(logFileKeyFieldValueEntity)
      })
    })
    arrayBuffer
  }

  /** 批量发送FileFieldValue的POST请求 */
  def batchPostFieldValue(taskId: String,phoenixUtil:PhoenixUtil, seq: Seq[LogFileKeyFieldValueEntity]) {
    val batchSize = 1000
    val total = seq.length
    if (total > batchSize) {
      val times = (total / batchSize)
      var start = 0
      for (i <- 1 to times) {
        val end = batchSize * i
        val fragment = seq.slice(start, end)
        LOG.info(s"batch $i,$start->$end start: " + new Date())
        println(s"----batch $i,$start->$end start: " + new Date())
        val responseFieldDesc = phoenixUtil.putLogFileKeyFieldValue(taskId, fragment)
        LOG.info(s"batch $i,$start->$end end: " + new Date())
        println(s"----batch $i,$start->$end end: " + new Date())
        LOG.info(s"batch $i,$start->$end : " + responseFieldDesc.toJSONString)
        println(s"----batch $i,$start->$end : " + responseFieldDesc.toJSONString)
        start = batchSize * i
      }
      val remain = seq.length - times * batchSize
      if (remain > 0) {
        val fragment = seq.slice(times * batchSize, seq.length)
        val responseFieldDesc = phoenixUtil.putLogFileKeyFieldValue(taskId, fragment)
        LOG.info(s"FieldValue_remain ${fragment.length}: " + responseFieldDesc.toJSONString)
        println(s"----FieldValue_remain ${fragment.length}: " + responseFieldDesc.toJSONString)
      }
    } else {
      val responseFieldDesc = phoenixUtil.putLogFileKeyFieldValue(taskId, seq)
      LOG.info(s"FieldValue_responseFieldDesc : " + responseFieldDesc.toJSONString)
      println(s"----FieldValue_responseFieldDesc : " + responseFieldDesc.toJSONString)

    }
  }

  def generateFieldDescEntityArrayBuffer(taskId: String, distinctOutputArray: Array[String]): Seq[LogFileFieldDescEntity] = {
    distinctOutputArray.flatMap(dir => {
      val list = ParquetHiveUtils.getParquetFilesFromHDFS(dir)

      val fieldInfo = if (list.size > 0) {
        val parquetMetaData = ParquetHiveUtils.parseSQLFieldInfos(list.head.getPath)
        parquetMetaData.map(meta => {
          val fieldName = meta._1
          val fieldType = meta._2
          val fieldSql = meta._3
          val rowType = meta._4
          val rowInfo = meta._5
          val logFileFieldDescEntity = new LogFileFieldDescEntity
          logFileFieldDescEntity.setTaskId(taskId)
          logFileFieldDescEntity.setLogPath(dir)
          logFileFieldDescEntity.setFieldName(fieldName)
          logFileFieldDescEntity.setFieldType(fieldType)
          logFileFieldDescEntity.setFieldSql(fieldSql)
          logFileFieldDescEntity.setRawType(rowType)
          logFileFieldDescEntity.setRawInfo(rowInfo)
          logFileFieldDescEntity.setIsDeleted(false)
          logFileFieldDescEntity.setCreateTime(new Date())
          logFileFieldDescEntity.setUpdateTime(new Date())
          logFileFieldDescEntity
        })
      } else {
        Nil
      }
      if(fieldInfo.size>80){
        LOG.info(s"parquet文件schema字段个数: $dir -> ${fieldInfo.size} ")
        println(s"----parquet文件schema字段个数: $dir -> ${fieldInfo.size} ")
      }
      fieldInfo
    })
  }


  /** 批量发送FileFieldDesc的POST请求 */
  def batchPostFileFieldDesc(taskId: String,phoenixUtil:PhoenixUtil ,seq: Seq[LogFileFieldDescEntity]) {
    val batchSize = 1000
    val total = seq.length
    if (total > batchSize) {
      val times = (total / batchSize)
      var start = 0
      for (i <- 1 to times) {
        val end = batchSize * i
        val fragment = seq.slice(start, end)
        LOG.info(s"batch $i,$start->$end start: " + new Date())
        println(s"----batch $i,$start->$end start: " + new Date())
        val responseFieldDesc = phoenixUtil.putLogFileFieldDesc(taskId, fragment)
        LOG.info(s"batch $i,$start->$end end: " + new Date())
        println(s"----batch $i,$start->$end end: " + new Date())
        LOG.info(s"batch $i,$start->$end : " + responseFieldDesc.toJSONString)
        println(s"----batch $i,$start->$end : " + responseFieldDesc.toJSONString)
        start = batchSize * i
      }
      val remain = seq.length - times * batchSize
      if (remain > 0) {
        val fragment = seq.slice(times * batchSize, seq.length)
        val responseFieldDesc = phoenixUtil.putLogFileFieldDesc(taskId, fragment)
        LOG.info(s"remain ${fragment.length}: " + responseFieldDesc.toJSONString)
        println(s"----remain ${fragment.length}: " + responseFieldDesc.toJSONString)
      }
    } else {
      val responseFieldDesc = phoenixUtil.putLogFileFieldDesc(taskId, seq)
      LOG.info(s"responseFieldDesc : " + responseFieldDesc.toJSONString)
      println(s"----responseFieldDesc : " + responseFieldDesc.toJSONString)
    }
  }


}

object MsgManager{
  def main(args: Array[String]): Unit = {
    val generator = IdGenerator.defaultInstance
    val taskId = generator.nextId().replace("/", "")
    println(taskId)
  }
}
