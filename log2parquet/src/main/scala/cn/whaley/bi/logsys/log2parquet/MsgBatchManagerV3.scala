package cn.whaley.bi.logsys.log2parquet

import java.io.File
import java.util.Date

import cn.whaley.bi.logsys.common.{IdGenerator, ConfManager}
import cn.whaley.bi.logsys.log2parquet.constant.{LogKeys, Constants}
import cn.whaley.bi.logsys.log2parquet.traits._
import cn.whaley.bi.logsys.log2parquet.utils.{ParquetHiveUtils, Json2ParquetUtil, MetaDataUtils}
import cn.whaley.bi.logsys.metadata.entity.{LogFileFieldDescEntity, LogFileKeyFieldValueEntity}
import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by michael on 2017/6/22.
  */
class MsgBatchManagerV3 extends InitialTrait with NameTrait with LogTrait with java.io.Serializable {

  var metaDataUtils :MetaDataUtils=null

  /**
    * 初始化方法
    * 如果初始化异常，则应该抛出异常
    * michael,cause of use spark distribute compute model,change to init processorChain in foreachPartition
    */
  override def init(confManager: ConfManager): Unit = {
    //MsgBatchManagerV3.inputPath = confManager.getConf("inputPath")


    /**
      * 批量加载metadata.applog_key_field_desc表,将数据结构作为广播变量,用来作为日志的输出路径模版使用
      **/
    //val appId2OutputPathTemplateMap = scala.collection.mutable.HashMap.empty[String, String]
    //appId2OutputPathTemplateMap.put("boikgpokn78sb95ktmsc1bnkechpgj9l","log_medusa_main3x_${log_type}_${event_id}/key_day=${key_day}/key_hour=${key_hour}")
    //val appId2OutputPathTemplateMap = MetaDataUtils.getAppId2OutputPathTemplateMap
    //MsgBatchManagerV3.appId2OutputPathTemplateMapBroadCast = MsgBatchManagerV3.sparkSession.sparkContext.broadcast(appId2OutputPathTemplateMap)
    val metadataService=confManager.getConf("metadataService")
    println("-----metadataService:"+metadataService)
    metaDataUtils=new MetaDataUtils(metadataService)
  }

  /**
    * 启动
    */
  def start(confManager: ConfManager): Unit = {
    val config = new SparkConf()

    //check if run on mac use MainObjTests
    if (confManager.getConf("masterURL") != null) {
      config.setMaster(confManager.getConf("masterURL"))
      println("---local master url:" + confManager.getConf("masterURL"))
    }
    val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()


    //读取原始文件
    val inputPath = confManager.getConf("inputPath")
    val rdd_original = sparkSession.sparkContext.textFile(inputPath, 2)
    println("rdd_original.count():" + rdd_original.count())
    LOG.info("rdd_original.count():" + rdd_original.count())

    //解析出输出目录
    val pathRdd = metaDataUtils.parseLogStrRddPath(rdd_original)
    println("pathRdd.count():" + pathRdd.count())
    LOG.info("pathRdd.count():" + pathRdd.count())
    pathRdd.take(10).foreach(println)

    //经过处理器链处理
    val logProcessGroupName = confManager.getConf(this.name, "LogProcessGroup")
    println("------logProcessGroupName:"+logProcessGroupName)
    val processGroupInstance = instanceFrom(confManager, logProcessGroupName).asInstanceOf[ProcessGroupTraitV2]
    val processedRdd = pathRdd.map(e => {
      val jsonObject = e._2
      val jsonObjectProcessed = processGroupInstance.process(jsonObject)
      //(Constants.DATA_WAREHOUSE + File.separator + e._1, jsonObjectProcessed)
      (e._1, jsonObjectProcessed)
    })
    println("processedRdd.count():" + processedRdd.count())
    LOG.info("processedRdd.count():" + processedRdd.count())
    processedRdd.take(10).foreach(println)

    //将经过处理器处理后，正常状态的记录使用规则库过滤【字段黑名单、重命名、行过滤】
    val okRowsRdd = processedRdd.filter(e=>e._2.hasErr==false)
    val afterRuleRdd=ruleHandle(pathRdd,okRowsRdd)
    println("afterRuleRdd.count():" + afterRuleRdd.count())
    LOG.info("afterRuleRdd.count():" + afterRuleRdd.count())
    //afterRuleRdd.filter(e=>e._2.toJSONString.contains("actionId")).take(10).foreach(println)
    afterRuleRdd.take(10).foreach(println)

    //输出正常记录到HDFS文件
    //Json2ParquetUtil.saveAsParquet(afterRuleRdd, sparkSession)

    //输出异常记录到HDFS文件
    val errRowsRdd = processedRdd.filter(row => row._2.hasErr).map(row => {
      row._2
    })
    println("errRowsRdd.count():" + errRowsRdd.count())
    LOG.info("errRowsRdd.count():" + errRowsRdd.count())
    errRowsRdd.take(10).foreach(println)

    val time=new Date().getTime
    //errRowsRdd.saveAsTextFile(s"${Constants.ODS_VIEW_HDFS_OUTPUT_PATH_TMP_ERROR}${File.separator}${time}")

    //生成元数据信息给元数据模块使用
    val path_file_value_map=pathRdd.map(e=>(e._1,e._3)).distinct().collect()
    println("path_file_value_map.length():" + path_file_value_map.length)
    LOG.info("path_file_value_map.length():" + path_file_value_map.length)
    path_file_value_map.take(10).foreach(println)
    ///generateMetaDataToTable(path_file_value_map)
  }


  /**
    * 向phoenix表插入数据[metadata.logfile_key_field_value,metadata.logfile_field_desc]
    * */
  def generateMetaDataToTable(path_file_value_map:Array[(String,scala.collection.mutable.Map[String,String])]): Unit ={
    val generator = IdGenerator.defaultInstance
    val taskId=generator.nextId()

    // metadata.logfile_key_field_value
    val fieldValueEntityArrayBuffer=generateFieldValueEntityArrayBuffer(taskId,path_file_value_map)
    println("fieldValueEntityArrayBuffer.length:" + fieldValueEntityArrayBuffer.length)
    LOG.info("fieldValueEntityArrayBuffer.length:" + fieldValueEntityArrayBuffer.length)
    fieldValueEntityArrayBuffer.take(10).foreach(println)
    val response=metaDataUtils.metadataService().putLogFileKeyFieldValue(taskId, fieldValueEntityArrayBuffer)
    println(response.toJSONString)

    //metadata.logfile_field_desc
    val distinctOutput=path_file_value_map.map(e=>{
      e._1
    }).distinct
    val fieldDescEntityArrayBuffer=generateFieldDescEntityArrayBuffer(taskId,distinctOutput)
    println("fieldDescEntityArrayBuffer.length:" + fieldDescEntityArrayBuffer.length)
    LOG.info("fieldDescEntityArrayBuffer.length:" + fieldDescEntityArrayBuffer.length)
    fieldDescEntityArrayBuffer.take(10).foreach(println)
    val responseFieldDesc=metaDataUtils.metadataService().putLogFileFieldDesc(taskId, fieldDescEntityArrayBuffer)
    println(responseFieldDesc.toJSONString)
  }

  /**
    *   Seq[(fieldName:String,fieldType:String,fieldSql:String,rowType:String,rowInfo:String)]
    * */
  def generateFieldDescEntityArrayBuffer(taskId:String,distinctOutputArray:Array[String]): ArrayBuffer[LogFileFieldDescEntity] ={
    val arrayBuffer = new ArrayBuffer[LogFileFieldDescEntity]()
    distinctOutputArray.map(dir=>{
      val list=ParquetHiveUtils.getParquetFilesFromHDFS(Constants.DATA_WAREHOUSE+File.separator+dir)
      if(list.size>0){
       val parquetMetaData = ParquetHiveUtils.parseSQLFieldInfos(list.head.getPath)
        parquetMetaData.map(meta=>{
          val fieldName=meta._1
          val fieldType=meta._2
          val fieldSql=meta._3
          val rowType=meta._4
          val rowInfo=meta._5
          val logFileFieldDescEntity=new LogFileFieldDescEntity
          logFileFieldDescEntity.setTaskId(taskId)
          logFileFieldDescEntity.setLogPath(Constants.DATA_WAREHOUSE+File.separator+dir)
          logFileFieldDescEntity.setFieldName(fieldName)
          logFileFieldDescEntity.setFieldType(fieldType)
          logFileFieldDescEntity.setFieldSql(fieldSql)
          logFileFieldDescEntity.setRawType(rowType)
          logFileFieldDescEntity.setRawInfo(rowInfo)
          logFileFieldDescEntity.setIsDeleted(false)
          logFileFieldDescEntity.setCreateTime(new Date())
          logFileFieldDescEntity.setUpdateTime(new Date())
          arrayBuffer+=logFileFieldDescEntity
        })
      }
    })
    arrayBuffer
  }


    /**
    * 生成FieldValueEntity的ArrayBuffer
    *
    *
    * 对应的表内容信息如下：
    * 每条记录通用字段
    *  logPath         ...key_hour=08
    *  appId           boikgpokn78sb95ktmsc1bnkechpgj9l
    *
    * 每个fieldName对应一条记录
    *
    *  fieldName       fieldValue
    *  -----------------------
    *  db_name         ods_view
    *  product_code    medusa
    *  app_code        main3x
    *  logType         event
    *  eventId         medusa_player_sdk_inner_outer_auth_parse
    *  key_day         20170614
    *  key_hour        08
    *  tab_prefix -> log
    *
    * path_file_value_map的结构体为(输出路径 -> Map[fieldName->fieldValue](appId是需要排除的))
    *
    *(ods_view.db/log_medusa_main3x_start_end_ad_vod_tencent_sdk_pre_play/key_day=20170614/key_hour=13,
    * Map(product_code -> medusa, key_hour -> 13, key_day -> 20170614,
    * appId -> boikgpokn78sb95ktmsc1bnkechpgj9l, tab_prefix -> log,
    * actionId -> ad-vod-tencent-sdk-pre-play,
    * db_name -> ods_view, logType -> start_end, app_code -> main3x))
    */
  def generateFieldValueEntityArrayBuffer(taskId:String,path_file_value_map:Array[(String,scala.collection.mutable.Map[String,String])]): ArrayBuffer[LogFileKeyFieldValueEntity] ={
    val arrayBuffer = new ArrayBuffer[LogFileKeyFieldValueEntity]()
    //每条记录的粒度为fieldName，fieldValue
    path_file_value_map.map(e=>{
      val outputPath=e._1
      val file_value_map=e._2
      val appId=file_value_map.get(LogKeys.LOG_APP_ID).getOrElse("noAppId")
      file_value_map.filter(e => !e._1.contains(LogKeys.LOG_APP_ID)).map(field=>{
        val logFileKeyFieldValueEntity=new LogFileKeyFieldValueEntity()
        logFileKeyFieldValueEntity.setAppId(appId)
        logFileKeyFieldValueEntity.setLogPath(Constants.DATA_WAREHOUSE+File.separator+outputPath)
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



  /**记录使用规则库过滤【字段黑名单、重命名、行过滤】*/
  def ruleHandle(pathRdd:RDD[(String, JSONObject,scala.collection.mutable.Map[String,String])],resultRdd: RDD[(String, ProcessResult[JSONObject])]):RDD[(String, JSONObject)] ={
    // 获得规则库的每条规则
    val rules = metaDataUtils.parseSpecialRules(pathRdd)
    println("---------------------rules"+rules)
    rules.foreach(println)
    println("---------------------rulesend"+rules)

    val afterRuleRdd = resultRdd.map(e => {
      val path = e._1
      val jsonObject = e._2.result.get

      //if(rules.filter(rule => (Constants.DATA_WAREHOUSE+File.separator+rule.path).equalsIgnoreCase(path)).size>0){
        if(rules.filter(rule => rule.path.equalsIgnoreCase(path)).size>0){
        //一个绝对路径唯一对应一条规则
        val rule = rules.filter(rule => rule.path.equalsIgnoreCase(path)).head
        //val rule = rules.filter(rule => (Constants.DATA_WAREHOUSE+File.separator+rule.path).equalsIgnoreCase(path)).head

        val fieldBlackFilter = rule.fieldBlackFilter
        fieldBlackFilter.foreach(blackField => {
          jsonObject.remove(blackField)
        })

        val rename = rule.rename
        rename.foreach(e => {
          if (jsonObject.containsKey(e._1)) {
            jsonObject.put(e._2, jsonObject.get(e._1))
            jsonObject.remove(e._1)
          }
        })

        val rowBlackFilter = rule.rowBlackFilter
        val resultJsonObject=if(rowBlackFilter.filter(item => jsonObject.get(item._1) != null && item._2 == jsonObject.getString(item._1)).size > 0) None
        else Some(jsonObject)
        (path, resultJsonObject)
      }else{
        //当路径没有找到规则的情况
        (path,Some(jsonObject))
      }
    }).filter(e => !(e._2.isEmpty)).map(item=>(item._1,item._2.get))
    afterRuleRdd
  }


  //medusa 2.x
  def initAllProcessGroup(): scala.collection.mutable.HashMap[String, ProcessGroupTraitV2] = {
    val processGroupName2processGroupInstance = scala.collection.mutable.HashMap.empty[String, ProcessGroupTraitV2]
    val confManager = new ConfManager(Array("MsgBatchManagerV3.xml", "settings.properties"))
    val allProcessGroup = confManager.getConf(this.name, "allProcessGroup")
    require(null != allProcessGroup)
    allProcessGroup.split(",").foreach(groupName => {
      val groupNameFromConfig = confManager.getConf(this.name, groupName)
      val processGroup = instanceFrom(confManager, groupNameFromConfig).asInstanceOf[ProcessGroupTraitV2]
      processGroup.init(confManager)
      processGroupName2processGroupInstance += (groupNameFromConfig -> processGroup)
    })
    val keyword = "appIdForProcessGroup."
    val appId2ProcessGroupName = confManager.getKeyValueByRegex(keyword)
    val appId2processGroupInstance = scala.collection.mutable.HashMap.empty[String, ProcessGroupTraitV2]

    appId2ProcessGroupName.foreach(e => {
      val appId = e._1
      val processGroupName = e._2
      val processGroupInstance = processGroupName2processGroupInstance.get(processGroupName).get
      appId2processGroupInstance.put(appId, processGroupInstance)
    })
    appId2processGroupInstance
  }

  /*def string2JsonObject(log: String): JSONObject = {
    try {
      val json = JSON.parseObject(log)
      json
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }*/

  /**
    * 关停
    */
  def shutdown(): Unit = {
    /* if (MsgBatchManagerV3.sparkSession != null) {
       MsgBatchManagerV3.sparkSession.close()
     }*/
  }

}

/** object MsgBatchManagerV3 {
  * /*val config = new SparkConf()
  * //config.setMaster("local[2]")
  * val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate() */
  * //var inputPath = ""
  * /*var appId2OutputPathTemplateMapBroadCast: Broadcast[scala.collection.mutable.HashMap[String, String]] = _
  * var specialRulesBroadCase:Broadcast[Array[AppLogFieldSpecialRules]]=_ */
  * } */
