package cn.whaley.bi.logsys.log2parquet

import java.io.File
import java.util.Date

import cn.whaley.bi.logsys.common.{ConfManager, IdGenerator}
import cn.whaley.bi.logsys.log2parquet.constant.{Constants, LogKeys}
import cn.whaley.bi.logsys.log2parquet.moretv2x.{LogPreProcess, LogUtils}
import cn.whaley.bi.logsys.log2parquet.traits._
import cn.whaley.bi.logsys.log2parquet.utils.{Json2ParquetUtil, MetaDataUtils, ParquetHiveUtils}
import cn.whaley.bi.logsys.metadata.entity.{LogFileFieldDescEntity, LogFileKeyFieldValueEntity}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by michael on 2017/6/22.
  */
class MsgBatchManagerV3 extends InitialTrait with NameTrait with LogTrait with java.io.Serializable {

  var metaDataUtils: MetaDataUtils = null

  /**
    * 初始化方法
    * 如果初始化异常，则应该抛出异常
    */
  override def init(confManager: ConfManager): Unit = {
   val metadataService = confManager.getConf("metadataService")
    val timeout=confManager.getConfOrElseValue("metadata","readTimeout","300000").toInt
    println("-----timeout:" + timeout)
    println("-----metadataService:" + metadataService)
    metaDataUtils = new MetaDataUtils(metadataService,timeout)
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
   /* val rdd_original = sparkSession.sparkContext.textFile(inputPath, 200).map(line=>{
      val json=try {
        Some(JSON.parseObject(line))
      }
      catch {
        case _: Throwable => {
          None
        }
      }
      if(json.isDefined){
        val appId=json.get.getString("appId")
        if(appId.equalsIgnoreCase(Constants.MEDUSA2X_APP_ID)){
            Some("medusa 2x split function")
        }else{
          Some(line)
        }
      }else{
        None
      }
    }
    ).filter(row => row.isDefined).map(row => row.get)*/

   val rdd_original = sparkSession.sparkContext.textFile(inputPath, 200).map(line=>{
     if(line.indexOf("\"appId\":\""+Constants.MEDUSA2X_APP_ID+"\"")>0){
       val jsObj = JSON.parseObject(line)
       val logBody = jsObj.getJSONObject("logBody")
       val msgStr = logBody.getString("log")
       val svrReceiveTime = logBody.getLong("svr_receive_time")
       val logType = LogUtils.getLogType(msgStr)
       val logData = LogPreProcess.matchLog(logType,s"$svrReceiveTime-$msgStr").toJSONObject
       Some(logData)
     }else{
       Some(JSON.parseObject(line))
     }
   }).filter(row => row.isDefined).map(row => row.get)


    //解析出输出目录
    val pathRdd = metaDataUtils.parseLogObjRddPath(rdd_original)

    //经过处理器链处理
    val logProcessGroupName = confManager.getConf(this.name, "LogProcessGroup")
    val processGroupInstance = instanceFrom(confManager, logProcessGroupName).asInstanceOf[ProcessGroupTraitV2]
    val processedRdd = pathRdd.map(e => {
      val jsonObject = e._2
      val jsonObjectProcessed = processGroupInstance.process(jsonObject)
      (e._1, jsonObjectProcessed)
    })


    //将经过处理器处理后，正常状态的记录使用规则库过滤【字段黑名单、重命名、行过滤】
    val okRowsRdd = processedRdd.filter(e => e._2.hasErr == false)
    val afterRuleRdd = ruleHandle(pathRdd, okRowsRdd)

    //输出正常记录到HDFS文件
    Json2ParquetUtil.saveAsParquet(afterRuleRdd, sparkSession)

    //输出异常记录到HDFS文件
    val errRowsRdd = processedRdd.filter(row => row._2.hasErr).map(row => {
      row._2
    })
    if(errRowsRdd.count()>0){
      val time = new Date().getTime
      errRowsRdd.saveAsTextFile(s"${Constants.ODS_VIEW_HDFS_OUTPUT_PATH_TMP_ERROR}${File.separator}${time}")
    }

    //生成元数据信息给元数据模块使用
    val path_file_value_map = pathRdd.map(e => (e._1, e._3)).distinct().collect()
    //println("path_file_value_map.length():" + path_file_value_map.length)
    LOG.info("path_file_value_map.length():" + path_file_value_map.length)
    path_file_value_map.take(10).foreach(println)
    generateMetaDataToTable(sparkSession,path_file_value_map)
  }


  /**
    * 向phoenix表插入数据[metadata.logfile_key_field_value,metadata.logfile_field_desc]
    **/
  def generateMetaDataToTable(sparkSession:SparkSession,path_file_value_map: Array[(String, scala.collection.mutable.Map[String, String])]): Unit = {
    val generator = IdGenerator.defaultInstance
    val taskId = generator.nextId().replace("/", "")
    println("----------taskId:" + taskId)

    //生成metadata.logfile_key_field_value表数据
    val fieldValueEntityArrayBuffer = generateFieldValueEntityArrayBuffer(taskId, path_file_value_map)
    println("fieldValueEntityArrayBuffer.length:" + fieldValueEntityArrayBuffer.length)
    LOG.info("fieldValueEntityArrayBuffer.length:" + fieldValueEntityArrayBuffer.length)
    batchPostFieldValue(taskId,fieldValueEntityArrayBuffer)

    //获得不同的输出路径
    val distinctOutput = path_file_value_map.map(e => {
      e._1
    }).distinct
    //打印输出路径
    println("------distinctOutput.begin:"+distinctOutput.length)
    distinctOutput.foreach(println)
    println("------distinctOutput.end")
    LOG.info("------distinctOutput.begin:"+distinctOutput.length)
    distinctOutput.foreach(e=>{LOG.info(e)})
    LOG.info("------distinctOutput.end")

    //生成metadata.logfile_field_desc表数据
    val fieldDescEntityArrayBuffer = generateFieldDescEntityArrayBuffer(sparkSession,taskId, distinctOutput)
    println("fieldDescEntityArrayBuffer.length:" + fieldDescEntityArrayBuffer.length)
    LOG.info("fieldDescEntityArrayBuffer.length:" + fieldDescEntityArrayBuffer.length)
    batchPostFileFieldDesc(taskId,fieldDescEntityArrayBuffer)

    //发送taskId给元数据模块
    val responseTaskIdResponse=metaDataUtils.metadataService().postTaskId2MetaModel(taskId, "111")
    LOG.info(s"responseTaskIdResponse : "+responseTaskIdResponse.toJSONString)
    println(s"----responseTaskIdResponse : "+responseTaskIdResponse.toJSONString)
  }

  /**批量发送FileFieldValue的POST请求*/
  def batchPostFieldValue(taskId:String,seq:Seq[LogFileKeyFieldValueEntity]) {
    val batchSize=1000
    val total=seq.length
    if(total>batchSize){
      val times=(total/batchSize)
      var start=0
      for(i <-1 to times){
        val end=batchSize*i
        val fragment=seq.slice(start,end)
        LOG.info(s"batch $i,$start->$end start: "+new Date())
        println(s"----batch $i,$start->$end start: "+new Date())
        val responseFieldDesc = metaDataUtils.metadataService().putLogFileKeyFieldValue(taskId, fragment)
        LOG.info(s"batch $i,$start->$end end: "+new Date())
        println(s"----batch $i,$start->$end end: "+new Date())
        LOG.info(s"batch $i,$start->$end : "+responseFieldDesc.toJSONString)
        println(s"----batch $i,$start->$end : "+responseFieldDesc.toJSONString)
        start=batchSize*i
      }
      val remain=seq.length-times*batchSize
      if(remain>0){
        val fragment=seq.slice(times*batchSize,seq.length)
        val responseFieldDesc = metaDataUtils.metadataService().putLogFileKeyFieldValue(taskId, fragment)
        LOG.info(s"FieldValue_remain $remain: "+responseFieldDesc.toJSONString)
        println(s"----FieldValue_remain $remain: "+responseFieldDesc.toJSONString)
      }
    }else{
      val responseFieldDesc = metaDataUtils.metadataService().putLogFileKeyFieldValue(taskId, seq)
      LOG.info(s"FieldValue_responseFieldDesc : "+responseFieldDesc.toJSONString)
      println(s"----FieldValue_responseFieldDesc : "+responseFieldDesc.toJSONString)

    }
  }

  /**批量发送FileFieldDesc的POST请求*/
  def batchPostFileFieldDesc(taskId:String,seq:Seq[LogFileFieldDescEntity]) {
    val batchSize=1000
    val total=seq.length
    if(total>batchSize){
      val times=(total/batchSize)
      var start=0
      for(i <-1 to times){
        val end=batchSize*i
        val fragment=seq.slice(start,end)
        LOG.info(s"batch $i,$start->$end start: "+new Date())
        println(s"----batch $i,$start->$end start: "+new Date())
        val responseFieldDesc = metaDataUtils.metadataService().putLogFileFieldDesc(taskId, fragment)
        LOG.info(s"batch $i,$start->$end end: "+new Date())
        println(s"----batch $i,$start->$end end: "+new Date())
        LOG.info(s"batch $i,$start->$end : "+responseFieldDesc.toJSONString)
        println(s"----batch $i,$start->$end : "+responseFieldDesc.toJSONString)
        start=batchSize*i
      }
      val remain=seq.length-times*batchSize
      if(remain>0){
        val fragment=seq.slice(times*batchSize,seq.length)
        val responseFieldDesc = metaDataUtils.metadataService().putLogFileFieldDesc(taskId, fragment)
        LOG.info(s"remain $remain: "+responseFieldDesc.toJSONString)
        println(s"----remain $remain: "+responseFieldDesc.toJSONString)
      }
    }else{
      val responseFieldDesc = metaDataUtils.metadataService().putLogFileFieldDesc(taskId, seq)
      LOG.info(s"responseFieldDesc : "+responseFieldDesc.toJSONString)
      println(s"----responseFieldDesc : "+responseFieldDesc.toJSONString)
    }
  }

  /**
    * 通过makeRDD方式,并发生成FieldDescEntity，但是发现原有方式这段耗时1分钟
    * Seq[(fieldName:String,fieldType:String,fieldSql:String,rowType:String,rowInfo:String)]
    **/
 /* def generateFieldDescEntityArrayBuffer(sparkSession:SparkSession,taskId: String, distinctOutputArray: Array[String]): Seq[LogFileFieldDescEntity] = {
    sparkSession.sparkContext.makeRDD(distinctOutputArray,Math.min(1000,distinctOutputArray.length)).flatMap(dir => {
      val list = ParquetHiveUtils.getParquetFilesFromHDFS(Constants.DATA_WAREHOUSE + File.separator + dir)
      val fieldInfos = if (list.size > 0) {
        val parquetMetaData = ParquetHiveUtils.parseSQLFieldInfos(list.head.getPath)
        parquetMetaData.map(meta => {
          val fieldName = meta._1
          val fieldType = meta._2
          val fieldSql = meta._3
          val rowType = meta._4
          val rowInfo = meta._5
          val logFileFieldDescEntity = new LogFileFieldDescEntity
          logFileFieldDescEntity.setTaskId(taskId)
          logFileFieldDescEntity.setLogPath(Constants.DATA_WAREHOUSE + File.separator + dir)
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
      LOG.info(s"FieldInfos: $dir -> ${fieldInfos.size} ")
      fieldInfos
    }).collect()
  }*/

  def generateFieldDescEntityArrayBuffer(sparkSession:SparkSession,taskId: String, distinctOutputArray: Array[String]): Seq[LogFileFieldDescEntity] = {
    distinctOutputArray.flatMap(dir => {
      val list = ParquetHiveUtils.getParquetFilesFromHDFS(Constants.DATA_WAREHOUSE + File.separator + dir)
      val fieldInfos = if (list.size > 0) {
        val parquetMetaData = ParquetHiveUtils.parseSQLFieldInfos(list.head.getPath)
        parquetMetaData.map(meta => {
          val fieldName = meta._1
          val fieldType = meta._2
          val fieldSql = meta._3
          val rowType = meta._4
          val rowInfo = meta._5
          val logFileFieldDescEntity = new LogFileFieldDescEntity
          logFileFieldDescEntity.setTaskId(taskId)
          logFileFieldDescEntity.setLogPath(Constants.DATA_WAREHOUSE + File.separator + dir)
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
      LOG.info(s"FieldInfos: $dir -> ${fieldInfos.size} ")
      println(s"----FieldInfos: $dir -> ${fieldInfos.size} ")
      fieldInfos
    })
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
      val appId = file_value_map.get(LogKeys.LOG_APP_ID).getOrElse("noAppId")
      file_value_map.filter(e => !e._1.contains(LogKeys.LOG_APP_ID)).map(field => {
        val logFileKeyFieldValueEntity = new LogFileKeyFieldValueEntity()
        logFileKeyFieldValueEntity.setAppId(appId)
        logFileKeyFieldValueEntity.setLogPath(Constants.DATA_WAREHOUSE + File.separator + outputPath)
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


  /** 记录使用规则库过滤【字段黑名单、重命名、行过滤】 */
  def ruleHandle(pathRdd: RDD[(String, JSONObject, scala.collection.mutable.Map[String, String])], resultRdd: RDD[(String, ProcessResult[JSONObject])]): RDD[(String, JSONObject)] = {
    // 获得规则库的每条规则
    val rules = metaDataUtils.parseSpecialRules(pathRdd)
    println("---------------------rules" + rules)
    rules.foreach(println)
    println("---------------------rulesend" + rules)

    val afterRuleRdd = resultRdd.map(e => {
      val path = e._1
      val jsonObject = e._2.result.get

      //if(rules.filter(rule => (Constants.DATA_WAREHOUSE+File.separator+rule.path).equalsIgnoreCase(path)).size>0){
      if (rules.filter(rule => rule.path.equalsIgnoreCase(path)).size > 0) {
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
        val resultJsonObject = if (rowBlackFilter.filter(item => jsonObject.get(item._1) != null && item._2 == jsonObject.getString(item._1)).size > 0) None
        else Some(jsonObject)
        (path, resultJsonObject)
      } else {
        //当路径没有找到规则的情况
        (path, Some(jsonObject))
      }
    }).filter(e => !(e._2.isEmpty)).map(item => (item._1, item._2.get))
    afterRuleRdd
  }


  //medusa 2.x,not used
 /* def initAllProcessGroup(): scala.collection.mutable.HashMap[String, ProcessGroupTraitV2] = {
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
  }*/

  /**
    * 关停
    */
  def shutdown(): Unit = {
  }
}




