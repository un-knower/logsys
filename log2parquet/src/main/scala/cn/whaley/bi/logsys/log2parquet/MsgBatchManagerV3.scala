package cn.whaley.bi.logsys.log2parquet

import java.io.File
import java.util.Date

import cn.whaley.bi.logsys.common.{ConfManager, IdGenerator}
import cn.whaley.bi.logsys.log2parquet.constant.{Constants, LogKeys}
import cn.whaley.bi.logsys.log2parquet.moretv2x.{LogPreProcess, LogUtils}
import cn.whaley.bi.logsys.log2parquet.traits._
import cn.whaley.bi.logsys.log2parquet.utils._
import cn.whaley.bi.logsys.metadata.entity.{LogFieldTypeInfoEntity, LogFileFieldDescEntity, LogFileKeyFieldValueEntity}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

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
    val timeout = confManager.getConfOrElseValue("metadata", "readTimeout", "300000").toInt
    println("-----timeout:" + timeout)
    println("-----metadataService:" + metadataService)
    metaDataUtils = new MetaDataUtils(metadataService, timeout)
  }

  def initMetaData(metaData: MetaDataUtils): Unit ={
    metaDataUtils = metaData
  }

  /**
    * 启动
    */
  def start(confManager: ConfManager): Unit = {
    val config = new SparkConf()
    //区分大小写，方能处理SSID，ssid同时存在于一条记录的情况
    config.set("spark.sql.caseSensitive", "true")
    //本地化测试使用,MainObjTests
    if (confManager.getConf("masterURL") != null) {
      config.setMaster(confManager.getConf("masterURL"))
      println("---local master url:" + confManager.getConf("masterURL"))
    }
    val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()

    val sc = sparkSession.sparkContext

    //创建累加器
    val renameFiledMyAcc = new MyAccumulator
    val baseInfoRenameFiledMyAcc = new MyAccumulator
    val removeFiledMyAcc = new MyAccumulator
    val fieldTypeSwitchMyAcc = new MyAccumulator
    val myAccumulator =  new MyAccumulator
    sc.register(renameFiledMyAcc,"renameFiledMyAcc")
    sc.register(baseInfoRenameFiledMyAcc,"baseInfoRenameFiledMyAcc")
    sc.register(removeFiledMyAcc,"removeFiledMyAcc")
    sc.register(fieldTypeSwitchMyAcc,"fieldTypeSwitchMyAcc")
    sc.register(myAccumulator,"myAccumulator")

    //读取原始文件
    val inputPath = confManager.getConf("inputPath")
    var rdd_original = sc.textFile(inputPath)
    //添加执行过滤某个appId
    val appId = confManager.getConf("appId")
    if(!("all").equals(appId)){
      rdd_original = rdd_original.filter(line=>{
        try{
          JSON.parseObject(line).getString("appId").equals(appId)
        } catch {
          case _: Throwable => {
            true
          }
        }
      })
    }
    val original = rdd_original.map(line => {
      try {
        //如果是medusa2x日志，首先做规范化处理
        if (line.indexOf("\"appId\":\"" + Constants.MEDUSA2X_APP_ID + "\"") > 0) {
          val jsObj = JSON.parseObject(line)
          val logBody = jsObj.getJSONObject(LogKeys.LOG_BODY)
          val msgStr = logBody.getString(LogKeys.LOG)
          val svrReceiveTime = logBody.getLong(LogKeys.SVR_RECEIVE_TIME)
          val logType = LogUtils.getLogType(msgStr)
          val logData = LogPreProcess.matchLog(logType, s"$svrReceiveTime-$msgStr").toJSONObject
          logBody.asInstanceOf[java.util.Map[String, Object]].putAll(logData.asInstanceOf[java.util.Map[String, Object]])
          Some(jsObj)
        } else {
          //非medusa2x日志
          Some(JSON.parseObject(line))
        }
      } catch {
        case _: Throwable => {
          None
        }
      }
    }).filter(row => row.isDefined).map(row => row.get)




    //经过处理器链处理
    val logProcessGroupName = confManager.getConf(this.name, "LogProcessGroup")
    val processGroupInstance = instanceFrom(confManager, logProcessGroupName).asInstanceOf[ProcessGroupTraitV2]
    val processedRdd = original.map(e => {
      processGroupInstance.process(e)
    })

    //输出异常记录到HDFS文件
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val startDate = confManager.getConf("startDate")
    val startHour = confManager.getConf("startHour")
    val time = s"${startDate}${startHour}"
    //将经过处理器处理后，正常状态的记录使用规则库过滤【字段黑名单、重命名、行过滤】
    val okRowsRdd = processedRdd.filter(row => !row.hasErr).map(row=>row.result.get)
    //解析出输出目录
    var rddSchema = metaDataUtils.parseLogObjRddPath(okRowsRdd,startDate,startHour)(myAccumulator)

    //添加执行过滤某个日志类型逻辑
    val tableName = confManager.getConf("tableName")
    rddSchema = if(!("all").equals(tableName)){
      rddSchema.filter(f=>{
        val svrTableName = f._2.getString("svrTableName")
        tableName.split("\\,").contains(svrTableName)
      })
    }else{
      rddSchema
    }



    //处理过滤黑名单表
    val blackTableInfoEntitys = metaDataUtils.metadataService().getAllBlackTableDesc()
    if(blackTableInfoEntitys.size != 0 ){
      //获取黑名单表明
      val blackTableList = blackTableInfoEntitys.map(entity=>{
        entity.getTableName
      })
      rddSchema = rddSchema.filter(f=>{
        val tableName = f._2.getString("svrTableName")
        if(blackTableList.contains(tableName)){
          false
        }else{
          true
        }
      })
    }
    //数据类型处理
    val logFieldTypeInfoEntitys = metaDataUtils.metadataService().getAllLogFieldTypeInfo()
    val fieldLevel = getFieldTypeMap(logFieldTypeInfoEntitys,"field")
    val realLogTypeLevel = getFieldTypeMap(logFieldTypeInfoEntitys,"realLogType")
    val tableLevel = getFieldTypeMap(logFieldTypeInfoEntitys,"table")
    val pathRdd = rddSchema.map(row=>{
      val path = row._1
      val jSONObject = row._2
      //tableName
      val realLogType = jSONObject.getString("realLogType")
      val tableName = jSONObject.getString("svrTableName")

      //全量字段
      val fieldAllMap = if(fieldLevel.size != 0 ){
        fieldLevel.get("ALL").get
      }else{
        Map[String,String]()
      }
      //logType级别字段
      val logTypeFieldMap = if(realLogTypeLevel.size != 0 && realLogTypeLevel.keySet.contains(realLogType)){
        realLogTypeLevel.get(realLogType).get
      }else{
        Map[String,String]()
      }
     //表级别字段
      val tableFieldMap = if(tableLevel.size != 0 && tableLevel.keySet.contains(tableName)){
        tableLevel.get(tableName).get
      }else{
        Map[String,String]()
      }
      val it = jSONObject.keySet().toList
      it.foreach(key => {
        if(tableFieldMap.size !=0 && tableFieldMap.keySet.contains(key.trim.toLowerCase)){
          //table级别
          processFiledType(fieldTypeSwitchMyAcc,jSONObject,key,tableFieldMap)
        }else if(logTypeFieldMap.size !=0 && logTypeFieldMap.keySet.contains(key.trim.toLowerCase)){
          //logType级别
          processFiledType(fieldTypeSwitchMyAcc,jSONObject,key,logTypeFieldMap)
        }else if(fieldAllMap.size !=0 && fieldAllMap.keySet.contains(key.trim.toLowerCase)){
          //全量级别
          processFiledType(fieldTypeSwitchMyAcc,jSONObject,key,fieldAllMap)
        }
      })

      jSONObject.remove("svrTableName")
      (path,jSONObject)
    })
//    pathRdd.foreach(f=>{
//     println(f._2)
//   })
//    System.exit(-1)
//
//    val pathRdd = rddSchema.map(row=>{
//      val path = row._1
//      val jSONObject = row._2
//      (path,jSONObject)
//    })
    val afterRuleRdd = ruleHandle(sc,pathRdd)(myAccumulator,renameFiledMyAcc,baseInfoRenameFiledMyAcc,removeFiledMyAcc)
    //输出正常记录到HDFS文件
    println("-------Json2ParquetUtil.saveAsParquet start at "+new Date())
    val appLogSecialEntities = metaDataUtils.queryAppLogSpecialFieldDescConf()
    //baseInfo信息
    val baseInfoEntities = metaDataUtils.metadataService().getAllLogBaseInfo()

    Json2ParquetUtil.saveAsParquet(afterRuleRdd,time,sparkSession,appLogSecialEntities,baseInfoEntities)
//    afterRuleRdd.foreach(f=>{
//      println(f._2)
//    })
//    println(s"afterRuleRdd count : ${afterRuleRdd.count()}")
//    System.exit(1)
    println("-------Json2ParquetUtil.saveAsParquet end at "+new Date())
    //删除输入数据源
    if(fs.exists(new Path(inputPath))){
     fs.delete(new Path(inputPath),true)
    }

    println("=============== 规则处理移除字段 =============== ：")
    removeFiledMyAcc.value.toList.sortBy(_._2).foreach(r=>{
      if(isValid(r._1)){
          println("移除字段："+r._1+" --> 移除次数:"+r._2)
      }
    })
/*
    val lines = FileUtil.loadFile()
    val map = new mutable.HashMap[String,String]
    lines.filter(f=> ! f.contains("#") && ! f.isEmpty).foreach(f=>{
      val key = f.split(":")(0)
      val productLine = f.split(":")(1)
      val flag = f.split(":")(2)
      if("0".equals(flag)){
        map += (key->productLine)
      }
    })

    removeFiledMyAcc.value.toList.sortBy(-_._2).foreach(r=>{
      if(isValid(r._1)){
        //处在名单里，且flag=0的不打印
        val key = r._1.split("->")(0)
        val value = r._1.split("->")(1)
        if(!map.getOrElseUpdate(key,"").equals(value)){
          println("移除字段："+r._1+" --> 移除次数:"+r._2)
        }
      }
    })
*/

    val fieldTypeSwitchMap = fieldTypeSwitchMyAcc.value.toMap
    println("=============== 字段类型转换异常:表字段级别 =============== ：")
    fieldTypeSwitchMyAcc.value.toList.filter(r=>{
      r._1.contains("table:")
    }).sortBy(-_._2).foreach(r=>{
      val name = r._1.split("->")(0)
      val times = r._2
      val total = fieldTypeSwitchMap.getOrDefault(name,1)
      val rate = times.toDouble/total
      println(s"字段类型转换异常 表字段：${name} --> 转换异常次数: ${times} -> 总次数：${total} ->占比：${rate} ")
    })

    println("=============== 字段类型转换异常:字段级别 =============== ：")
    fieldTypeSwitchMyAcc.value.toList.filter(r=>{
       r._1.contains("field:")
    }).sortBy(-_._2).foreach(r=>{
      val name = r._1.split("->")(0)
      val times = r._2
      val total = fieldTypeSwitchMap.getOrDefault(name,1)
      val rate = times.toDouble/total
      println(s"字段类型转换异常 字段级别：${name} --> 转换异常次数: ${times} -> 总次数：${total} ->占比：${rate} ")
    })

    println("=============== 规则处理baseInfo 重命名字段 =============== ：")
    baseInfoRenameFiledMyAcc.value.toList.sortBy(-_._2).foreach(r=>{
      if(isValid(r._1)){
        println("baseinfo 重命名字段："+r._1+" --> 重命名次数:"+r._2)
      }
    })

    println("=============== 规则处理重命名字段 =============== ：")
    renameFiledMyAcc.value.toList.sortBy(-_._2).foreach(r=>{
      if(isValid(r._1)){
        println("重命名字段："+r._1+" --> 重命名次数:"+r._2)
      }
    })

    myAccumulator.value.foreach(r=>{
      val key = r._1
      key match {
        case "exceptionJsonAcc" => println("处理链前删除异常记录数 : "+r._2)
        case "jsonRowAcc" => println("规则处理后的记录数 : "+r._2)
        case "okRowAcc" => println("处理链后成功记录数 : "+r._2)
        case "deleteRowAcc" => println("规则处理删除记录数 : "+r._2)
        case "removeFiledAcc" => println("规则处理移除字段记录数 : "+r._2)
        case "renameFiledAcc" => println("规则处理重命名字段记录数 : "+r._2)
        case "baseInfoRenameFiledMyAcc" => println("规则处理baseInfo重命名字段记录数 : "+r._2)
        case _ => ""
      }
    })

  }



  /**
    * 向phoenix表插入数据[metadata.logfile_key_field_value,metadata.logfile_field_desc]
    **/
  def generateMetaDataToTable(sparkSession: SparkSession, pathSchema:Array[(String,scala.collection.mutable.Map[String,String])],taskFlag:String): Unit = {
    //生成元组的RDD，元组内容为:(输出路径,用来拼接表名称或分区的字段的Map[logType->detail,key_day->20170712,....])
    val path_file_value_map = pathSchema
    println("path_file_value_map.length():" + path_file_value_map.length)
    println("---------------------")

    //生成taskId
    val generator = IdGenerator.defaultInstance
    val taskId = generator.nextId().replace("/", "")
    println("----------taskId:" + taskId)

    //生成metadata.logfile_key_field_value表数据
    val fieldValueEntityArrayBuffer = generateFieldValueEntityArrayBuffer(taskId, path_file_value_map)
    println("fieldValueEntityArrayBuffer.length:" + fieldValueEntityArrayBuffer.length)
    LOG.info("fieldValueEntityArrayBuffer.length:" + fieldValueEntityArrayBuffer.length)
    println("-------batchPostFieldValue start at "+new Date())
    batchPostFieldValue(taskId, fieldValueEntityArrayBuffer)
    println("-------batchPostFieldValue end at "+new Date())

    //获得不同的输出路径
    val distinctOutput = path_file_value_map.map(e => {
      e._1
    }).distinct
    //打印输出路径
    println("------distinctOutput.begin:" + distinctOutput.length)
    distinctOutput.foreach(println)
    println("------distinctOutput.end")
    LOG.info("------distinctOutput.begin:" + distinctOutput.length)
    distinctOutput.foreach(e => {
      LOG.info(e)
    })
    LOG.info("------distinctOutput.end")

    //生成metadata.logfile_field_desc表数据
    val fieldDescEntityArrayBuffer = generateFieldDescEntityArrayBuffer(sparkSession, taskId, distinctOutput)
    println("fieldDescEntityArrayBuffer.length:" + fieldDescEntityArrayBuffer.length)
    LOG.info("fieldDescEntityArrayBuffer.length:" + fieldDescEntityArrayBuffer.length)
    println("-------batchPostFileFieldDesc start at "+new Date())
    batchPostFileFieldDesc(taskId, fieldDescEntityArrayBuffer)
    println("-------batchPostFileFieldDesc end at "+new Date())

    //发送taskId给元数据模块
    println("-------发送taskId给元数据模块 start at "+new Date())
    val responseTaskIdResponse = metaDataUtils.metadataService().postTaskId2MetaModel(taskId, taskFlag)
    LOG.info(s"responseTaskIdResponse : " + responseTaskIdResponse.toJSONString)
    println(s"----responseTaskIdResponse : " + responseTaskIdResponse.toJSONString)
    println("-------发送taskId给元数据模块 end at "+new Date())

  }

  /** 批量发送FileFieldValue的POST请求 */
  def batchPostFieldValue(taskId: String, seq: Seq[LogFileKeyFieldValueEntity]) {
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
        val responseFieldDesc = metaDataUtils.metadataService().putLogFileKeyFieldValue(taskId, fragment)
        LOG.info(s"batch $i,$start->$end end: " + new Date())
        println(s"----batch $i,$start->$end end: " + new Date())
        LOG.info(s"batch $i,$start->$end : " + responseFieldDesc.toJSONString)
        println(s"----batch $i,$start->$end : " + responseFieldDesc.toJSONString)
        start = batchSize * i
      }
      val remain = seq.length - times * batchSize
      if (remain > 0) {
        val fragment = seq.slice(times * batchSize, seq.length)
        val responseFieldDesc = metaDataUtils.metadataService().putLogFileKeyFieldValue(taskId, fragment)
        LOG.info(s"FieldValue_remain ${fragment.length}: " + responseFieldDesc.toJSONString)
        println(s"----FieldValue_remain ${fragment.length}: " + responseFieldDesc.toJSONString)
      }
    } else {
      val responseFieldDesc = metaDataUtils.metadataService().putLogFileKeyFieldValue(taskId, seq)
      LOG.info(s"FieldValue_responseFieldDesc : " + responseFieldDesc.toJSONString)
      println(s"----FieldValue_responseFieldDesc : " + responseFieldDesc.toJSONString)

    }
  }

  /** 批量发送FileFieldDesc的POST请求 */
  def batchPostFileFieldDesc(taskId: String, seq: Seq[LogFileFieldDescEntity]) {
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
        val responseFieldDesc = metaDataUtils.metadataService().putLogFileFieldDesc(taskId, fragment)
        LOG.info(s"batch $i,$start->$end end: " + new Date())
        println(s"----batch $i,$start->$end end: " + new Date())
        LOG.info(s"batch $i,$start->$end : " + responseFieldDesc.toJSONString)
        println(s"----batch $i,$start->$end : " + responseFieldDesc.toJSONString)
        start = batchSize * i
      }
      val remain = seq.length - times * batchSize
      if (remain > 0) {
        val fragment = seq.slice(times * batchSize, seq.length)
        val responseFieldDesc = metaDataUtils.metadataService().putLogFileFieldDesc(taskId, fragment)
        LOG.info(s"remain ${fragment.length}: " + responseFieldDesc.toJSONString)
        println(s"----remain ${fragment.length}: " + responseFieldDesc.toJSONString)
      }
    } else {
      val responseFieldDesc = metaDataUtils.metadataService().putLogFileFieldDesc(taskId, seq)
      LOG.info(s"responseFieldDesc : " + responseFieldDesc.toJSONString)
      println(s"----responseFieldDesc : " + responseFieldDesc.toJSONString)
    }
  }

  /**
    * 通过makeRDD方式,并发生成FieldDescEntity，但是发现原有方式这段耗时1分钟
    * Seq[(fieldName:String,fieldType:String,fieldSql:String,rowType:String,rowInfo:String)]
    **/
  def generateFieldDescEntityArrayBuffer(sparkSession: SparkSession, taskId: String, distinctOutputArray: Array[String]): Seq[LogFileFieldDescEntity] = {
    distinctOutputArray.flatMap(dir => {
      val list = ParquetHiveUtils.getParquetFilesFromHDFS(Constants.DATA_WAREHOUSE + File.separator + dir)
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
      if(fieldInfo.size>80){
       LOG.info(s"parquet文件schema字段个数: $dir -> ${fieldInfo.size} ")
       println(s"----parquet文件schema字段个数: $dir -> ${fieldInfo.size} ")
      }
      fieldInfo
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
      file_value_map.filter(e => !e._1.contains(LogKeys.LOG_APP_ID)).foreach(field => {
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
  def ruleHandle(sc:SparkContext,resultRdd: RDD[(String, JSONObject)])(
    implicit myAccumulator:MyAccumulator=new MyAccumulator,
    renameFiledMyAcc:MyAccumulator=new MyAccumulator,
    baseInfoRenameFiledMyAcc:MyAccumulator=new MyAccumulator,
    removeFiledMyAcc:MyAccumulator=new MyAccumulator
  ): RDD[(String, JSONObject)] = {
    // 获得规则库的每条规则
    val rules = metaDataUtils.parseSpecialRules(resultRdd)
    val rowBlackFilterMap = mutable.Map[String,Map[String,String]]()
    rules.foreach(rule=>{
      val path = rule.path
      //行删除
      val rowBlackFilter = rule.rowBlackFilter
      val map1 = mutable.Map[String,String]()
      rowBlackFilter.foreach(e=>{
        val key = e._1
        val value = e._2
        map1.put(key,value)
      })
      rowBlackFilterMap.put(path,map1.toMap)

    })

    val myBroadcast = MyBroadcast(rowBlackFilterMap.toMap)
    val broadcast = sc.broadcast(myBroadcast)
    //TODO debug
    val afterRuleRdd = resultRdd.map(e => {
      val rowBlackFilterMap = broadcast.value.rowBlackFilterMap
      myAccumulator.add("okRowAcc")
      val path = e._1
      val jsonObject = e._2

      //删除行
      if(rowBlackFilterMap.keySet.contains(path) && !rowBlackFilterMap.get(path).isEmpty){
          //该path下有删除行规则
        val map = rowBlackFilterMap.get(path).get
        map.keys.foreach(key=>{
          if(jsonObject.containsKey(key) && jsonObject.getString(key) == map.get(key).get){
            jsonObject.clear()
          }
        })
        //累加器影响的行数
        if(jsonObject.isEmpty){
          //记录删除
          myAccumulator.add("deleteRowAcc")
        }
      }

      (path, Some(jsonObject))
    }).filter(e => !(e._2.isEmpty)).map(item => {
      //统计输出json数据量
      //      jsonRowAcc.add(1)
      myAccumulator.add("jsonRowAcc")
      (item._1, item._2.get)
    })
    afterRuleRdd
  }
  //释放资源
  def shutdown(): Unit = {
    //sparkSession.close()
  }
  case class MyBroadcast(rowBlackFilterMap:Map[String,Map[String,String]])

  def isValid(s:String)={
    val regex = """[a-zA-Z0-9-_>]*"""
    s.matches(regex)
  }

  /**
    * json key 的正则
    * @param s
    * @return
    */
  def isinValidKey(s:String)={
    val regex = "^[0-9]*$"
    s.matches(regex)
  }

  /**
    *获取字段对应的类型
    * @param logFieldTypeInfoEntitys
    * @param ruleLevel table 表级别,realLogType 日志类型级别,field字段级别
    */
  def getFieldTypeMap(logFieldTypeInfoEntitys : List[LogFieldTypeInfoEntity],ruleLevel:String): Map[String,Map[String,String]] ={
    val map =  mutable.Map[String,Map[String,String]]()

   logFieldTypeInfoEntitys
      .filter(entity=>{
        entity.getRuleLevel.equals(ruleLevel)
    }).groupBy(entity=>{
     //以name 分组
      entity.getName
    }).foreach(e=>{
     val fieldTypeMap = mutable.Map[String,String]()
      val name = e._1
      val entities = e._2
      entities.foreach(entitiy=>{
        val filedName = entitiy.getFieldName
        val typeFlag = entitiy.getTypeFlag
        fieldTypeMap.put(filedName,typeFlag)
      })
      map.put(name.trim,fieldTypeMap.toMap)
    })
    map.toMap
  }

  def processFiledType(fieldTypeSwitchMyAcc:MyAccumulator,jsonObject:JSONObject,key:String,fieldTypeMap:Map[String,String]): Unit ={
    //字段类型标识 1:String 2:Long 3:Double 4:Array
    val typeFlag = fieldTypeMap.get(key.trim.toLowerCase).get
    typeFlag match {
      case "1" => switchString(fieldTypeSwitchMyAcc,key,jsonObject)
      case "2" => switchLong(fieldTypeSwitchMyAcc,key,jsonObject)
      case "3" => switchDouble(fieldTypeSwitchMyAcc,key,jsonObject)
      case "4" => switchJsonArray(fieldTypeSwitchMyAcc,key,jsonObject)
      case "5" => switchJsonArrayString(fieldTypeSwitchMyAcc,key,jsonObject)
      case "6" => switchJsonArrayBigInt(fieldTypeSwitchMyAcc,key,jsonObject)
      case "7" => switchJsonArrayStruct(fieldTypeSwitchMyAcc,key,jsonObject)
      case _ =>
    }
  }

  /**
    * 转换为string
    * @param key
    * @param json
    */
  def switchString(fieldTypeSwitchMyAcc:MyAccumulator,key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      json.put(key,value)
    }catch {
      case e:Exception=>{
        e.printStackTrace()
        json.remove(key)
      }
    }
  }

  /**
    * 转换为Array
    * @param key
    * @param json
    */
  def switchJsonArray(fieldTypeSwitchMyAcc:MyAccumulator,key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      val jSONArray = JSON.parseArray(value)
      if(jSONArray.size() == 0){
        json.remove(key)
      }else{
        json.put(key,jSONArray)
      }
    }catch {
      case e:Exception=>{
        e.printStackTrace()
        json.remove(key)
      }
    }
  }


  /**
    * Array 中字段 转换成String类型
    * array String
    * @param key
    * @param json
    */
  def switchJsonArrayString(fieldTypeSwitchMyAcc:MyAccumulator,key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      val jSONArray = JSON.parseArray(value)
      if(jSONArray.size() == 0){
        json.remove(key)
      }else{
        val newJsonArray = new JSONArray()
        for( i<- 0 to ( jSONArray.size()-1 )){
          val value = jSONArray.get(i).toString
          newJsonArray.add(value)
        }
        json.put(key,newJsonArray)
      }
    }catch {
      case e:Exception=>{
        json.remove(key)
      }
    }
  }
  /**
    * array bigInt
    * @param key
    * @param json
    */
  def switchJsonArrayBigInt(fieldTypeSwitchMyAcc:MyAccumulator,key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      val jSONArray = JSON.parseArray(value)
      if(jSONArray.size() == 0){
        json.remove(key)
      }else{
        val newJsonArray = new JSONArray()

        for( i<- 0 to ( jSONArray.size()-1 )){
          val value = jSONArray.get(i).toString
          newJsonArray.add(value.toLong)
        }
        json.put(key,newJsonArray)
      }
    }catch {
      case e:Exception=>{
        json.remove(key)
      }
    }
  }



  /**
    * array struct
    * @param key
    * @param json
    */
  def switchJsonArrayStruct(fieldTypeSwitchMyAcc:MyAccumulator,key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      val jSONArray = JSON.parseArray(value)
      if(jSONArray.size() == 0){
        json.remove(key)
      }else{
        val newJsonArray = new JSONArray()
        for( i<- 0 to ( jSONArray.size()-1 )){
          val jsonObject = jSONArray.getJSONObject(i)
          val keys = jsonObject.keySet().toList
          keys.foreach(key=>{
            jsonObject.put(key,jsonObject.getString(key))
          })
          newJsonArray.add(jsonObject)
        }
        json.put(key,newJsonArray)
      }
    }catch {
      case e:Exception=>{
        json.remove(key)
      }
    }
  }





  /**
    * 转换成Long
    * @param key
    * @param json
    */
  def switchLong(fieldTypeSwitchMyAcc:MyAccumulator,key:String,json:JSONObject): Unit ={
    val tableName = json.getString("svrTableName")
    fieldTypeSwitchMyAcc.add(s"${tableName}:${key}")
    fieldTypeSwitchMyAcc.add(s"${key}")
    try {
      val value = json.getString(key).trim
      json.put(key,value.toLong)
    }catch {
      case e:Exception=>{
        fieldTypeSwitchMyAcc.add(s"table:${tableName}:${key}->long exception")
        fieldTypeSwitchMyAcc.add(s"field:${key}->long exception")
        e.printStackTrace()
        json.remove(key)
      }
    }
  }

  /**
    * 转换成double

    */
  private def switchDouble(fieldTypeSwitchMyAcc:MyAccumulator,key:String,json:JSONObject):Unit = {
    try {
      val value = json.getString(key).trim
      json.put(key,value.toDouble)
    }catch {
      case e:Exception=>{
        e.printStackTrace()
        json.remove(key)
      }
    }
  }


}




