package cn.whaley.bi.logsys.log2parquet

import java.io.File
import java.util.Date

import cn.whaley.bi.logsys.common.{ConfManager, IdGenerator}
import cn.whaley.bi.logsys.log2parquet.constant.{Constants, LogKeys}
import cn.whaley.bi.logsys.log2parquet.moretv2x.{LogPreProcess, LogUtils}
import cn.whaley.bi.logsys.log2parquet.traits._
import cn.whaley.bi.logsys.log2parquet.utils._
import cn.whaley.bi.logsys.metadata.entity.{LogFileFieldDescEntity, LogFileKeyFieldValueEntity}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

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

  /**
    * 启动
    */
  def start(confManager: ConfManager): Unit = {
    val config = new SparkConf()
    //本地化测试使用,MainObjTests
    if (confManager.getConf("masterURL") != null) {
      config.setMaster(confManager.getConf("masterURL"))
      println("---local master url:" + confManager.getConf("masterURL"))
    }
    val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()

    val sc = sparkSession.sparkContext
    //创建累加器
    val renameFiledMyAcc = new MyAccumulator
    val removeFiledMyAcc = new MyAccumulator
    val myAccumulator =  new MyAccumulator
    sc.register(renameFiledMyAcc,"renameFiledMyAcc")
    sc.register(removeFiledMyAcc,"removeFiledMyAcc")
    sc.register(myAccumulator,"myAccumulator")

    //读取原始文件
    val inputPath = confManager.getConf("inputPath")
    val rdd_original = sc.textFile(inputPath).map(line => {
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
    val processedRdd = rdd_original.map(e => {
      processGroupInstance.process(e)
    })
//      .repartition(3000)
      .persist(StorageLevel.MEMORY_AND_DISK)


    //输出异常记录到HDFS文件
    val errRowsRdd = processedRdd.filter(row => {
      if(row.hasErr){
        myAccumulator.add("errRowAcc")
      }
        row.hasErr
    })
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val startDate = confManager.getConf("startDate")
    val startHour = confManager.getConf("startHour")
    val time = s"${startDate}${startHour}"
    if(errRowsRdd.count()>0){
      val errRowNum = myAccumulator.value.getOrElseUpdate("errRowAcc",0)
      println(s"处理链后失败记录数 : $errRowNum")

      val errOutPath = s"${Constants.ODS_VIEW_HDFS_OUTPUT_PATH_TMP_ERROR}${File.separator}${time}"
      if(fs.exists(new Path(errOutPath))){
        fs.delete(new Path(errOutPath),true)
      }
      fs.deleteOnExit(new Path(errOutPath))
      errRowsRdd.repartition(5).saveAsTextFile(errOutPath)
    }

    //将经过处理器处理后，正常状态的记录使用规则库过滤【字段黑名单、重命名、行过滤】
    val okRowsRdd = processedRdd.filter(row => !row.hasErr).map(row=>row.result.get)
    //解析出输出目录
    val rddSchame = metaDataUtils.parseLogObjRddPath(okRowsRdd,startDate,startHour)(myAccumulator)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val pathRdd = rddSchame.map(row=>{
      val path = row._1
      val jSONObject = row._2
      (path,jSONObject)
    })
    val pathSchema:Array[(String,scala.collection.mutable.Map[String,String])] = rddSchame.map(row=>{
      val path = row._1
      val schema = row._3
      (path,schema)
    }).distinct().collect()

    val afterRuleRdd = ruleHandle(sc,pathRdd)(myAccumulator,renameFiledMyAcc,removeFiledMyAcc)
    //输出正常记录到HDFS文件
    println("-------Json2ParquetUtil.saveAsParquet start at "+new Date())
    Json2ParquetUtil.saveAsParquet(afterRuleRdd,time,sparkSession)
//    println(s"afterRuleRdd count : ${afterRuleRdd.count()}")
//    System.exit(1)
    println("-------Json2ParquetUtil.saveAsParquet end at "+new Date())
    rddSchame.unpersist()
    processedRdd.unpersist()
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
        case _ => ""
      }
    })
    //生成元数据信息给元数据模块使用

   val taskFlag = confManager.getConf("taskFlag")
    assert(taskFlag.length==3)
    generateMetaDataToTable(sparkSession, pathSchema,taskFlag)
  }



  /**
    * 向phoenix表插入数据[metadata.logfile_key_field_value,metadata.logfile_field_desc]
    **/
  def generateMetaDataToTable(sparkSession: SparkSession, pathSchema:Array[(String,scala.collection.mutable.Map[String,String])],taskFlag:String): Unit = {
    //生成元组的RDD，元组内容为:(输出路径,用来拼接表名称或分区的字段的Map[logType->detail,key_day->20170712,....])
    val path_file_value_map = pathSchema
    println("path_file_value_map.length():" + path_file_value_map.length)
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
        LOG.info(s"FieldValue_remain $remain: " + responseFieldDesc.toJSONString)
        println(s"----FieldValue_remain $remain: " + responseFieldDesc.toJSONString)
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
        LOG.info(s"remain $remain: " + responseFieldDesc.toJSONString)
        println(s"----remain $remain: " + responseFieldDesc.toJSONString)
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
    removeFiledMyAcc:MyAccumulator=new MyAccumulator
  ): RDD[(String, JSONObject)] = {
    // 获得规则库的每条规则
    val rules = metaDataUtils.parseSpecialRules(resultRdd)
    val rowBlackFilterMap = mutable.Map[String,Map[String,String]]()
    val fieldBlackFilterMap = mutable.Map[String,List[String]]()
    val renameMap = mutable.Map[String,Map[String,String]]()
    rules.foreach(rule=>{
      val path = rule.path
      //字段删除
      val fieldBlackFilter = rule.fieldBlackFilter
      val list = new ListBuffer[String]()
      fieldBlackFilter.foreach(e=>{
        list +=(e)
      })
      fieldBlackFilterMap.put(path,list.toList)
      //行删除
      val rowBlackFilter = rule.rowBlackFilter
      val map1 = mutable.Map[String,String]()
      rowBlackFilter.foreach(e=>{
        val key = e._1
        val value = e._2
        map1.put(key,value)
      })
      rowBlackFilterMap.put(path,map1.toMap)
      //重命名
      val rename = rule.rename
      val map2 = mutable.Map[String,String]()
      rename.foreach(e=>{
        val oldName = e._1
        val newName = e._2
        map2.put(oldName,newName)
      })
      renameMap.put(path,map2.toMap)

    })
    val myBroadcast = MyBroadcast(rowBlackFilterMap.toMap,fieldBlackFilterMap.toMap,renameMap.toMap)
    val broadcast = sc.broadcast(myBroadcast)
    //TODO debug
    val afterRuleRdd = resultRdd.map(e => {
      val rowBlackFilterMap = broadcast.value.rowBlackFilterMap
      val fieldBlackFilterMap =  broadcast.value.fieldBlackFilterMap
      val renameMap =  broadcast.value.renameMap
      myAccumulator.add("okRowAcc")
      val path = e._1
      val jsonObject = e._2
      //用于累加器比较规则处理后数据的变化
      val compareJson = JSON.parseObject(jsonObject.toJSONString)
      //删除行
      if(rowBlackFilterMap.keySet.contains(path) && !rowBlackFilterMap.get(path).isEmpty){
        val map = rowBlackFilterMap.get(path).get
        map.keys.foreach(key=>{
          if(jsonObject.containsKey(key) && jsonObject.getString(key) == map.get(key).get){
            jsonObject.clear()
          }
        })
      }
      if(!jsonObject.isEmpty){
        //删除字段
        if(fieldBlackFilterMap.keySet.contains(path) && !fieldBlackFilterMap.get(path).isEmpty){
          val array = fieldBlackFilterMap.get(path).get
          array.foreach(field=>{
            if(jsonObject.containsKey(field)){
              removeFiledMyAcc.add(field+"->"+path.split("/")(1).split("_")(1))
              jsonObject.remove(field)
            }
          })
        }
        //重命名
        if(renameMap.keySet.contains(path) && !renameMap.get(path).isEmpty){
          val map = renameMap.get(path).get
          map.keys.foreach(oldName=>{
            if(jsonObject.containsKey(oldName)){
              val newName = map.get(oldName).get
              jsonObject.put(newName, jsonObject.get(oldName))
              renameFiledMyAcc.add(oldName+"->"+path.split("/")(1).split("_")(1))
              jsonObject.remove(oldName)
            }
          })
        }
      }
      //累加器影响的行数
      if(!jsonObject.isEmpty){
        if(jsonObject.size()!=compareJson.size()){
          //删除字段
          myAccumulator.add("removeFiledAcc")
        }
        val keys = compareJson.keySet()
        val difKeysNum = jsonObject.keySet().toArray(new Array[String](0)).filter(key=>{
          !keys.contains(key)
        }).size
        if(difKeysNum >0){
          myAccumulator.add("renameFiledAcc")
          // renameFiledAcc.add(1)
        }
      }else{
        //记录删除
        myAccumulator.add("deleteRowAcc")
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
  case class MyBroadcast(rowBlackFilterMap:Map[String,Map[String,String]],fieldBlackFilterMap:Map[String,List[String]],renameMap:Map[String,Map[String,String]])

  def isValid(s:String)={
    val regex = """[a-zA-Z0-9-_>]*"""
    s.matches(regex)
  }

}




