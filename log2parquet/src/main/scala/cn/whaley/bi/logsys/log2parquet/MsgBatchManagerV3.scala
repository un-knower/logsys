package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.entity.LogFromEntity
import cn.whaley.bi.logsys.log2parquet.traits._
import cn.whaley.bi.logsys.log2parquet.utils.PhoenixUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by michael on 2017/6/22.
  */
class MsgBatchManagerV3 extends InitialTrait with NameTrait with LogTrait {

  private val config = new SparkConf()
  private val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
  private var inputPath = ""

  /**
    * 初始化方法
    * 如果初始化异常，则应该抛出异常
    * michael,cause of use spark distribute compute model,change to init processorChain in foreachPartition
    */
  override def init(confManager: ConfManager): Unit = {
    inputPath = confManager.getConf("inputPath")


    //批量加载metadata.applog_key_field_desc表
    //批量加载metadata.applog_special_field_desc表
    //将数据结构作为广播变量

    val stat=PhoenixUtils.getStatement
    stat.execute("select ")


  }

  /**
    * 启动
    *
    *
    */
  def start(): Unit = {
    val rdd_original = sparkSession.sparkContext.textFile(inputPath, 200)
    //按logType和eventId分类
    rdd_original.foreachPartition(
      partition => {
        val confManager = new ConfManager(Array("MsgBatchManagerV2.xml", "settings.properties"))
        val logProcessGroupName = confManager.getConf(this.name, "LogProcessGroup")
        val processGroupInstance=instanceFrom(confManager, logProcessGroupName).asInstanceOf[ProcessGroupTraitV2]


        //批量加载metadata.applog_key_field_desc表
        //批量加载metadata.applog_special_field_desc表


        //分叉处理medusa2x的处理器组初始化
        initAllProcessGroup

        partition.foreach(line => {
          val jsonObject = string2JsonObject(line)
          if (null != jsonObject) {
            if (jsonObject.containsKey(LogKeys.LOG_APP_ID)) {
              val appID = jsonObject.getString(LogKeys.LOG_APP_ID)
              if("medusa2xappID".equalsIgnoreCase(appID)){
                //进入分叉逻辑
                val jsonObjectAfter=initAllProcessGroup.get(appID).get.process(jsonObject).result.get
                //val log_type=jsonObjectAfter.getString(LogKeys.LOG_BODY_REAL_LOG_TYPE)
                //(log_type,jsonObjectAfter.toJSONString)
              }else{
                val jsonObjectAfter=processGroupInstance.process(jsonObject).result.get
                //(jsonObjectAfter.toJSONString)
              }
            }
          }
        })
      })

    /**输出路径
      * 通过appid读取[metadata.applog_key_field_desc]表，通过【表字段，分区字段（排序）】获得输出路径的非hive表非分区字段，
      * 通过logTime获得key_day和key_hour获得hive表分区字段。
      */
    //val outputPath=PathUtil.getOdsViewPath(appID, startDate, startHour, "logType", "eventID")
  }

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
    val keyword="appIdForProcessGroup."
    val appId2ProcessGroupName=confManager.getKeyValueByRegex(keyword)
    val appId2processGroupInstance = scala.collection.mutable.HashMap.empty[String, ProcessGroupTraitV2]

    appId2ProcessGroupName.foreach(e => {
      val appId=e._1
      val processGroupName=e._2
      val processGroupInstance=processGroupName2processGroupInstance.get(processGroupName).get
      appId2processGroupInstance.put(appId,processGroupInstance)
    })
    appId2processGroupInstance
  }

  def string2JsonObject(log: String): JSONObject = {
    try {
      val json = JSON.parseObject(log)
      json
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }

  /**
    * 关停
    */
  def shutdown(): Unit = {
    if (sparkSession != null) {
      sparkSession.close()
    }
  }
}
