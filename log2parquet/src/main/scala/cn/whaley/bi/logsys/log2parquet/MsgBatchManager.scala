package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.traits._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * Created by michael on 2017/6/22.
  */
class MsgBatchManager extends InitialTrait with NameTrait with LogTrait {

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
    //val processorChainName = confManager.getConf(this.name, "processorChain")
    //processorChain = instanceFrom(confManager, processorChainName).asInstanceOf[GenericProcessorChain]
    //sparkSession=SparkSession.builder().config(config).getOrCreate()
  }

  /**
    * 启动
    *
    *
    */
  def start(): Unit = {
    //加载数据
    /*val appID=""
    val startDate=""
    val startHour=""
    val partition=2000

    val odsPath = PathUtil.getOdsOriginPath(appID,startDate, startHour)
    val rdd_original = sparkSession.sparkContext.textFile(odsPath, partition)*/

    val rdd_original = sparkSession.sparkContext.textFile(inputPath, 200)
    //按logType和eventId分类
    rdd_original.foreachPartition(
      partition => {
        initAllProcessGroup
        partition.foreach(line => {
          val json = string2JsonObject(line)
          if (null != json) {
            if (json.containsKey(LogKeys.LOG_APP_ID)) {
              val appID = json.get(LogKeys.LOG_APP_ID)


            }
          }
        })
      })

    //
    //val outputPath=PathUtil.getOdsViewPath(appID, startDate, startHour, "logType", "eventID")
  }

  def initAllProcessGroup(): scala.collection.mutable.HashMap[String, ProcessGroupTrait] = {
    val processGroupName2processGroupInstance = scala.collection.mutable.HashMap.empty[String, ProcessGroupTrait]
    val confManager = new ConfManager(Array("MsgBatchManager.xml", "settings.properties"))
    val allProcessGroup = confManager.getConf(this.name, "allProcessGroup")
    require(null != allProcessGroup)
    allProcessGroup.split(",").foreach(groupName => {
      val groupNameFromConfig = confManager.getConf(this.name, groupName)
      val processGroup = instanceFrom(confManager, groupNameFromConfig).asInstanceOf[ProcessGroupTrait]
      processGroup.init(confManager)
      processGroupName2processGroupInstance += (groupNameFromConfig -> processGroup)
    })
    val keyword="appIdForProcessGroup."
    val appId2ProcessGroupName=confManager.getKeyValueByRegex(keyword)
    val appId2processGroupInstance = scala.collection.mutable.HashMap.empty[String, ProcessGroupTrait]

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
