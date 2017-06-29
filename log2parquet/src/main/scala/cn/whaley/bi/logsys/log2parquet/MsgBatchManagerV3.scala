package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.traits._
import cn.whaley.bi.logsys.log2parquet.utils.{MetaDataUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * Created by michael on 2017/6/22.
  */
class MsgBatchManagerV3 extends InitialTrait with NameTrait with LogTrait {


  /**
    * 初始化方法
    * 如果初始化异常，则应该抛出异常
    * michael,cause of use spark distribute compute model,change to init processorChain in foreachPartition
    */
  override def init(confManager: ConfManager): Unit = {
    MsgBatchManagerV3.inputPath = confManager.getConf("inputPath")

    /**
      * 批量加载metadata.applog_special_field_desc表,将数据结构作为广播变量,用来作为黑白名单的过滤规则使用
      **/
    //TODO 黑白名单读取phoenix表

    /**
      * 批量加载metadata.applog_key_field_desc表,将数据结构作为广播变量,用来作为日志的输出路径模版使用
      **/
    val appId2OutputPathTemplateMap = MetaDataUtils.getAppId2OutputPathTemplateMap
    MsgBatchManagerV3.appId2OutputPathTemplateMapBroadCast = MsgBatchManagerV3.sparkSession.sparkContext.broadcast(appId2OutputPathTemplateMap)
  }

  /**
    * 启动
    *
    *
    */
  def start(): Unit = {
    val rdd_original = MsgBatchManagerV3.sparkSession.sparkContext.textFile(MsgBatchManagerV3.inputPath, 200)

    //初始化元数据表

    //按logType和eventId分类
    val rdd_result = rdd_original.mapPartitions(
      partition => {
        val confManager = new ConfManager(Array("MsgBatchManagerV2.xml", "settings.properties"))
        val logProcessGroupName = confManager.getConf(this.name, "LogProcessGroup")
        val processGroupInstance = instanceFrom(confManager, logProcessGroupName).asInstanceOf[ProcessGroupTraitV2]

        //分叉处理medusa2x的处理器组初始化
        initAllProcessGroup

        partition
          .map(line => string2JsonObject(line))
          .filter(obj => obj != null)
          .map(jsonObject => {
            val appID = jsonObject.getString(LogKeys.LOG_APP_ID)
            val ret = if ("boikgpokn78sb95ktmsc1bnken8tuboa".equalsIgnoreCase(appID)) {
              //进入分叉逻辑
              initAllProcessGroup.get(appID).get.process(jsonObject)
              //val jsonObjectAfter=initAllProcessGroup.get(appID).get.process(jsonObject).result.get
              //val outputPath= jsonObjectAfter.get("outputPath")
              //(outputPath,jsonObjectAfter.toJSONString)
            } else {
              processGroupInstance.process(jsonObject)

              //val jsonObjectAfter=processGroupInstance.process(jsonObject).result.get
              //val outputPath= jsonObjectAfter.get("outputPath")
              //(outputPath,jsonObjectAfter.toJSONString)
            }
            ret
          })

      })

    val errRows = rdd_result.filter(row => row.hasErr)

    val okRows=rdd_result.filter(row => row.hasErr == false).map(row => {
      val outputPath = row.result.get.getString("outputPath")
      (outputPath, row.result.get)
    })


    val rdd1=okRows.groupByKey(200)
    //mapPartitionsWithIndex



    /** 输出路径
      * 通过appid读取[metadata.applog_key_field_desc]表，通过【表字段，分区字段（排序）】获得输出路径的非hive表非分区字段，
      * 通过logTime获得key_day和key_hour获得hive表分区字段。
      *
      * boikgpokn78sb95ktmsc1bnkechpgj9l->log_medusa_main3x_${log_type}_${event_id}/key_day=${key_day}/key_hour=${key_hour}
      *
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
    if (MsgBatchManagerV3.sparkSession != null) {
      MsgBatchManagerV3.sparkSession.close()
    }
  }

}

object MsgBatchManagerV3 {
  val config = new SparkConf()
  val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
  var inputPath = ""
  var appId2OutputPathTemplateMapBroadCast: Broadcast[scala.collection.mutable.HashMap[String, String]] = _
}
