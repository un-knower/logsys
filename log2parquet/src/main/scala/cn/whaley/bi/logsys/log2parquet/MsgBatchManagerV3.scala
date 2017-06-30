package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.traits._
import cn.whaley.bi.logsys.log2parquet.utils.MetaDataUtils.AppLogFieldSpecialRules
import cn.whaley.bi.logsys.log2parquet.utils.{MetaDataUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * Created by michael on 2017/6/22.
  */
class MsgBatchManagerV3 extends InitialTrait with NameTrait with LogTrait with java.io.Serializable {


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

    //广播规则库
    val rdd_original = MsgBatchManagerV3.sparkSession.sparkContext.textFile(MsgBatchManagerV3.inputPath, 2)
    val pathRdd = MetaDataUtils.parseLogStrRddPath(rdd_original)
    val specialRuels= MetaDataUtils.parseSpecialRules(pathRdd)
    MsgBatchManagerV3.specialRulesBroadCase = MsgBatchManagerV3.sparkSession.sparkContext.broadcast(specialRuels)

  }

  /**
    * 启动
    *
    *
    */
  def start(): Unit = {
    val rdd_original = MsgBatchManagerV3.sparkSession.sparkContext.textFile(MsgBatchManagerV3.inputPath, 2)
    println("rdd_original.count():"+rdd_original.count())
    LOG.info("rdd_original.count():"+rdd_original.count())

    val rdd_result = rdd_original.mapPartitions(
      partition => {
        val confManager = new ConfManager(Array("MsgBatchManagerV3.xml", "settings.properties"))
        val logProcessGroupName = confManager.getConf(this.name,"LogProcessGroup")
        println("---logProcessGroupName:"+logProcessGroupName)
        val processGroupInstance = instanceFrom(confManager, logProcessGroupName).asInstanceOf[ProcessGroupTraitV2]

        //分叉处理medusa2x的处理器组初始化
        //initAllProcessGroup

        partition
          .map(line => string2JsonObject(line))
          .filter(obj => obj != null)
          .map(jsonObject => {
            val appID = jsonObject.getString(LogKeys.LOG_APP_ID)
            val ret = if ("xxxxxxxxxxxxx_boikgpokn78sb95ktmsc1bnken8tuboa".equalsIgnoreCase(appID)) {
              //进入分叉逻辑
              //initAllProcessGroup.get(appID).get.process(jsonObject)
              processGroupInstance.process(jsonObject)
            } else {
              processGroupInstance.process(jsonObject)
            }
            //println(ret)
            ret
          })
      })



   /* val errRows = rdd_result.filter(row => row.hasErr).map(row => {
      val outputPath = row.result.get.getString(LogKeys.LOG_OUTPUT_PATH)
      (outputPath, row.result.get)
    })*/

    val okRows=rdd_result.filter(row => row.hasErr == false).map(row => {
      val outputPath = row.result.get.getString(LogKeys.LOG_OUTPUT_PATH)
      (outputPath, row.result.get)
    })
    println("okRows.count():"+okRows.count())
    println("okRows.first():"+okRows.first())


    //errRows.saveAsTextFile("/data_warehouse/ods_view.db/test_error_row")
    //okRows.saveAsTextFile("/data_warehouse/ods_view.db/test_ok_row")

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
  //config.setMaster("local[2]")
  val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
  var inputPath = ""
  var appId2OutputPathTemplateMapBroadCast: Broadcast[scala.collection.mutable.HashMap[String, String]] = _
  var specialRulesBroadCase:Broadcast[Array[AppLogFieldSpecialRules]]=_
}
