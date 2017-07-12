package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.entity.LogFromEntity
import cn.whaley.bi.logsys.log2parquet.traits._
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by michael on 2017/6/22.
  * Not used
  */
class MsgBatchManagerV2/* extends InitialTrait with NameTrait with LogTrait*/ {

  /*private val config = new SparkConf()
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
    val rdd_original = sparkSession.sparkContext.textFile(inputPath, 200)
    //按logType和eventId分类
    rdd_original.foreachPartition(
      partition => {
        val confManager = new ConfManager(Array("MsgBatchManagerV2.xml", "settings.properties"))
        val logProcessGroupName = confManager.getConf(this.name, "LogProcessGroup")
        val processGroupInstance=instanceFrom(confManager, logProcessGroupName).asInstanceOf[ProcessGroupTraitV2]

        partition.foreach(line => {
          val jsonObject = string2JsonObject(line)
          if (null != jsonObject) {
            processGroupInstance.process(jsonObject)
          }
        })
      })

    /**输出路径
      * 通过appid读取[metadata.applog_key_field_desc]表，通过【表字段，分区字段（排序）】获得输出路径的非hive表非分区字段，
      * 通过logTime获得key_day和key_hour获得hive表分区字段。
      */
    //val outputPath=PathUtil.getOdsViewPath(appID, startDate, startHour, "logType", "eventID")
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
  }*/
}
