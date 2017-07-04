package cn.whaley.bi.logsys.log2parquet

import java.io.File
import java.util.Date

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.Constants
import cn.whaley.bi.logsys.log2parquet.traits._
import cn.whaley.bi.logsys.log2parquet.utils.{ParquetHiveUtils, Json2ParquetUtil, MetaDataUtils}
import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by michael on 2017/6/22.
  */
class MsgBatchManagerV3 extends InitialTrait with NameTrait with LogTrait with java.io.Serializable {

  var metaDataUtils :MetaDataUtils

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
    val processGroupInstance = instanceFrom(confManager, logProcessGroupName).asInstanceOf[ProcessGroupTraitV2]
    val processedRdd = pathRdd.map(e => {
      val jsonObject = e._2
      val jsonObjectProcessed = processGroupInstance.process(jsonObject)
      (Constants.DATA_WAREHOUSE + File.separator + e._1, jsonObjectProcessed)
    })
    println("processedRdd.count():" + processedRdd.count())
    LOG.info("processedRdd.count():" + processedRdd.count())
    processedRdd.take(10).foreach(println)

    //将经过处理器处理后，正常状态的记录使用规则库过滤【字段黑名单、重命名、行过滤】
    val okRowsRdd = processedRdd.filter(e=>e._2.hasErr==false)
    val afterRuleRdd=ruleHandle(pathRdd,okRowsRdd)
    println("afterRuleRdd.count():" + afterRuleRdd.count())
    LOG.info("afterRuleRdd.count():" + afterRuleRdd.count())
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

    //TODO 读parquet文件，生成元数据信息给元数据模块使用


  }

  def debug(rdd:RDD[(String, JSONObject)]): Unit ={

  }

  def generateMetaData(): Unit ={
    /*val path=
    ParquetHiveUtils.parseSQLFieldInfos("")*/


    /**1.distinct output path
      *2.read
      *
      *
      * */

  }

  /**记录使用规则库过滤【字段黑名单、重命名、行过滤】*/
  def ruleHandle(pathRdd:RDD[(String, JSONObject)],resultRdd: RDD[(String, ProcessResult[JSONObject])]):RDD[(String, JSONObject)] ={
    // 获得规则库的每条规则
    val rules = metaDataUtils.parseSpecialRules(pathRdd)

    val afterRuleRdd = resultRdd.map(e => {
      val path = e._1
      val jsonObject = e._2.result.get

      if(rules.filter(rule => rule.path.equalsIgnoreCase(path)).size>0){
        //一个绝对路径唯一对应一条规则
        val rule = rules.filter(rule => rule.path.equalsIgnoreCase(path)).head

        val fieldBlackFilter = rule.fieldBlackFilter
        fieldBlackFilter.map(blackField => {
          jsonObject.remove(blackField)
        })

        val rename = rule.rename
        rename.map(e => {
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
