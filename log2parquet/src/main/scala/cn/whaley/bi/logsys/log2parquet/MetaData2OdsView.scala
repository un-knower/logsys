package cn.whaley.bi.logsys.log2parquet

import java.util
import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.utils.MetaDataUtils
import cn.whaley.bi.logsys.metadata.entity.{AppLogKeyFieldDescEntity}
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scalaj.http.{Http, HttpOptions}

/**
  * Created by guohao on 2017/9/28.
  */
object MetaData2OdsView extends LogTrait{

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val key_day = args(1)
    val key_hour =  args(2)
    val taskFlag =  args(3)
    val configFile = args(4)
    //获取配置信息
    val confManager = new ConfManager(Array(configFile))
    val metadataServer = confManager.getConf("metadataService")
    val readTimeOut = confManager.getConfOrElseValue("metadata", "readTimeout", "300000").toInt


    val config = new Configuration()
    val fs = FileSystem.get(config)
    val fileStatus = fs.listStatus(new Path(s"$inputPath"))
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getName)
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val arrayBuffer = new ArrayBuffer[(String,scala.collection.mutable.Map[String,String])]()
    val appIdInfoMap = getMapInfo(metadataServer,readTimeOut)
    fileStatus.foreach(fs=>{
      val path = fs.getPath.toString
      val schema = path.split("/")(5)
      if(schema.startsWith("log_")){
        val fullPath = s"$inputPath/${schema}/key_day=${key_day}/key_hour=${key_hour}"
        val fs = FileSystem.get(config)
        if(fs.exists(new Path(fullPath))){
          val splits = schema.split("_")
          val tab_prefix = splits(0)
          val product_code = splits(1)
          var app_code =  splits(2)
          //修复全局菜单2.0  global_menu_2
          if("global".equals(app_code)){
            app_code="global_menu_2"
          }
          val realLogType = schema.replaceAll(s"${tab_prefix}_${product_code}_${app_code}_","")
          val db_name = "ods_view"
          val map = mutable.Map[String, String]()
          map.put("db_name",db_name)
          map.put("tab_prefix",tab_prefix)
          map.put("app_code",app_code)
          map.put("product_code",product_code)
          map.put("realLogType",realLogType)
          map.put("key_hour",key_hour)
          map.put("key_day",key_day)
          val appId = getAppId(app_code,product_code,appIdInfoMap)
          map.put("appId",appId)
          val path = s"ods_view.db/$schema/key_day=$key_day/key_hour=$key_hour"
          arrayBuffer.+=((path,map))
        }

      }
    })
    val msgBatchManagerV3 = new MsgBatchManagerV3
    val metaDataUtils = new MetaDataUtils(metadataServer, readTimeOut)
    msgBatchManagerV3.initMetaData(metaDataUtils)
    msgBatchManagerV3.generateMetaDataToTable(sparkSession,arrayBuffer.toArray,taskFlag)
  }

  /**
    * 获取appId
    * @param appCode
    * @param productCode
    * @return
    */
  def getAppId(appCode:String,productCode:String,map:mutable.HashMap[String,String]):String={
    val key = s"${appCode}_${productCode}"
    val appId = map.getOrElseUpdate(key,"")
    appId
  }

  /**
    * map
    * key :appcode_productcode
    * value: appId
    * @return
    */
  def getMapInfo(metadataServer:String,readTimeOut:Int): mutable.HashMap[String,String] ={
    val itemsIterator = getAllAppLogKeyFieldDesc(metadataServer,readTimeOut)
    val array = ArrayBuffer[AppLogKeyFieldDescEntity]()
    while(itemsIterator.hasNext){
      val item = itemsIterator.next()
      array.+=(item)
    }
    val items = array.filter(item=>{
      item.getFieldFlag == 1 && item.isDeleted == false && (item.getFieldName == "app_code" || item.getFieldName == "product_code")
    })
      .map(item=>(item.getAppId, item.getFieldName, item.getFieldDefault))
    val appcode_productcode_appId = mutable.HashMap[String,String]()
    items.map(row => row._1).distinct.map(appId => {
      val appcodes = items.filter(row=> row._1 == appId  && row._2 =="app_code" &&  row._3 != null ).map(row=>row._3).distinct
      val productcodes = items.filter(row=> row._1 == appId  && row._2 =="product_code" ).map(row=>row._3).distinct
      appcodes.foreach(appcode=>{
        productcodes.foreach(productcode=>{
          val key = s"${appcode}_${productcode}"
          val value = appId
          appcode_productcode_appId +=(key->value)
        })
      })
    })
    appcode_productcode_appId
  }

  /**
    * 查询所有applog_key_field_desc记录
    */
  def getAllAppLogKeyFieldDesc(metadataServer:String,readTimeOut:Int) : util.Iterator[AppLogKeyFieldDescEntity] = {
    val response = Http(metadataServer + "/metadata/applog_key_field_desc/all")
      .option(HttpOptions.readTimeout(readTimeOut))
      .method("GET").asString
    if (!response.isSuccess) {
      throw new RuntimeException(response.body)
    }
    JSON.parseArray(response.body,classOf[AppLogKeyFieldDescEntity]).iterator()
  }


}
