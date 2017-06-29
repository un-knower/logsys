package cn.whaley.bi.logsys.log2parquet

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.regex.Pattern

import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.processingUnit.OutputPathProcessingUnits
import cn.whaley.bi.logsys.log2parquet.utils.{DateFormatUtils, MetaDataUtils, PhoenixUtils}
import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by baozhiwang on 2017/6/22.
  */
object Test {
  def main(args: Array[String]) {
    //getTableName()
  /*  val appId2TableNameMap =MetaDataUtils.getAppId2TableNameMap
    appId2TableNameMap.foreach(e=>{
      println(e._1+"->"+e._2)
    })*/
  /*  val appId2TableNameMap =MetaDataUtils.getAppId2TableNameMap
    println(appId2TableNameMap.size)
    val appId2PartitionName=MetaDataUtils.getAppId2PartitionName
    println(appId2PartitionName.size)
    appId2PartitionName.foreach(e=>{
      println(e._1+"->"+e._2)
    })*/

   /* val appId2OutputPathTemplateMap=getAppId2OutputPathTemplateMap
    appId2OutputPathTemplateMap.foreach(e=>{
      println(e._1+"->"+e._2)
    })*/

   // testOutputPath
    testSparkSession




  }


  def testSparkSession(): Unit ={
    val inputPath="/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13/boikgpokn78sb95ktmsc1bnkechpgj9l_2017061413_raw_7_575892351.json.gz"
    val config = new SparkConf()
    config.setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
    val rdd=sparkSession.sparkContext.textFile(inputPath, 2)
    println(rdd.count())
  }


def testOutputPath(): Unit ={
   var outputPathTemplate= Some("log_medusa_main3x_${log_type}_${event_id}/key_day=${key_day}/key_hour=${key_hour}").get
  val jsonObject=new JSONObject()
   jsonObject.put(LogKeys.LOG_BODY_REAL_LOG_TYPE,"play")
   jsonObject.put(LogKeys.LOG_BODY_EVENT_ID,"helios-player-sdk-startPlay")
   jsonObject.put(LogKeys.LOG_KEY_DAY,"20170628")
   jsonObject.put(LogKeys.LOG_KEY_HOUR,"05")
   if(outputPathTemplate.nonEmpty){
     if(outputPathTemplate.contains("${log_type}")&&jsonObject.containsKey(LogKeys.LOG_BODY_REAL_LOG_TYPE)){
      val realLogType=jsonObject.getString(LogKeys.LOG_BODY_REAL_LOG_TYPE)
      println("realLogType:"+realLogType)
      outputPathTemplate=outputPathTemplate.replace("${log_type}",realLogType)
    }else{
      //remove ${log_type} from tableName
      println("not in:")
      outputPathTemplate=outputPathTemplate.replace("_${log_type}","")

      println("aa_${log_type}".replace("_${log_type}",""))
    }

    if(outputPathTemplate.contains("${event_id}")&&jsonObject.containsKey(LogKeys.LOG_BODY_EVENT_ID)){
      val eventId=jsonObject.getString(LogKeys.LOG_BODY_EVENT_ID)
      outputPathTemplate=outputPathTemplate.replace("${event_id}",eventId).replace(".","_").replace("-","_")
    }else{
      //remove ${event_id} from tableName
      outputPathTemplate=outputPathTemplate.replace("_${event_id}","")
    }

     if(outputPathTemplate.contains("${key_day}")&&jsonObject.containsKey(LogKeys.LOG_KEY_DAY)){
       val key_day=jsonObject.getString(LogKeys.LOG_KEY_DAY)
       outputPathTemplate=outputPathTemplate.replace("${key_day}",key_day)
     }else{
       //remove ${event_id} from tableName
       outputPathTemplate=outputPathTemplate.replace("key_day=${key_day}","")
     }

     if(outputPathTemplate.contains("${key_hour}")&&jsonObject.containsKey(LogKeys.LOG_KEY_HOUR)){
       val key_hour=jsonObject.getString(LogKeys.LOG_KEY_HOUR)
       outputPathTemplate=outputPathTemplate.replace("${key_hour}",key_hour)
     }else{
       //remove ${event_id} from tableName
       outputPathTemplate=outputPathTemplate.replace("key_hour=${key_hour}","")
     }


    jsonObject.put(LogKeys.LOG_OUTPUT_PATH,outputPathTemplate)
  }
  println("----"+jsonObject.get(LogKeys.LOG_OUTPUT_PATH))

}


  def getAppId2OutputPathTemplateMap: scala.collection.mutable.HashMap[String,String] ={
    val appId2OutputPathTemplateMap = scala.collection.mutable.HashMap.empty[String,String]
    val appId2TableNameMap =MetaDataUtils.getAppId2TableNameMap
    val appId2PartitionName=MetaDataUtils.getAppId2PartitionName

    val list=getAllAppId
    list.foreach(appId=>{
      val tableName=appId2TableNameMap(appId)
      val partitionName=appId2PartitionName(appId)
      val outputPathTemplate=tableName+File.separator+partitionName
      appId2OutputPathTemplateMap.put(appId,outputPathTemplate)
    })
    appId2OutputPathTemplateMap
  }


  def getAppId2PartitionName: scala.collection.mutable.HashMap[String,String] = {
    val appId2PartitonNameMap = scala.collection.mutable.HashMap.empty[String,String]
    val list=getAllAppId
    list.foreach(appId=>{
      val partitionName=getPartitionNameByConfig(appId)
      appId2PartitonNameMap.put(appId,partitionName)
    })
    appId2PartitonNameMap
  }

  def getPartitionNameByConfig(appId:String): String ={
    val fieldName="FIELDNAME"
    val fieldDefault="FIELDDEFAULT"

    val globalMap = scala.collection.mutable.HashMap.empty[String,String]
    val globalList = scala.collection.mutable.ListBuffer.empty[String]
    val globalSet = scala.collection.mutable.Set.empty[String]

    val sqlGlobalConfig=
      s"""select * from metadata.applog_key_field_desc
          |where
          |      ISDELETED=false   and
          |      FIELDFLAG=2       and
          |      (APPID='ALL' or APPID='$appId')
          |order by FIELDORDER""".stripMargin
    val stat = PhoenixUtils.getStatement
    val result=stat.executeQuery(sqlGlobalConfig)
    while(result.next()){
      val FIELDNAME_VAR=result.getString(fieldName)
      val FIELDDEFAULT_VAR=result.getString(fieldDefault)
      if(!globalSet.contains(FIELDNAME_VAR)){
        globalSet.add(FIELDNAME_VAR)
        globalList.+=(FIELDNAME_VAR)
      }
      globalMap.put(FIELDNAME_VAR,FIELDDEFAULT_VAR)
    }

    val appIdTableName=StringBuilder.newBuilder
    globalList.foreach(e=>{
      val eValue=globalMap.get(e).get
      if(null==eValue){
        appIdTableName.append(e+"=${"+e+"}")
      }else{
        appIdTableName.append(eValue+"="+eValue)
      }
      appIdTableName.append(File.separator)
    })
    var tableName=appIdTableName.toString()
    if(!tableName.isEmpty&&tableName.endsWith(File.separator)){
      tableName=tableName.substring(0,tableName.length-1)
    }
    tableName
  }



  def getAppId2TableNameMap: scala.collection.mutable.HashMap[String,String] ={
    val appId2TableNameMap = scala.collection.mutable.HashMap.empty[String,String]
    val list=getAllAppId
    list.foreach(appId=>{
      val tableName=getTableNameByConfig(appId)
      appId2TableNameMap.put(appId,tableName)
    })

    appId2TableNameMap.foreach(e=>{
      println(e._1+"->"+e._2)
    })

    appId2TableNameMap
  }


  def getAllAppId():scala.collection.mutable.ListBuffer[String]={
    val sqlGlobalConfig= s"""select distinct APPID
        |from metadata.applog_key_field_desc
        |where ISDELETED=false""".stripMargin
    val stat = PhoenixUtils.getStatement
    val result=stat.executeQuery(sqlGlobalConfig)
    val list=scala.collection.mutable.ListBuffer.empty[String]
    while(result.next()){
      val appId=result.getString("APPID")
      list.append(appId)
    }
    list
  }

  def getTableNameByConfig(appId:String): String ={
    val fieldName="FIELDNAME"
    val fieldDefault="FIELDDEFAULT"
    val table_split_string="_"

    val globalMap = scala.collection.mutable.HashMap.empty[String,String]
    val globalList = scala.collection.mutable.ListBuffer.empty[String]
    val globalSet = scala.collection.mutable.Set.empty[String]

    val sqlGlobalConfig=
      s"""select * from metadata.applog_key_field_desc
          |where
          |      ISDELETED=false   and
          |      FIELDFLAG=1       and
          |      (APPID='ALL' or APPID='$appId')
          |order by FIELDORDER""".stripMargin
    val stat = PhoenixUtils.getStatement
    val result=stat.executeQuery(sqlGlobalConfig)
    while(result.next()){
      val FIELDNAME_VAR=result.getString(fieldName)
      val FIELDDEFAULT_VAR=result.getString(fieldDefault)
      if(!globalSet.contains(FIELDNAME_VAR)){
        globalSet.add(FIELDNAME_VAR)
        globalList.+=(FIELDNAME_VAR)
      }
      globalMap.put(FIELDNAME_VAR,FIELDDEFAULT_VAR)
     }

    val appIdTableName=StringBuilder.newBuilder
    globalList.foreach(e=>{
      val eValue=globalMap.get(e).get
      if(null==eValue){
        appIdTableName.append("${"+e+"}")
      }else{
        appIdTableName.append(eValue)
      }
      appIdTableName.append(table_split_string)
    })
    var tableName=appIdTableName.toString()
    if(!tableName.isEmpty&&tableName.endsWith(table_split_string)){
      tableName=tableName.substring(0,tableName.length-1)
    }
    tableName
  }

  def test1() {
    val regex = "appIdForProcessGroup"
    val p = Pattern.compile(regex)
    val m = p.matcher("appIdForProcessGroup.boikgpokn78sb95ktmsc1bnkechpgj9l")
    println(m.find())
  }



  /**
    *
    * a. select * from metadata.applog_key_field_desc where APPID='ALL' and ISDELETED=false and FIELDFLAG=2 order by FIELDORDER;
    * b. select * from metadata.applog_key_field_desc where APPID='boikgpokn78sb95ktmsc1bnkechpgj9l' and ISDELETED=false and FIELDFLAG=2 order by FIELDORDER;

    *
    * */
  def getTableName(): Unit = {
    val tab_prefix="tab_prefix"
    val product_code="product_code"
    val app_code="app_code"
    val log_type="log_type"
    val event_id="event_id"


    val APPID="APPID"
    val FIELDNAME="FIELDNAME"
    val FIELDDEFAULT="FIELDDEFAULT"
    val FIELDORDER="FIELDORDER"

    val table_split_string="_"

    /**
      * 数据结构
      *
      * 1.将全局配置放入List[Map[key,value]]中
      * 2.将app配置放入List[Map[key,value]]中
      * 3.merge这两个List,得到表名称
      *
      *
      * 最终期望的数据结构：
      * 每个appId对应的表名称Map【如果默认值为null，暂时${FIELDNAME}填充】
      * Map<appId,表名称>
      * */

    val globalMap = scala.collection.mutable.HashMap.empty[String,String]
    val globalList = scala.collection.mutable.ListBuffer.empty[String]
    val globalSet = scala.collection.mutable.Set.empty[String]
    val medsua3xAppId="boikgpokn78sb95ktmsc1bnkechpgj9l"
    // val globalMap =   new util.TreeMap[String, String]

    val sqlGlobalConfig=
      s"""select * from metadata.applog_key_field_desc
          |where
          |      ISDELETED=false   and
          |      FIELDFLAG=1       and
          |      (APPID='ALL' or APPID='$medsua3xAppId')
          |order by FIELDORDER""".stripMargin
    val stat = PhoenixUtils.getStatement
    val result=stat.executeQuery(sqlGlobalConfig)

    while(result.next()){
      val FIELDNAME_VAR=result.getString(FIELDNAME)
      val FIELDDEFAULT_VAR=result.getString(FIELDDEFAULT)
      if(!globalSet.contains(FIELDNAME_VAR)){
        globalSet.add(FIELDNAME_VAR)
        globalList.+=(FIELDNAME_VAR)
      }
      globalMap.put(FIELDNAME_VAR,FIELDDEFAULT_VAR)
      println("-------"+FIELDNAME_VAR+"->"+FIELDDEFAULT_VAR)
      /* val tab_prefix_var=result.getString(tab_prefix)
       val product_code_var=result.getString(product_code)
       val app_code_var=result.getString(app_code)
       val log_type_var=result.getString(log_type)
       val event_id_var=result.getString(event_id)
       println(tab_prefix_var+","+product_code_var+","+app_code_var+","+log_type_var+","+event_id_var)*/
    }

    println("globalMap:")
    /*val it=globalMap.entrySet().iterator()
    while(it.hasNext){
      val e=it.next()
      println(e.getKey+","+e.getValue)
    }
*/


    globalMap.foreach(e=>{
      println(e._1+","+e._2)
    })


    val appIdTableName=StringBuilder.newBuilder
    //val tableFragmentNameWithOrder=scala.collection.mutable.ArrayBuffer.empty[String]
    globalList.foreach(e=>{
      val eValue=globalMap.get(e).get
      if(null==eValue){
        appIdTableName.append("${"+e+"}")
      }else{
        appIdTableName.append(eValue)
      }
      appIdTableName.append(table_split_string)
    })
    println("------------")


    var tableName=appIdTableName.toString()
    if(!tableName.isEmpty&&tableName.endsWith(table_split_string)){
      tableName=tableName.substring(0,tableName.length-1)
    }
    println(tableName)
  }
}



