package cn.whaley.bi.logsys.log2parquet.utils

import java.io.File

/**
  * Created by baozhiwang on 2017/6/27.
  */
object MetaDataUtils {

  /**
    * 用来获得appId->输出路径的Map,例如：
    * boikgpokn78sb95ktmsc1bnkechpgj9l->log_medusa_main3x_${log_type}_${event_id}/key_day=${key_day}/key_hour=${key_hour}
    */
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

  /**
    * 用来获得appId->表名称的Map,例如：
    * boikgpokn78sb95ktmsc1bnkechpgj9l->log_medusa_main3x_${log_type}_${event_id}
    */
  def getAppId2TableNameMap: scala.collection.mutable.HashMap[String,String] ={
    val appId2TableNameMap = scala.collection.mutable.HashMap.empty[String,String]
    val list=getAllAppId
    list.foreach(appId=>{
      val tableName=getTableNameByConfig(appId)
      appId2TableNameMap.put(appId,tableName)
    })
    appId2TableNameMap
  }

  /**
    * 用来获得appId->分区名称的Map,例如：
    * boikgpokn78sb95ktmsc1bnkechpgj9l->key_day=${key_day}/key_hour=${key_hour}
    */
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

}
