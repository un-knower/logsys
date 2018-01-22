import java.io.File
import java.util.Date
import java.util.regex.Pattern

import cn.whaley.bi.logsys.log2parquet.MainObj
import cn.whaley.bi.logsys.log2parquet.constant.Constants
import cn.whaley.bi.logsys.log2parquet.processingUnit.JsonFormatProcessingUnits
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.utils.{MetaDataUtils, ParquetHiveUtils, SendMail}
import cn.whaley.bi.logsys.metadata.entity.LogFileFieldDescEntity
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by baozhiwang on 2017/7/4.
  */
class MainObjTest extends LogTrait{
  @Test
  def testMainObjTest: Unit = {
    val args = Array("MsgProcExecutor",
//    "--f","MsgBatchManagerV3.xml,settings.properties","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/fileType.json","--c","masterURL=local[1]","--c","startDate=20171117", "--c" ,"startHour=09" ,"--c" ,"taskFlag=111","--c" ,"realLogType=all","--c" ,"appId=all")
//    "--f","MsgBatchManagerV3.xml,settings.properties","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/test.json","--c","masterURL=local[1]","--c","startDate=20171117", "--c" ,"startHour=09" ,"--c" ,"taskFlag=111","--c" ,"tableName=all","--c" ,"appId=all")
    "--f","MsgBatchManagerV3.xml,settings.properties","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/launcher_module_view.json","--c","masterURL=local[1]","--c","startDate=20171117", "--c" ,"startHour=09" ,"--c" ,"taskFlag=111","--c" ,"tableName=all","--c" ,"appId=all")

    MainObj.main(args)
  }

  @Test
  def parseSQLFieldInfos(): Unit ={
    val path=new Path("/data_warehouse/ods_view.db/log_medusa_main3x_event_medusa_player_sdk_startPlay/key_day=19700101/key_hour=08/part-00000-7a49cc93-5035-4aee-b440-efaadc510fa9.snappy.parquet")
    //val path=new Path("/data_warehouse/ods_view.db/log_medusa_main3x_event_medusa_player_sdk_startPlay/key_day=19700101/key_hour=08")
    val result=ParquetHiveUtils.parseSQLFieldInfos(path)
    result.map(e=>println(e))
  }

  @Test
  def getParquetFilesFromHDFS(): Unit ={
    //val path=new Path("/data_warehouse/ods_view.db/log_medusa_main3x_event_medusa_player_sdk_startPlay/key_day=19700101/key_hour=08/part-00000-7a49cc93-5035-4aee-b440-efaadc510fa9.snappy.parquet")
    val path="/data_warehouse/ods_view.db/log_medusa_main3x_event_medusa_player_sdk_startPlay/key_day=19700101/key_hour=08"
    //val path=new Path("/data_warehouse/ods_view.db/log_medusa_main3x_event_medusa_player_sdk_startPlay/key_day=19700101/key_hour=08")
    val result=ParquetHiveUtils.getParquetFilesFromHDFS(path)
    result.map(e=>println(e.getPath))
  }


  @Test
  def getMetaInfo(): Unit ={
    val path="/data_warehouse/ods_view.db/log_medusa_main3x_matchdetail/key_day=19700101/key_hour=08"
    val list=ParquetHiveUtils.getParquetFilesFromHDFS(path)
    if(list.size>0) {
      val parquetMetaData = ParquetHiveUtils.parseSQLFieldInfos(list.head.getPath)
      parquetMetaData.map(e=>println(e))
    }
  }


  def getUtils()=new MetaDataUtils( "http://bigdata-appsvr-130-5:8084",1000000)

  @Test
  def parseSQLFieldInfosAndPutRequest(): Unit ={
    val arrayBuffer = new ArrayBuffer[LogFileFieldDescEntity]()
    val taskId="test123456"
    val path=new Path("/data_warehouse/ods_view.db/log_medusa_main3x_event_medusa_player_sdk_startPlay/key_day=19700101/key_hour=08/part-00000-7a49cc93-5035-4aee-b440-efaadc510fa9.snappy.parquet")
    val parquetMetaData = ParquetHiveUtils.parseSQLFieldInfos(path)
    parquetMetaData.map(meta=>{
      val fieldName=meta._1
      val fieldType=meta._2
      val fieldSql=meta._3
      val rowType=meta._4
      val rowInfo=meta._5
      val logFileFieldDescEntity=new LogFileFieldDescEntity
      logFileFieldDescEntity.setTaskId(taskId)
      logFileFieldDescEntity.setLogPath(Constants.DATA_WAREHOUSE+File.separator)
      logFileFieldDescEntity.setFieldName(fieldName)
      logFileFieldDescEntity.setFieldType(fieldType)
      logFileFieldDescEntity.setFieldSql(fieldSql)
      logFileFieldDescEntity.setRawType(rowType)
      logFileFieldDescEntity.setRawInfo(rowInfo)
      logFileFieldDescEntity.setIsDeleted(false)
      logFileFieldDescEntity.setCreateTime(new Date())
      logFileFieldDescEntity.setUpdateTime(new Date())
      println(fieldName+","+fieldType+","+fieldSql+","+rowType+","+rowInfo)
      arrayBuffer+=logFileFieldDescEntity
    })

    println(arrayBuffer.length)
    val response= getUtils.metadataService().putLogFileFieldDesc(taskId,arrayBuffer)
    println(response)
  }

  @Test
  def postTaskId2MetaModel(): Unit ={
    //http://bigdata-appsvr-130-5:8084/metadata/processTask/AAABXS9LtE8K4Ax2FoAAAAA/111
    //AAABXtH43PwK4IF8CMAAAAA
    val taskId="AAABXtFGTAIK4AasIkAAAAA"
    val taskFlag="111"
    val response=getUtils.metadataService().postTaskId2MetaModel(taskId,taskFlag)
   println(response)
  }

  @Test
  def testGrammar(): Unit ={
    val a="111155031049"
    a.toInt
  }

  @Test
  def fieldDescEntityTest(): Unit ={
    val arrayBuffer = new ArrayBuffer[LogFileFieldDescEntity]()
    val taskId="test123456"
    val path=new Path("/data_warehouse/ods_view.db/log_medusa_main3x_event_medusa_player_sdk_startPlay/key_day=19700101/key_hour=08/part-00000-7a49cc93-5035-4aee-b440-efaadc510fa9.snappy.parquet")
    val parquetMetaData = ParquetHiveUtils.parseSQLFieldInfos(path)
    parquetMetaData.map(meta=>{
      val fieldName=meta._1
      val fieldType=meta._2
      val fieldSql=meta._3
      val rowType=meta._4
      val rowInfo=meta._5
      val logFileFieldDescEntity=new LogFileFieldDescEntity
      logFileFieldDescEntity.setTaskId(taskId)
      logFileFieldDescEntity.setLogPath(Constants.DATA_WAREHOUSE+File.separator)
      logFileFieldDescEntity.setFieldName(fieldName)
      logFileFieldDescEntity.setFieldType(fieldType)
      logFileFieldDescEntity.setFieldSql(fieldSql)
      logFileFieldDescEntity.setRawType(rowType)
      logFileFieldDescEntity.setRawInfo(rowInfo)
      logFileFieldDescEntity.setIsDeleted(false)
      logFileFieldDescEntity.setCreateTime(new Date())
      logFileFieldDescEntity.setUpdateTime(new Date())
      println(fieldName+","+fieldType+","+fieldSql+","+rowType+","+rowInfo)
      arrayBuffer+=logFileFieldDescEntity
    })

    println(arrayBuffer.length)
    val response= getUtils.metadataService().putLogFileFieldDesc(taskId,arrayBuffer)
    println(response)
  }

@Test
  def test2(): Unit ={
  //val dir="/data_warehouse/ods_view.db/log_medusa_main3x_event_medusa_notification_action/key_day=20170710/key_hour=12"
  val dir="ods_view.db/log_medusa_main3x_pageview_medusa_home_button/key_day=20170710/key_hour=12"
  val list = ParquetHiveUtils.getParquetFilesFromHDFS(Constants.DATA_WAREHOUSE + File.separator + dir)
  println(list.length)
  println(list)
}

  @Test
  def test3: Unit = {
    val line="{\"msgSignFlag\":0,\"msgId\":\"AAABXWtHeOUKEwn4bHbqvQAB\",\"msgFormat\":\"json\",\"_sync\":{\"rawTopic\":\"log-raw-boikgpokn78sb95ktmsc1bnk\",\"rawTs\":1500743432505,\"odsTs\":1500743439432,\"rawOffset\":1392288712,\"rawParId\":2},\"logTime\":1500743234559,\"logBody\":{\"logType\":\"start_end\",\"productModel\":\"MiBOX_mini\",\"svr_host\":\"log.moretv.com.cn\",\"weatherCode\":\"101011000\",\"svr_remote_addr\":\"61.49.106.162\",\"groupId\":\"178\",\"buildDate\":\"20160816\",\"uuid\":\"0ba9fbe0-7e6c-49d6-acc8-ed26c841ef10\",\"apkVersion\":\"3.0.9\",\"promotionChannel\":\"shafa\",\"pageType\":\"运营图\",\"videoName\":\"Heartbreak Hotel & I Just Wanna Dance - 柳熙烈的写生簿 现场版 16/06/25\",\"svr_req_url\":\"/medusalog/\",\"contentType\":\"mv\",\"happenTime\":1500743234559,\"episodeSid\":\"fh233fxy3ftv\",\"apkSeries\":\"MoreTV_TVApp3.0_Medusa\",\"eventId\":\"medusa-startPageView-startpage\",\"uploadTime\":\"20170723011032\",\"version\":\"01\",\"userId\":\"721d70292dade8726323a01a95f820b0\",\"versionCode\":\"309\",\"accountId\":\"\",\"svr_content_type\":\"application/json\",\"svr_forwarded_for\":\"-\",\"videoSid\":\"fh233fxy3ftv\",\"appEnterWay\":\"native\",\"svr_receive_time\":1500743432421,\"svr_fb_Time\":\"2017-07-22T17:10:32.505Z\",\"actionId\":\"medusa-play-play-pause\",\"svr_req_method\":\"POST\",\"operation\":\"play\",\"md5\":\"35bd7971d8684c69c1e757a2a601faab\",\"ts\":1500743432307,\"status\":\"end\"},\"logSignFlag\":1,\"appId\":\"boikgpokn78sb95ktmsc1bnkechpgj9l\",\"logId\":\"AAABXWtHeOUKEwn4bHbqvQAB0004\",\"logVersion\":\"02\",\"msgSource\":\"ngx_log\",\"msgVersion\":\"1.0\",\"msgSite\":\"10.19.9.248\"}"
    val jsonObject=JSON.parseObject(line)
    println(jsonObject)
    val util=new JsonFormatProcessingUnits()
    val after = util.process(jsonObject)
    println(after.result.get.getString("videoName"))
  }

  @Test
  def test4: Unit = {

    val inputPath = "/data_warehouse/ods_origin.db/log_origin/key_day=20170925/key_hour=00"
    println(inputPath.split("/key_hour")(0))

  }


  @Test
  def testJsonObject: Unit ={
   val line="{\n    \"_sync\":{\n            \"rawTopic\":\"log-raw-boikgpokn78sb95ktmsc1bnk\",\n            \"rawTs\":1497416396072,\n            \"odsTs\":1497416402715,\n            \"rawOffset\":575016728,\n            \"rawParId\":9\n        },\n    \"appId\":\"boikgpokn78sb95ktmsc1bnkechpgj9l\"\n}"
   val jsonObject = JSON.parseObject(line)
   val logBody = jsonObject.getJSONObject("_sync")

    jsonObject.asInstanceOf[java.util.Map[String, Object]].putAll(logBody.asInstanceOf[java.util.Map[String, Object]])
    jsonObject.remove("_sync")
    println(jsonObject)
  }


  @Test
  def test5(): Unit ={
//    val input = "/data_warehouse/ods_origin.db/log_origin/key_day=20171121/key_hour=20/part-00849"
    println(5/4)
    val input ="/data_warehouse/ods_origin.db/log_origin/test.json"
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    val sparkSession = SparkSession.builder().appName("test").config(conf).getOrCreate()
    val df = sparkSession.read.json(input)
    df.schema.foreach(f=>{
      println(s"name -> ${f.name} dataType -> ${f.dataType} typeName -> ${f.dataType.typeName} simpleString -> ${f.dataType.simpleString}")
    })

    println("--------------")

    val list = df.schema.map(f=>{
      f.name
    }).toList
//    getDfSchema(list,"log_whaleytv_main_play111","whaleytv")

  }
  def getDfSchema(list:List[String],tableName:String,productCode:String): List[String] ={
    //先rename 在filter
    println(s"tableName $tableName")
    println(s"productCode $productCode")
    println(s"fields 初始值 $list")
    var fields = list
    val metaDataUtils = new MetaDataUtils("http://odsviewmd.whaleybigdata.com", 100000)
    val entities = metaDataUtils.queryAppLogSpecialFieldDescConf()
    //该产品下或者全日志或者表级别
    val appLogSpecialFieldEntities = entities.filter(entity=>entity._1 == productCode || entity._1 == "log" || entity._1 == tableName )
    //字段重命名: Seq[(源字段名,字段目标名)]
    //1.baseinfo 白名单 重命名
    val baseInfoFieldMap = mutable.Map[String,String]()
    val baseInfoFields = metaDataUtils.metadataService().getAllLogBaseInfo().filter(item=>{
      item.isDeleted == false && item.getProductCode == productCode
    }).map(item => item.getFieldName)
    baseInfoFields.foreach(baseInfoField=>{
      fields.foreach(field=>{
        //字段只有忽略忽略大小写在baseInfo中，需要重命名
        if(!baseInfoField.equals(field) && baseInfoField.equalsIgnoreCase(field)){
          baseInfoFieldMap.put(field,field+"_r")
        }
      })
    })
    fields = fields.filter(field => !baseInfoFieldMap.keySet.contains(field))
    //2.黑名单重命名
    //2.1表级别
    val tabRenameMap= handleRename(appLogSpecialFieldEntities,fields,tableName)
    fields = fields.filter(field => !tabRenameMap.keySet.contains(field))
    println(s"tabRenameMap ${tabRenameMap}")
    //2.2产品线级别
    val productRenameMap= handleRename(appLogSpecialFieldEntities,fields,productCode)
    fields = fields.filter(field => !productRenameMap.keySet.contains(field))
    println(s"productRenameMap ${productRenameMap}")
    //2.3全局级别
    val logRenameMap= handleRename(appLogSpecialFieldEntities,fields,"log")
    fields = fields.filter(field => !productRenameMap.keySet.contains(field))
    println(s"logRenameMap ${logRenameMap}")
    val whiteRenameMap = baseInfoFieldMap.toMap ++ tabRenameMap ++ productRenameMap ++ logRenameMap
    println(s"whiteRenameMap ${whiteRenameMap}")
    //字段过滤器: Seq[源字段名]
    //字段删除黑名单
    val fieldFilterList = appLogSpecialFieldEntities.filter(conf => conf._3 == "fieldFilter").flatMap(conf => {
      val specialValue = conf._4
      val fieldPattern = if (specialValue.charAt(0) == '1') {
        Pattern.compile(conf._2, Pattern.CASE_INSENSITIVE)
      }
      else {
        Pattern.compile(conf._2)
      }
      val isReserve = specialValue.charAt(1) == '0'
      fields.filter(field => fieldPattern.matcher(field).find()).map(field => (field, isReserve))
    })
    //剔除白名单字段
    val whiteList = fieldFilterList.filter(_._2)
    val fieldBlackFilter = fieldFilterList.filter(item => {
      val field = item._1
      !whiteList.exists(p => p._1 == field)
    }).map(item => item._1)
    if(fieldBlackFilter.size> 0){
      println("字段删除 :"+fieldBlackFilter)
    }
    fields = fields.filter(field=> !fieldBlackFilter.contains(field))
    //        println("fields...为重命名之前......"+fields)
    //处理fields中相同的字段
    //to do ...

    val set = fields.toStream.map(field=>field.toLowerCase).toSet
    //字段列表中含有字段模糊，需要重命名
    val renameMap = mutable.Map[String,String]()
    if(fields.size != set.size){
      val dealRenameList =  fields
      var i = 1
      dealRenameList.foreach(dealField=>{
        fields = fields.filter(field=>{
          val flag = !field.equals(dealField) && field.equalsIgnoreCase(dealField)
          if(flag && !whiteRenameMap.values.toSet.contains(field)){
            renameMap.put(field,s"${field}_${i}")
            i=i+1
            false
          }else{
            true
          }
        })
      })
    }
    if(renameMap.size !=0 ){
      val e = new RuntimeException(renameMap.toString())
      SendMail.post(e, s"[Log2Parquet new][Json2ParquetUtil][] field rename", Array("app-bigdata@whaley.cn"))
    }
    println(s"renameMap ${renameMap.toMap}")
    println(s"whiteRenameMap ${whiteRenameMap.values.toSet}")

    val allRenameMap = whiteRenameMap ++ renameMap.toMap
    //把重命名字段做到field list中
    allRenameMap.foreach(f=>{
      val oldName = f._1
      val newName = f._2
      //newName如存在原始的list中需要特殊处理
      if(!fields.contains(newName)){
        fields = fields.::(s"`$oldName` as `$newName`")
      }else{
        println(s"fields $fields")
        fields = fields.filter(field=>field !=newName)
        fields = fields.::(s"(case when `$newName` is null or `$newName` == '' then `$oldName` else `$newName` end ) as `$newName`")
      }
    })
    println("fields 最终直"+fields)
    fields
  }

  /**
    * 处理重命名
    * @param appLogSpecialFieldEntities
    * @param fields
    * @param logPathReg
    * @return renameMap 处理重命名，leftFields剩下的字段
    */
  def handleRename(appLogSpecialFieldEntities:Seq[(String, String, String, String, Int)],fields:List[String],logPathReg:String): Map[String,String] ={
    val renameMap = appLogSpecialFieldEntities.filter(conf => conf._3 == "rename" && conf._1 == logPathReg).flatMap(conf => {
      fields.filter(field => conf._2.r.findFirstMatchIn(field).isDefined).map(field => (field, conf._4))
    }).toMap
    renameMap
  }

  @Test
  def test1111(): Unit ={
    //先rename 在filter
    var fields = List("useid","retcode","appid","vipLevel","productSn","Abc","abc","ABC","ddd","_msg","Ddd")
    val productCode = "whaleytv"
    val tableName = "log_whaleytv_main_play111"
    val metaDataUtils = new MetaDataUtils("http://odsviewmd.whaleybigdata.com", 100000)
    val entities = metaDataUtils.queryAppLogSpecialFieldDescConf()
    //该产品下或者全日志或者表级别
    val appLogSpecialFieldEntities = entities.filter(entity=>entity._1 == productCode || entity._1 == "log" || entity._1 == tableName )

    //字段重命名: Seq[(源字段名,字段目标名)]
    //1.baseinfo 白名单 重命名
    val baseInfoFieldMap = mutable.Map[String,String]()
    val baseInfoFields = metaDataUtils.metadataService().getAllLogBaseInfo().filter(item=>{
      item.isDeleted == false && item.getProductCode == productCode
    }).map(item => item.getFieldName)
    baseInfoFields.foreach(baseInfoField=>{
      fields.foreach(field=>{
        //字段只有忽略忽略大小写在baseInfo中，需要重命名
        if(!baseInfoField.equals(field) && baseInfoField.equalsIgnoreCase(field)){
          baseInfoFieldMap.put(field,field+"_r")
        }
      })
    })

    fields = fields.filter(field => !baseInfoFieldMap.keySet.contains(field))
    //2.黑名单重命名
    //2.1表级别
    val tabRenameMap= handleRename(appLogSpecialFieldEntities,fields,tableName)
    fields = fields.filter(field => !tabRenameMap.keySet.contains(field))
    println(s"tabRenameMap ${tabRenameMap}")
    //2.2产品线级别
    val productRenameMap= handleRename(appLogSpecialFieldEntities,fields,productCode)
    fields = fields.filter(field => !productRenameMap.keySet.contains(field))
    println(s"productRenameMap ${productRenameMap}")
    //2.3全局级别
    val logRenameMap= handleRename(appLogSpecialFieldEntities,fields,"log")
    fields = fields.filter(field => !productRenameMap.keySet.contains(field))
    println(s"logRenameMap ${logRenameMap}")

    val whiteRenameMap = baseInfoFieldMap.toMap ++ tabRenameMap ++ productRenameMap ++ logRenameMap


    //字段过滤器: Seq[源字段名]
    //字段删除黑名单
    val fieldFilterList = appLogSpecialFieldEntities.filter(conf => conf._3 == "fieldFilter").flatMap(conf => {
      val specialValue = conf._4
      val fieldPattern = if (specialValue.charAt(0) == '1') {
        Pattern.compile(conf._2, Pattern.CASE_INSENSITIVE)
      }
      else {
        Pattern.compile(conf._2)
      }
      val isReserve = specialValue.charAt(1) == '0'
      fields.filter(field => fieldPattern.matcher(field).find()).map(field => (field, isReserve))
    })

    println(s"fieldFilterList $fieldFilterList" )

    //剔除白名单字段
    val whiteList = fieldFilterList.filter(_._2)
    val fieldBlackFilter = fieldFilterList.filter(item => {
      val field = item._1
      !whiteList.exists(p => p._1 == field)
    }).map(item => item._1)

    println("字段删除 :"+fieldBlackFilter)
    fields = fields.filter(field=> !fieldBlackFilter.contains(field))
    println("fields........."+fields)
    //处理fields中相同的字段
    //to do ...
    val set = fields.toStream.map(field=>field.toLowerCase).toSet
    //字段列表中含有字段模糊，需要重命名
    val renameMap = mutable.Map[String,String]()
    if(fields.size != set.size){
      //邮件发送
      val dealRenameList =  fields
      println(s"fields ${fields}")
      var i = 1
      dealRenameList.foreach(dealField=>{
        fields = fields.filter(field=>{
          val flag = !field.equals(dealField) && field.equalsIgnoreCase(dealField)
          if(flag && !whiteRenameMap.values.toSet.contains(field)){
            renameMap.put(field,s"${field}_${i}")
            i=i+1
            false
          }else{
            true
          }
        })
      })
    }
    if(renameMap.size !=0 ){
      val e = new RuntimeException(renameMap.toString())
      SendMail.post(e, s"[Log2Parquet new][Json2ParquetUtil] field rename", Array("app-bigdata@whaley.cn"))
    }
    println(s"whiteRenameMap ${whiteRenameMap.values.toSet}")
    println(s"renameMap ${renameMap.toMap}")
    println("fields "+fields)
    val allRenameMap = whiteRenameMap ++ renameMap.toMap
    println(s"allRenameMap ${allRenameMap}")
    //把重命名字段做到field list中
    allRenameMap.foreach(f=>{
      val oldName = f._1
      val newName = f._2
      if(!fields.contains(newName)){
        fields = fields.::(s"`$oldName` as `$newName`")
      }else{
        fields = fields.filter(field=>field !=newName)
        fields = fields.::(s"(case when `$newName` is null or `$newName` == '' then `$oldName` else `$newName` end ) as `$newName`")
      }

    })
    println("fields "+fields)
    fields

  }



}
