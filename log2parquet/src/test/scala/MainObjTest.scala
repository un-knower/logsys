import java.io.File
import java.util.Date

import cn.whaley.bi.logsys.log2parquet.MainObj
import cn.whaley.bi.logsys.log2parquet.constant.Constants
import cn.whaley.bi.logsys.log2parquet.processingUnit.JsonFormatProcessingUnits
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.utils.{MetaDataUtils, ParquetHiveUtils}
import cn.whaley.bi.logsys.metadata.entity.LogFileFieldDescEntity
import com.alibaba.fastjson.JSON
import org.apache.hadoop.fs.{FileStatus, Path}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
  * Created by baozhiwang on 2017/7/4.
  */
class MainObjTest extends LogTrait{
  @Test
  def testMainObjTest: Unit = {
    val args = Array("MsgProcExecutor",
      "--f","MsgBatchManagerV3.xml,settings.properties","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId2=boikgpokn78sb95kjhfrendo8dc5mlsr/key_day=20170904/key_hour=23/boikgpokn78sb95kjhfrendo8dc5mlsr_ffcb2dea-6bad-474a-832c-8129257908c0.json.gz","--c","masterURL=local[1]","--c","isJsonDirDelete=true", "--c" ,"isTmpDirDelete=false")
      //      "--f","MsgBatchManagerV3.xml,settings.properties","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendo8dc5mlsr/key_day=20170731/key_hour=20/boikgpokn78sb95kjhfrendo8dc5mlsr_2017073120_raw_9_589961803.json.gz","--c","masterURL=local[1]","--c","isJsonDirDelete=true", "--c" ,"isTmpDirDelete=false")
//      "--f","MsgBatchManagerV3.xml,settings.properties","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendo8dc5mlsr/key_day=20170101/key_hour=00/a.json","--c","masterURL=local[1]","--c","isJsonDirDelete=true", "--c" ,"isTmpDirDelete=false")
    //    "--f","MsgBatchManagerV3.xml,settings.properties","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170101/key_hour=00/a.json","--c","masterURL=local[1]")
    //     "--f","MsgBatchManagerV3.xml,settings.properties","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170723/key_hour=01/boikgpokn78sb95ktmsc1bnkechpgj9l_2017072301_raw_0_1392502233.json.gz","--c","masterURL=local[1]")

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


  def getUtils()=new MetaDataUtils( "http://bigdata-appsvr-130-5:8084")

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
    val taskId="AAABXS9LtE8K4Ax2FoAAAAA"
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
    val line="{\"msgId\":\"ac\"}"
    val jsonObject=JSON.parseObject(line)
    println(jsonObject)
    if(jsonObject.get("msgId").equals("ac")){
      println("====equal")
    }
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

}
