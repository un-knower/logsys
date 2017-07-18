import java.io.File
import java.util.Date

import cn.whaley.bi.logsys.log2parquet.MainObj
import cn.whaley.bi.logsys.log2parquet.constant.Constants
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.utils.{MetaDataUtils, ParquetHiveUtils}
import cn.whaley.bi.logsys.metadata.entity.LogFileFieldDescEntity
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
      "--f","MsgBatchManagerV3.xml,settings.properties","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13/boikgpokn78sb95ktmsc1bnkechpgj9l_2017061413_raw_7_575892351.json.gz","--c","masterURL=local[2]")
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
    val start="true"
    println(start.toBoolean)
  }

}
