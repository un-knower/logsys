import cn.whaley.bi.logsys.log2parquet.MainObj
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.utils.ParquetHiveUtils
import org.apache.hadoop.fs.Path
import org.junit.Test

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
    //val path=new Path("/data_warehouse/ods_view.db/log_medusa_main3x_event_medusa_player_sdk_startPlay/key_day=19700101/key_hour=08/[a-z0-9.-]*.parquet")
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
}
