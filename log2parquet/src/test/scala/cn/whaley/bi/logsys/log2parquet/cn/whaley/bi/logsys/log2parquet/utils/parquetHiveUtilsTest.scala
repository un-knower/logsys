package cn.whaley.bi.logsys.log2parquet.cn.whaley.bi.logsys.log2parquet.utils

import cn.whaley.bi.logsys.log2parquet.utils.ParquetHiveUtils
import org.apache.hadoop.fs.Path
import org.junit.Test

/**
  * Created by baozhiwang on 2017/7/4.
  */
class parquetHiveUtilsTest {

  @Test
  def parseSQLFieldInfos(): Unit ={
    val path=new Path("/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13/boikgpokn78sb95ktmsc1bnkechpgj9l_2017061413_raw_7_575892351.json.gz")
    val result=ParquetHiveUtils.parseSQLFieldInfos(path)
     result.map(e=>println(e))
  }
}
