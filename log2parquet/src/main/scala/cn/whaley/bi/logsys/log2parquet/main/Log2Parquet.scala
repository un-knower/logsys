package cn.whaley.bi.logsys.log2parquet.main

import cn.whaley.bi.logsys.log2parquet.utils.PhoenixUtils

/**
  * Created by baozhiwang on 2017/6/13.
  */
object Log2Parquet {
  def main(args: Array[String]) {
    val con=PhoenixUtils.getConnection
    val stat=con.createStatement()
    val rst = stat.executeQuery("select * from METADATA.APPLOG_SPECIAL_FIELD_DESC")
    while (rst.next()) {
      println(rst.getString(1))
    }
  }
}

/**
*   !describe METADATA.APPLOG_SPECIAL_FIELD_DESC
  *
  *   UPSERT INTO METADATA.APPLOG_SPECIAL_FIELD_DESC VALUES('foo','bar');
  *   select * from METADATA.APPLOG_SPECIAL_FIELD_DESC
* */