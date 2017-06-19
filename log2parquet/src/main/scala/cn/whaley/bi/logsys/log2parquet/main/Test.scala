package cn.whaley.bi.logsys.log2parquet.main

import cn.whaley.bi.logsys.log2parquet.utils.DateFormatUtils

/**
  * Created by baozhiwang on 2017/6/14.
  */
object Test {

  def main(args: Array[String]) {
    val hour = DateFormatUtils.enHourAdd("23", 1)
    println(hour)


    /*val con=PhoenixUtils.getConnection
    val stat=con.createStatement()
    val rst = stat.executeQuery("select * from METADATA.APPLOG_SPECIAL_FIELD_DESC")
    while (rst.next()) {
      println(rst.getString(1))
    }*/

  }
}
