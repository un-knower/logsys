package cn.whaley.bi.logsys.log2parquet.utils

import java.sql.{Connection, DriverManager}

/**
  * Created by baozhiwang on 2017/6/13.
  */
object PhoenixUtils {

  /**It is not necessary to pool Phoenix JDBC Connections.
    * https://phoenix.apache.org/faq.html#Should_I_pool_Phoenix_JDBC_Connections */
  def getConnection:Connection={
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val con=DriverManager.getConnection("jdbc:phoenix:bigdata-cmpt-128-1,bigdata-cmpt-128-13,bigdata-cmpt-128-25:2181")
    con
  }

}
