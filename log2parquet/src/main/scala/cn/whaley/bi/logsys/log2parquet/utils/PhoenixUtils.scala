package cn.whaley.bi.logsys.log2parquet.utils

import java.sql.{Statement, Connection, DriverManager}

/**
  * Created by michael on 2017/6/13.
  */
object PhoenixUtils {

  /**It is not necessary to pool Phoenix JDBC Connections.
    * https://phoenix.apache.org/faq.html#Should_I_pool_Phoenix_JDBC_Connections */
  def getConnection:Connection={
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val con=DriverManager.getConnection("jdbc:phoenix:bigdata-cmpt-128-1,bigdata-cmpt-128-13,bigdata-cmpt-128-25:2181")
    con
  }


  def getStatement:Statement={
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val con=DriverManager.getConnection("jdbc:phoenix:bigdata-cmpt-128-1,bigdata-cmpt-128-13,bigdata-cmpt-128-25:2181")
    val stat=con.createStatement()
    stat
  }

  def closeConnection(con:Connection)={
   if(null!=con){
    con.close()
   }
  }

}
