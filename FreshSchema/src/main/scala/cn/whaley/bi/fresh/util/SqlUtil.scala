package cn.whaley.bi.fresh.util

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.sql.{Connection, DriverManager, SQLException}
import java.util.{List, Map}

import org.apache.commons.dbutils.handlers.{ArrayHandler, ArrayListHandler, MapListHandler}
import org.apache.commons.dbutils.{DbUtils, QueryRunner}


class SqlUtil(driver: String, url: String, user: String, password: String) {

  /**
    * 定义变量
    */
  val queryRunner: QueryRunner = new QueryRunner

  Class.forName(driver)
  lazy val conn: Connection = DriverManager.getConnection(url, user, password)

  /**
    * 向数据库中插入记录
    *
    * @param sql    预编译的sql语句
    * @param params 插入的参数
    * @return 影响的行数
    * @throws SQLException
    */
  def insert(sql: String, params: Any*): Int = {
    queryRunner.update(conn, sql, asJava(params): _*)
  }

  /**
    * 通过指定的SQL语句和参数查询数据
    *
    * @param sql    预编译的sql语句
    * @param params 查询参数
    * @return 查询结果
    */
  def selectOne(sql: String, params: Any*): Array[AnyRef] = {
    queryRunner.query(conn, sql, new ArrayHandler(), asJava(params): _*)
  }

  /**
    * 通过指定的SQL语句和参数查询数据
    *
    * @param sql    预编译的sql语句
    * @param params 查询参数
    * @return 查询结果
    */
  def selectMapList(sql: String, params: Any*): List[Map[String, AnyRef]] = {
    queryRunner.query(conn, sql, new MapListHandler(), asJava(params): _*)
  }

  /**
    * 通过指定的SQL语句和参数查询数据
    *
    * @param sql    预编译的sql语句
    * @param params 查询参数
    * @return 查询结果
    */
  def selectArrayList(sql: String, params: Any*): List[Array[AnyRef]] = {
    queryRunner.query(conn, sql, new ArrayListHandler(), asJava(params): _*)
  }

  def executeSql(sql:String) = {
    queryRunner.update(sql)
  }

  /**
    * 删除错乱的数据
    *
    * @param sql    delete sql
    * @param params delete sql params
    * @return
    */
  def delete(sql: String, params: Any*) = {
    queryRunner.update(conn, sql, asJava(params): _*)
  }

  /**
    * 释放资源，如关闭数据库连接
    */
  def destory() {
    DbUtils.closeQuietly(conn)
  }

  def update(sql: String, params: Any*): Int = {
    queryRunner.update(conn, sql, asJava(params): _*)
  }

  def asJava(params: Seq[Any]) = {
    params.map {
      case null => null
      case e: Long => new JLong(e)
      case e: Double => new JDouble(e)
      case e: String => e
      case e: Short => new JShort(e)
      case e: Int => new Integer(e)
      case e: Float => new JFloat(e)
      case e: Any => e.asInstanceOf[Object]
    }
  }

}
