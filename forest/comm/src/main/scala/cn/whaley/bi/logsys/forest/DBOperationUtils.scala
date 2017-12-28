package cn.whaley.bi.logsys.forest

import java.sql.SQLException
import java.util.{List, Map}
import javax.sql.DataSource

import com.dianping.zebra.group.jdbc.GroupDataSource
import org.springframework.jdbc.core.JdbcTemplate


class DBOperationUtils(var jdbcRef: String) {
  private var dataSource: DataSource = null
  private var jdbcTemplate: JdbcTemplate = null


  try {
    val groupDataSource = new GroupDataSource(jdbcRef)
    groupDataSource.init()
    dataSource = groupDataSource
    //else throw new RuntimeException("The database name(" + database + ") is not valid!")
    jdbcTemplate = new JdbcTemplate(dataSource)
  }
  catch {
    case e: Exception => {
      e.printStackTrace
    }
  }


  /**
    * 向数据库中插入记录
    *
    * @param sql    预编译的sql语句
    * @param params 插入的参数
    * @return 影响的行数
    * @throws SQLException
    */
  def insert(sql: String, params: Object*): Int = {
    jdbcTemplate.update(sql, params: _*)
  }

  /**
    * Update the data of the database
    */
  def update(sql: String, params: Object*): Int = {
    jdbcTemplate.update(sql, params: _*)
  }


  /**
    * 通过指定的SQL语句和参数查询数据
    *
    * @param sql    预编译的sql语句
    * @param params 查询参数
    * @return 查询结果
    */
  def selectMapList(sql: String, params: Object*): List[Map[String, Object]] = {
    jdbcTemplate.queryForList(sql, params: _*)
  }

  /**
    * 删除错乱的数据
    *
    * @param sql    delete sql
    * @param params delete sql params
    * @return
    */
  def delete(sql: String, params: Object*) = {
    jdbcTemplate.update(sql, params: _*)
  }

  /**
    * 释放资源，如关闭数据库连接
    */
  def destory(): Unit = {

  }
}

