package cn.whaley.bi.logsys.log2parquet.utils

import java.sql.{Statement, Connection, DriverManager}

import com.alibaba.fastjson.{JSONObject, JSONArray}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by michael on 2017/6/13.
 */
object PhoenixUtils {

    /** It is not necessary to pool Phoenix JDBC Connections.
      * https://phoenix.apache.org/faq.html#Should_I_pool_Phoenix_JDBC_Connections */
    def getConnection: Connection = {
        val className="org.apache.phoenix.queryserver.client.Driver"
        val jdbcUrl="jdbc:phoenix:thin:url=http://bigdata-appsvr-130-7:8765;serialization=PROTOBUF"
        Class.forName(className)
        DriverManager.getConnection(jdbcUrl)
        //Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
        //val con = DriverManager.getConnection("jdbc:phoenix:bigdata-cmpt-128-1,bigdata-cmpt-128-13,bigdata-cmpt-128-25:2181")
        //con
    }


    def getStatement: Statement = {
        getConnection.createStatement()
        //Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
        //val con = DriverManager.getConnection("jdbc:phoenix:bigdata-cmpt-128-1,bigdata-cmpt-128-13,bigdata-cmpt-128-25:2181")
        //val stat = con.createStatement()
        //stat
    }

    def closeConnection(con: Connection) = {
        if (null != con) {
            con.close()
        }
    }

    /**
     * 执行查询,并以Seq[JSONObject]的形式返回结果
     *
     * @param sql
     * @return
     */
    def query(sql: String): Seq[JSONObject] = {
        val statement = getStatement
        val rs = statement.executeQuery(sql)
        val metadata = rs.getMetaData
        val result = new ArrayBuffer[JSONObject]()
        while (rs.next()) {
            val row = new JSONObject()
            for (i <- 1 to metadata.getColumnCount) {
                val key = metadata.getColumnLabel(i)
                val value = rs.getObject(i)
                row.put(key, value)
            }
            result += row
        }
        statement.close()
        result
    }

}
