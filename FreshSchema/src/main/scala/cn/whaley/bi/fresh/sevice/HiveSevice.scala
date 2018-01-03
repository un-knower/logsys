package cn.whaley.bi.fresh.sevice

import java.util

import cn.whaley.bi.fresh.entity.PathSchema
import cn.whaley.bi.fresh.util.SqlUtil

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by guohao on 2017/12/27.
  */
object HiveSevice {

  def main(args: Array[String]): Unit = {


    val tablePattern = "log_eagle_main_%"
    val db = new SqlUtil("com.mysql.jdbc.Driver", "jdbc:mysql://bigdata-cmpt-128-25:3306/hive?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "hive", "hive@whaley")

    val sql = s"select TBL_ID,TBL_NAME from TBLS where DB_ID = 254 and TBL_NAME like '$tablePattern'"

    //解析tableName 和 tableId
    val tables = db.selectArrayList(sql).map(arr=>{
      val tableId = arr(0).toString.toLong
      val tableName = arr(1).toString
      HiveTable("ods_view", tableName, tableId)
    })




    //1.获取表名、字段、类型
    val tableAndFieldAndTypeSql = s"""SELECT
                 |	tb.TBL_NAME,
                 |	COLUMNS_V2.COLUMN_NAME,
                 |	COLUMNS_V2.TYPE_NAME
                 |FROM
                 |	(
                 |		SELECT
                 |			*
                 |		FROM
                 |			TBLS
                 |		WHERE
                 |			DB_ID = 254 and TBL_NAME like '${tablePattern}'
                 |	) tb
                 |LEFT JOIN SDS ON tb.SD_ID = SDS.SD_ID
                 |LEFT JOIN COLUMNS_V2 ON SDS.CD_ID = COLUMNS_V2.CD_ID""".stripMargin

    //tableName ->map
    //map=>colName->colType
    val tableColMap = new mutable.HashMap[String,mutable.HashMap[String,String]]()
    db.selectArrayList(tableAndFieldAndTypeSql).map(arr=>{
      val tableName = arr(0).toString
      val colName = arr(1).toString
      val colType = arr(2).toString
      (tableName,colName,colType)
    }).groupBy(f=>{
      f._1
    }).foreach(f=>{
      val tableName = f._1
      val colMap = new mutable.HashMap[String,String]()
      f._2.foreach(col=>{
        val colName =  col._2
        val colType = col._3
        colMap.put(colName,colType)
      })
      tableColMap.put(tableName,colMap)
    })
 /*  tableColMap.foreach(f=>{
      val tableName = f._1
      println(s"tableName ... ${tableName} ------------------------------")
      f._2.foreach(col=>{
        val colName = col._1
        val colType = col._2
        println(s"colName ... ${colName} ->  colType ... ${colType}")
      })
    })*/

    //2.获取表和location 信息
    val partitionSql = "select part_name,location from PARTITIONS a join SDS b on a.sd_id = b.sd_id where a.tbl_id = ? order by part_name desc"
    val pathSchemas = new util.ArrayList[PathSchema]()
     tables.map(table => {
      val tableName = table.tableName
      val colMap = tableColMap.getOrElseUpdate(tableName,new mutable.HashMap[String,String])
       db.selectArrayList(partitionSql, table.tableId).foreach(arr => {
         val part_name = arr(0).toString
         println(s" part_name ${part_name}")
        val location = arr(1).toString
         val pathSchema = new PathSchema()
         pathSchema.path = location
         pathSchema.col = colMap
         pathSchemas.add(pathSchema)
      })
    })


    pathSchemas.foreach(schema=>{
      val path = schema.path
      val cols = schema.col
      println(s"path : ${path} -> cols : ${cols.toString()}")
    })

  }





  /**
    *
    * @param tablePattern
    */
  def getPathSchema(tablePattern:String,partitionPattern:String): List[PathSchema]  ={
    val db = new SqlUtil("com.mysql.jdbc.Driver", "jdbc:mysql://bigdata-cmpt-128-25:3306/hive?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "hive", "hive@whaley")
    val tableColMap = getTabelColSchema(db,tablePattern)
    getLocationWithSchema(db,tablePattern,partitionPattern,tableColMap)
  }



  def getTabelColSchema(db:SqlUtil,tablePattern:String): mutable.HashMap[String,mutable.HashMap[String,String]] ={
    //获取表名、字段、类型
    val tableAndFieldAndTypeSql = s"""SELECT
                                      |	tb.TBL_NAME,
                                      |	COLUMNS_V2.COLUMN_NAME,
                                      |	COLUMNS_V2.TYPE_NAME
                                      |FROM
                                      |	(
                                      |		SELECT
                                      |			*
                                      |		FROM
                                      |			TBLS
                                      |		WHERE
                                      |			DB_ID = 254 and TBL_NAME like '${tablePattern}'
                                      |	) tb
                                      |LEFT JOIN SDS ON tb.SD_ID = SDS.SD_ID
                                      |LEFT JOIN COLUMNS_V2 ON SDS.CD_ID = COLUMNS_V2.CD_ID""".stripMargin


    //tableName ->map
    //map=>colName->colType
    val tableColMap = new mutable.HashMap[String,mutable.HashMap[String,String]]()
    db.selectArrayList(tableAndFieldAndTypeSql).map(arr=>{
      val tableName = arr(0).toString
      val colName = arr(1).toString
      val colType = arr(2).toString
      (tableName,colName,colType)
    }).groupBy(f=>{
      f._1
    }).foreach(f=>{
      val tableName = f._1
      val colMap = new mutable.HashMap[String,String]()
      f._2.foreach(col=>{
        val colName =  col._2
        val colType = col._3
        colMap.put(colName,colType)
      })
      tableColMap.put(tableName,colMap)
    })
    tableColMap
  }


  def getLocationWithSchema(db:SqlUtil,tablePattern:String,partitionPattern:String,tableColMap:mutable.HashMap[String,mutable.HashMap[String,String]]): List[PathSchema] ={
    val sql = s"select TBL_ID,TBL_NAME from TBLS where DB_ID = 254 and TBL_NAME like '$tablePattern'"
    //解析tableName 和 tableId
    val tables = db.selectArrayList(sql).map(arr=>{
      val tableId = arr(0).toString.toLong
      val tableName = arr(1).toString
      HiveTable("ods_view", tableName, tableId)
    })

    //2.获取表和location 信息
//    val partitionSql = s"select part_name,location from PARTITIONS a join SDS b on a.sd_id = b.sd_id where a.tbl_id = ?   order by part_name desc"

    val partitionSql = s"""SELECT
                         |	part_name,
                         |	location
                         |FROM
                         |	(
                         |		SELECT
                         |			*
                         |		FROM
                         |			PARTITIONS
                         |		WHERE
                         |			part_name LIKE '${partitionPattern}'
                         |	) a
                         |JOIN SDS b ON a.sd_id = b.sd_id
                         |WHERE
                         |	a.tbl_id = ?
                         |ORDER BY
                         |	part_name DESC """.stripMargin

    val pathSchemas = new util.ArrayList[PathSchema]()
    tables.map(table => {
      val tableName = table.tableName
      val colMap = tableColMap.getOrElseUpdate(tableName,new mutable.HashMap[String,String])
      db.selectArrayList(partitionSql, table.tableId).foreach(arr => {

        val location = arr(1).toString
        val pathSchema = new PathSchema()
        pathSchema.path = location
        pathSchema.col = colMap
        pathSchemas.add(pathSchema)
      })
    })
    pathSchemas.toList
  }








}



case class HiveTable(dbName: String, tableName: String, tableId: Long)
