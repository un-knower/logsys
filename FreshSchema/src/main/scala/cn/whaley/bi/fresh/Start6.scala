package cn.whaley.bi.fresh

import java.io.PrintWriter
import java.util.concurrent.Callable

import cn.whaley.bi.fresh.entity.ParquetSchema
import cn.whaley.bi.fresh.sevice.HiveSevice
import cn.whaley.bi.fresh.util.ParquetHiveUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer


/**
  * 创建人：郭浩
  * 创建时间：2018/02/26
  * 程序作用：1.输出重命名的字段2.输出需要删除的字段
  * 数据输入：表名规则，日期规则，文件路径
  * 数据输出：
  */



object Start6 {
  def main(args: Array[String]): Unit = {

    val config = new Configuration()
    config.setBoolean("fs.hdfs.impl.disable.cache", true)

    val sparkConf = new SparkConf()
    //区分大小写，方能处理SSID，ssid同时存在于一条记录的情况
    sparkConf.set("spark.sql.caseSensitive", "true")
    sparkConf.setMaster("local[2]")
    val tablePattern = "log_eagle_main_live%"
    val partitionPattern = "%201710%"
        val fileName = "eagle.txt"
//    val tablePattern = args(0)
//    val partitionPattern = args(1)
//    val fileName = args(2)

    println(s"tablePattern is ${tablePattern}")
    println(s"partitionPattern is ${partitionPattern}")
    println(s"fileName is ${fileName}")

    val pathSchemas = HiveSevice.getPathSchema(tablePattern,partitionPattern)
    val outPutSchemas =  pathSchemas.map(pathSchema=>{
      val path = pathSchema.path
      val hiveColSchema = pathSchema.col
      val parquetSchema = getSchema(path)
      val deleteField = new ListBuffer[String]
      val selectField = new ListBuffer[String]

      //parquet字段列表
      val parquetFields = new ListBuffer[String]

      parquetSchema.foreach(f=>{
        val fieldName = f.fieldName
        parquetFields.append(fieldName)
        val colType = hiveColSchema.getOrElseUpdate(fieldName.toLowerCase,null)
        if( colType == null){
          //parquet独有字段 删除
          deleteField.append(fieldName)
        }else{
          selectField.append(fieldName)
        }
      })
      //忽略大小写重复字段
      val fields = parquetFields.toList.sortBy(f=>f.length)
      val replaceFile =  new ListBuffer[String]
      for(i<- 0 to (fields.size-2)){
        if(fields(i).equalsIgnoreCase(fields(i+1))){
          replaceFile.append(s"${fields(i) -> fields(i+1)}")
        }
      }

      //没有多余的字段，返回null
      if(deleteField.length == 0 && replaceFile.length ==0){
        null
      }else{
        (path,replaceFile.toList,deleteField.toList,selectField.toList)
      }
    }).filter(f=>{
      f != null
    })

    val replaceSchema = outPutSchemas.filter(f=>f._2.size !=0 )
    println("replaceSchema size is "+replaceSchema.size)
    val out1 = new PrintWriter(s"f://${fileName}")
    replaceSchema.foreach(f=>{
      println(s" path ... ${f._1}")
      println(s" replaceFile ... ${f._2}")
      println("-----------------------")
      out1.append(s"${f._1}").append("|").append(s"${f._2}").append("\n")
    })
    out1.append("===========删除字段==================== \n")
    out1.flush()
    val filterSchema = outPutSchemas.filter(f=>f._3.size !=0 )
    println("filterSchema size is "+filterSchema.size)
    filterSchema.foreach(f=>{
      println(s" path ... ${f._1}")
      println(s" deleteField ... ${f._3}")
      println(s" selectField ... ${f._4}")
//      out1.append(s"${f._1}").append("|").append(s"${f._3}").append("|").append(s"${f._4}")
      out1.append(s"${f._1}").append("|").append(s"${f._3}").append("\n")
      println("-----------------------")
    })
    out1.flush()


    var list = new ListBuffer[String]
    filterSchema.foreach(f=> {
      list = list ++ f._3
    })

    out1.append("============删除字段set=================== \n")
    out1.append(list.toSet.toString())
    out1.flush()
    out1.close()


    println("is over ....")



  }






  /**
    * 解析 parquet schema
    * @param path
    * @return
    */
  def getSchema(path:String):Seq[ParquetSchema]={
      val list = ParquetHiveUtils.getParquetFilesFromHDFS(path)
      val fieldSchema = if (list.size > 0) {
        val parquetMetaData = ParquetHiveUtils.parseSQLFieldInfos(list.head.getPath)
        parquetMetaData.map(meta => {
          val fieldName = meta._1
          val fieldType = meta._2
          val fieldSql = meta._3
          val rowType = meta._4
          val parquetSchema = new ParquetSchema
          parquetSchema.fieldName = fieldName
          parquetSchema.fieldType = fieldType
          parquetSchema.fieldSql = fieldSql
          parquetSchema.rowType = rowType
          parquetSchema
        })
      }else{
        Nil
      }
      fieldSchema
  }


}
