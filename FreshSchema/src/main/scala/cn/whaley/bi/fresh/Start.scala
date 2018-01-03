package cn.whaley.bi.fresh



import java.util.concurrent.{Callable, Executors, TimeUnit}

import cn.whaley.bi.fresh.entity.ParquetSchema
import cn.whaley.bi.fresh.sevice.HiveSevice
import cn.whaley.bi.fresh.util.ParquetHiveUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}



/**
  * 创建人：郭浩
  * 创建时间：2017/12/26
  * 程序作用：
  * 数据输入：
  * 数据输出：
  */
object Start {
  def main(args: Array[String]): Unit = {

    val config = new Configuration()
    config.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.get(config)

    val sparkConf = new SparkConf()
    //区分大小写，方能处理SSID，ssid同时存在于一条记录的情况
    sparkConf.set("spark.sql.caseSensitive", "true")
//    sparkConf.setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

//    val tablePattern = "log_whaleytv_main_play"
//    val partitionPattern = "%20171112%"

    val tablePattern = args(0)
    val partitionPattern = args(1)

    println(s"tablePattern is ${tablePattern}")
    println(s"partitionPattern is ${partitionPattern}")

    val pathSchemas = HiveSevice.getPathSchema(tablePattern,partitionPattern)
    val finalPathSchema = pathSchemas.map(pathSchema=>{
      val path = pathSchema.path
      val hiveColSchema = pathSchema.col
      val parquetSchema = getSchema(path)
      val selectSql = parquetSchema.map(f=>{
        val fieldName = f.fieldName
        val fieldType = f.fieldType
        val colType = hiveColSchema.getOrElseUpdate(fieldName.toLowerCase,null)
        if(colType == null || fieldType.equalsIgnoreCase("struct") || fieldType.equalsIgnoreCase("array") || fieldType.equalsIgnoreCase(colType)){
          //parquet独有字段 或者和hive schema 字段类型一致
          fieldName
        }else{
          //字段类型不一致，需要转型
          s" CAST( ${fieldName} as ${colType}) "
        }
      }).toList
      (path ,selectSql)
    }).filter(f=>{
      val selectSql = f._2.toString()
      selectSql.contains("CAST(")
    })

    finalPathSchema.foreach(f=>{
      println(f)
    })

    if(finalPathSchema.size == 0){
      println("no match patch ... ")
      System.exit(-1)
    }
    val executor = Executors.newFixedThreadPool(Math.min(100,finalPathSchema.size))
    val futures = finalPathSchema.map(f=>{
      val inputPath = f._1
      val fieldSchema = f._2
      val callable = new ProcessCallable(sparkSession,fs,inputPath,fieldSchema)
      executor.submit(callable)
    })
    //等待执行结果
    val jsonProcTimeOut = 7200
    futures.foreach(future => {
      val ret = future.get(jsonProcTimeOut, TimeUnit.SECONDS)
      println(ret)
    })
    //关闭线程池
    println("shutdown....")
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.MINUTES)
    println("shutdown.")

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

  def getSchema2(sparkSession: SparkSession,path:String): Unit ={
    val df = sparkSession.read.parquet(path)
    df.schema.foreach(f=>{
      val fieldName = f.name
      val fieldType = f.dataType.typeName
      println(s"fieldName ... ${fieldName}")
      println(s"fieldType ... ${fieldType}")
    })
  }

  private class ProcessCallable(sparkSession: SparkSession,fs:FileSystem,inputPath:String,fieldSchema:List[String]) extends Callable[String]{
    override def call(): String = {
      try {
        val splitSize = (100 * 1024 * 1024).toLong
        val inputSize = fs.getContentSummary(new Path(inputPath)).getLength
        val partitionNum = Math.ceil(inputSize.toDouble / splitSize.toDouble).toInt
        println(s"partitionNum is ${partitionNum}")

        val outPath = inputPath
        val tmpInputPath = inputPath.replaceAll("hans","hans/tmp")
        val tmpOutPath = s"${tmpInputPath}/bak"
        val df = sparkSession.read.parquet(inputPath)
        //1.写临时的输出目录
        df.selectExpr(fieldSchema:_*).coalesce(partitionNum).write.mode(SaveMode.Overwrite).parquet(tmpOutPath)
        //2.move input数据到 tmpInputPath
//        val tmpInputPathParent = tmpInputPath.substring(0,tmpInputPath.lastIndexOf("/"))
        fs.rename(new Path(inputPath),new Path(tmpInputPath))
        //3.move tmpOutPath 数据 到 最终的输出目录
//        val outPathParent = outPath.substring(0,outPath.lastIndexOf("/"))
        fs.rename(new Path(tmpOutPath),new Path(outPath))
        s"inputPath $inputPath is sucess  ... \n tmpOutPath is ${tmpOutPath} \n tmpInputPath is ${tmpInputPath} "
      }catch {
        case e:Exception =>{
          e.printStackTrace()
          s"$inputPath is error ..."
        }
      }
    }


  }

}
