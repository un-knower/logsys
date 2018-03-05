package cn.whaley.bi.fresh

import java.util.concurrent.{Callable, Executors, TimeUnit}

import cn.whaley.bi.fresh.entity.ParquetSchema
import cn.whaley.bi.fresh.sevice.HiveSevice
import cn.whaley.bi.fresh.util.ParquetHiveUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer


/**
  * 创建人：郭浩
  * 创建时间：2018/02/26
  * 程序作用：1.去重parquet中和hive schema 不同的字段
  * 数据输入：表名规则，日期规则
  * 数据输出：
  */



object Start5 {
  def main(args: Array[String]): Unit = {

    val retainFields = Array("method","urlPath","remoteIp","host","hour",
      "forwardedIp","product","log_msgId","params","tags")
    val config = new Configuration()
    config.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.get(config)

    val sparkConf = new SparkConf()
    //区分大小写，方能处理SSID，ssid同时存在于一条记录的情况
    sparkConf.set("spark.sql.caseSensitive", "true")
//    sparkConf.setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

//    val tablePattern = "log_medusa_main3x_homeview"
//    val partitionPattern = "%20170214%"

    val tablePattern = args(0)
    val partitionPattern = args(1)

    println(s"tablePattern is ${tablePattern}")
    println(s"partitionPattern is ${partitionPattern}")

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
        if( colType == null && !retainFields.contains(fieldName) ){
          //parquet独有字段 删除 ,部分字段保留
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

   /* val replaceSchema = outPutSchemas.filter(f=>f._2.size !=0 )
    println("replaceSchema size is "+replaceSchema.size)
    replaceSchema.foreach(f=>{
      println(s" path ... ${f._1}")
      println(s" replaceFile ... ${f._2}")
      println("-----------------------")
    })*/
    val filterSchema = outPutSchemas.filter(f=>f._3.size !=0 )
    println("filterSchema size is "+filterSchema.size)
    filterSchema.foreach(f=>{
      print(s" path ... ${f._1}")
      print(s" || deleteField ... ${f._3}")
      println(s" || selectField ... ${f._4}")
      println("-----------------------")
    })

    if(filterSchema.size <=0){
      System.exit(-1)
    }

//    System.exit(-1)


    val executor = Executors.newFixedThreadPool(Math.min(50,filterSchema.size))
    val futures = filterSchema.map(f=>{
      val inputPath = f._1
      val fieldSchema = f._4
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


  def getFieldName(fieldName:String): String ={
    //纯数字字段
    val flag = isDigit(fieldName)
    if(flag){
      s"`$fieldName`"
    }else{
      fieldName
    }
  }

  def isDigit(s:String)={
    val regex = "^([0-9]+)$"
    s.matches(regex)
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
        val splitSize = (150 * 1024 * 1024).toLong
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
        fs.rename(new Path(inputPath),new Path(tmpInputPath))
        //3.move tmpOutPath 数据 到 最终的输出目录
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
