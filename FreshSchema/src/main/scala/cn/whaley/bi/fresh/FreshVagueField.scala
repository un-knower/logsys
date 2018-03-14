package cn.whaley.bi.fresh

import java.util.concurrent.{Callable, Executors, TimeUnit}

import cn.whaley.bi.fresh.entity.ParquetSchema
import cn.whaley.bi.fresh.sevice.HiveSevice
import cn.whaley.bi.fresh.util.{ParquetHiveUtils, SendMail}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by guohao on 2018/3/13.
  * 刷新模糊字段
  */
object FreshVagueField {
  def main(args: Array[String]): Unit = {

    val config = new Configuration()
    config.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.get(config)

    val sparkConf = new SparkConf()
//    区分大小写，方能处理SSID，ssid同时存在于一条记录的情况
    sparkConf.set("spark.sql.caseSensitive", "true")
//    sparkConf.setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

//        val tablePattern = "log_whaleytv_main_paystatus"
//        val partitionPattern = "%20171114%"

    val tablePattern = args(0)
    val partitionPattern = args(1)

    println(s"tablePattern is ${tablePattern}")
    println(s"partitionPattern is ${partitionPattern}")

    val pathSchemas = HiveSevice.getPathSchema(tablePattern,partitionPattern)
    val replaceSchema =  pathSchemas.map(pathSchema=>{
      val path = pathSchema.path
      val parquetSchema = getSchema(path)
      //parquet字段列表
      val parquetFields = new ListBuffer[String]

      parquetSchema.foreach(f=>{
        val fieldName = f.fieldName
        parquetFields.append(fieldName)
      })
      //忽略大小写重复字段
      println(s"parquetFields ${parquetFields.toString()}")
      val fields = parquetFields.toList.sortBy(f=>f.length)
      val replaceFile =  new ListBuffer[List[String]]
      for(i<- 0 to (fields.size-2)){
        if(fields(i).equalsIgnoreCase(fields(i+1))){
          val vagleFields = new ListBuffer[String]
          vagleFields.append(fields(i))
          vagleFields.append(fields(i+1))
          replaceFile.append(vagleFields.toList)
        }
      }

      //没有多余的字段，返回null
      if(replaceFile.length ==0){
        null
      }else{
        (path,replaceFile.toList,parquetFields.toList)
      }
    }).filter(f=>{
      f != null
    })
//     println("replaceSchema size is "+replaceSchema.size)
//     replaceSchema.foreach(f=>{
//       println(s" path ... ${f._1}")
//       println(s" replaceFile ... ${f._2}")
//       println(s" parquetFields ... ${f._3}")
//       println("-----------------------")
//     })

    if(replaceSchema.size <=0){
      System.exit(-1)
    }

//    System.exit(-1)
    val executor = Executors.newFixedThreadPool(Math.min(50,replaceSchema.size))
    val futures = replaceSchema.map(f=>{
      val inputPath = f._1
      val replaceFile = f._2
      val parquetFields = f._3
      val callable = new ProcessCallable(sparkSession,fs,inputPath,replaceFile,parquetFields)
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


  private class ProcessCallable(sparkSession: SparkSession,fs:FileSystem,inputPath:String,replaceFile:List[List[String]],parquetFields:List[String]) extends Callable[String]{
    override def call(): String = {
      try {

        val df = sparkSession.read.parquet(inputPath)
        val deleteFields = new ListBuffer[String]
        val alarmFields = new ListBuffer[List[String]]
        val info = replaceFile.map(replaceFile=>{
          val col1 = replaceFile(0)
          val col2 = replaceFile(1)
          val count1 = df.select(col1).where(s"$col1 is not null").count().toDouble
          val count2 = df.select(col2).where(s"$col2 is not null ").count().toDouble
          println(s" col1->$col1 ,count1 ->${count1} ")
          println(s" col2->$col2 ,count2 ->${count2} ")
          val maxCount = Math.max(count1,count2)
          val totalCount = count1 + count2
          println(s"maxCount $maxCount")
          println(s"totalCount $totalCount")
          val rate = maxCount/totalCount
          println(s"rate $rate")
          if(rate >= 0.95 ){
            //删除字段,删除count值小的
            if(maxCount == count1){
              println(s"删除 col2->$col2 ,count2 ->${count2} ")
              deleteFields.append(col2)
            }else{
              println(s"删除 col1->$col1 ,count1 ->${count1} ")
              deleteFields.append(col1)
            }
          }else{
            //输出路径，删除规则等信息
            alarmFields.append(replaceFile)
          }

          //alarmFields两者之前差距甚微需要告警
          if(alarmFields.size > 0){
            //发告警邮件，或输出
            val message = s" rate->$rate , col1->$col1->$count1 , col2->$col2->$count2 , path->$inputPath"
            val subject = "[刷新模糊字段 rate <0.95 ]"
            val users = Array("app-bigdata@whaley.cn")
            SendMail.post(message,subject,users)
          }
          //deleteFields 字段删除，执行select
          println(s"deleteFields $deleteFields")
          var restult = ""
          if(deleteFields.size > 0 ){
            val selectFields = parquetFields.filter(field=> !deleteFields.contains(field))
            println(s"selectFields $selectFields")
            restult = executeFilterField(df,fs,inputPath,selectFields)
          }
          restult
        })
        info.toString()
      }catch {
        case e:Exception =>{
          e.printStackTrace()
          s"$inputPath is error ..."
        }
      }
    }

    def executeFilterField(df:DataFrame,fs:FileSystem,inputPath:String,selectFields:List[String]): String ={
      try {
        val splitSize = (150 * 1024 * 1024).toLong
        val inputSize = fs.getContentSummary(new Path(inputPath)).getLength
        val partitionNum = Math.ceil(inputSize.toDouble / splitSize.toDouble).toInt
        println(s"partitionNum is ${partitionNum}")
        val outPath = inputPath
        val tmpInputPath = inputPath.replaceAll("hans","hans/tmp")
        val tmpOutPath = s"${tmpInputPath}/bak"
        //1.写临时的输出目录
        df.selectExpr(selectFields:_*).coalesce(partitionNum).write.mode(SaveMode.Overwrite).parquet(tmpOutPath)
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
