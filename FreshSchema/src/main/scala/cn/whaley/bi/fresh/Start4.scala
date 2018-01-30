package cn.whaley.bi.fresh

import java.util.concurrent.{Callable, Executors, TimeUnit}

import cn.whaley.bi.fresh.sevice.HiveSevice
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._

/**
  * Created by guohao on 2018/1/2.
  * 特定字段类型修改，如recommendList 为 arrayStruct
  * 1.读入parquet路基
  * 2.更具去schema判断是否包含该字段，以及该字段的类型
  * 3.转换成json，并且为该字段类型修改
  *
  * spark@bigdata-appsvr-130-5
  * cd /opt/dw/guohao/freshSchema/bin
  *  sh tmp4.sh log_whaleytv_main_ad_vod_adcast  %201712% adList
  */
object Start4 {

  def main(args: Array[String]): Unit = {

    val config = new Configuration()
    config.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.get(config)

    val sparkConf = new SparkConf()
    //区分大小写，方能处理SSID，ssid同时存在于一条记录的情况
    sparkConf.set("spark.sql.caseSensitive", "true")
//        sparkConf.setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

//        val tablePattern = "log_medusa_main3x_medusa_launcher_area_quit"
//        val partitionPattern = "%20171226%"
//        val field = "recommendList"

    val tablePattern = args(0)
    val partitionPattern = args(1)
    val field = args(2)
    println(s"tablePattern is ${tablePattern}")
    println(s"partitionPattern is ${partitionPattern}")
    println(s"field is ${field}")

    val broadcast = sparkSession.sparkContext.broadcast[String](field)

    val pathSchemas = HiveSevice.getPathSchema(tablePattern,partitionPattern)
    val finalPathSchema = pathSchemas.map(pathSchema=>{
      val path = pathSchema.path
      val schema = sparkSession.read.parquet(path).schema
      val flag = if(schema.fieldNames.contains(field)){
        //判断该字段的类型是否为ArrayType
        val size = schema.fields.filter(f=>{
          val name = f.name
          val dataType = f.dataType
          if(name.equalsIgnoreCase(field) &&  dataType.isInstanceOf[ArrayType]){

            true
          }else{
            false
          }
        }).size
        //最多只能有一个字段，且其类型为ArrayType
        if(size ==1){
          true
        }else{
          false
        }
      }else{
        false
      }

      (path,flag)
    }).filter(f=>{
      f._2
    })



    if(finalPathSchema.size == 0){
      println(s"finalPathSchema length is 0")
      System.exit(-1)
    }
    println(s"finalPathSchema.size ${finalPathSchema.size}")

    val executor = Executors.newFixedThreadPool(Math.min(100,finalPathSchema.size))
    val futures = finalPathSchema.map(f=>{
      val inputPath = f._1
      val callable = new ProcessCallable(sparkSession,fs,inputPath,broadcast)
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



  private class ProcessCallable(sparkSession: SparkSession,fs:FileSystem,inputPath:String,broadcast:Broadcast[String]) extends Callable[String]{
    override def call(): String = {
      try {
        val splitSize = (150 * 1024 * 1024).toLong
        val inputSize = fs.getContentSummary(new Path(inputPath)).getLength
        val partitionNum = Math.ceil(inputSize.toDouble / splitSize.toDouble).toInt
        println(s"partitionNum is ${partitionNum}")

        val outPath = inputPath
        //原文件备份
        val tmpInputPath = inputPath.replaceAll("hans","hans/tmp")
        val tmpParentPath = tmpInputPath.substring(0,tmpInputPath.lastIndexOf("/"))
        //存放的临时文件
        val tmpInputPath2 = inputPath.replaceAll("hans","hans/tmp2")
        //源parquet 转json
        val tmpOutPathJson = s"${tmpInputPath2}/json"
        //json处理后的文件
        val tmpOutPathJson2 = s"${tmpInputPath2}/json2"
        //parquet转json
        sparkSession.read.parquet(inputPath).write.json(tmpOutPathJson)
        //原始文件移动到临时目录下
        if(!fs.exists(new Path(tmpParentPath))){
          fs.mkdirs(new Path(tmpParentPath))
        }
        fs.rename(new Path(inputPath),new Path(tmpParentPath))

        //业务逻辑处理
        val field = broadcast.value
        val sc = sparkSession.sparkContext
          sc.textFile(tmpOutPathJson).
          map(line=>{
            val jsObj = JSON.parseObject(line)
          try {
            val keys = jsObj.keySet().toList
            if(keys.contains(field)){
              //修正字段类型
              val jSONArray = jsObj.getJSONArray(field)
              if(jSONArray.size() == 0){
                jsObj.remove(field)
              }else{
                val newJsonArray = new JSONArray()
                for( i<- 0 to ( jSONArray.size()-1 )){
                  val jsonObject = jSONArray.getJSONObject(i)
                  val keys = jsonObject.keySet().toList
                  keys.foreach(key=>{
                    jsonObject.put(key,jsonObject.getString(key))
                  })
                  newJsonArray.add(jsonObject)
                }
                jsObj.put(field,newJsonArray)
              }
            }
            jsObj
          }catch {
            case e:Exception=>{
              jsObj.remove(field)
              jsObj
            }
          }
        }).saveAsTextFile(tmpOutPathJson2)
        //保存到输出目录及原始目录
        sparkSession.read.json(tmpOutPathJson2).coalesce(partitionNum).write.mode(SaveMode.Overwrite).parquet(outPath)

        s"inputPath $inputPath is sucess  ... \n  tmpParentPath is ${tmpParentPath}  \n outPath is ${outPath}  \n tmpOutPathJson is ${tmpOutPathJson}"
      }catch {
        case e:Exception =>{
          e.printStackTrace()
          s"$inputPath is error ..."
        }
      }
    }


  }

}
