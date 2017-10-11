package cn.whaley.bi.logsys.log2parquet

import java.util.concurrent._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by guohao on 2017/7/26.
  */
object RecordCompare {
  var  parquetMap = Map[String,Long]()
  var jsonMap = Map[String,Long]()
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.set("spark.scheduler.listenerbus.eventqueue.size","10000")
    val sparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).config(conf).getOrCreate()
    val parquetPath="/data_warehouse/ods_view.db/"
    val jsonPath="/log/default/parquet/ods_view/1501055504124_json/"
//    val totalJsonSize = sparkSession.read.json("/log/default/parquet/ods_view/1501055504124_json/ods_view.db*key_day=20170726#key_hour=04").count()
//    println("parquet total size is : "+totalParquetSize)
//    println("json total size is : "+totalJsonSize)

    val config = new Configuration()
    val fs = FileSystem.get(config)
    val a = ArrayBuffer[String]()
    val b = ArrayBuffer[String]()

    getFiles(a,fs,new Path(parquetPath),"key_day=20170726/key_hour=04")
    getFiles(b,fs,new Path(jsonPath),"key_day=20170726#key_hour=04")

    println(" parquet dir size is : "+a.distinct.size)
    println(" json dir size is : "+b.distinct.size)
    computeRecordNum(sparkSession,a.distinct.toArray,b.distinct.toArray)

    println("#####################1:"+jsonMap.size)
    println("#####################1:"+parquetMap.size)

    jsonMap.keySet.foreach(jsonPath=>{
      val jsonCount = jsonMap.get(jsonPath).getOrElse(0)
      val parquetPath = jsonPath.replace("#","/").replace("log/default/parquet/ods_view/1501055504124_json","data_warehouse")
      val parquetCount = parquetMap.get(parquetPath).getOrElse(0)
      if(jsonCount != parquetCount){
        println("jsonPath->jsonCount :"+jsonPath+"->"+jsonCount)
        println("parquetPath->parquetCount :"+parquetPath+"->"+parquetCount)
        println("=========================================")
      }
//      else{
//        println("记录条数相同 jsonpath->"+jsonPath+", 记录数->"+jsonCount+":"+parquetCount)
//      }
    })

    var parquetTotalNum = 0L
    parquetMap.values.foreach(value=>{
      parquetTotalNum += value
    })
    println("parquetTotalNum : "+parquetTotalNum)


    var jsonTotalNum = 0L
    jsonMap.values.foreach(value=>{
      jsonTotalNum += value
    })
    println("jsonTotalNum : "+jsonTotalNum)



  }

  /**
    * 获取路径
    * @param b
    * @param fs
    * @param path
    * @param pattern
    * @return
    */
  def getFiles(b:ArrayBuffer[String],fs: FileSystem,path: Path,pattern:String):Unit={
    val fileStatus = fs.listStatus(path)
    fileStatus.foreach(f=>{
      if(f.isDirectory){
        val path = f.getPath
        getFiles(b,fs,path,pattern)
      }else{
        if(f.getPath.getParent.toString.endsWith(pattern) && !b.contains(f.getPath.getParent.toString.trim()) ){
          b += f.getPath.getParent.toString.trim()
        }
      }
    })
  }

  def computeRecordNum2(sparkSession: SparkSession,parquetDirArray:Array[String],jsonDirArray:Array[String]): Unit ={
    //parquet
    val parquetPathQueue = new ConcurrentLinkedQueue[String]
    parquetDirArray.foreach(path=>{
      parquetPathQueue.add(path)
    })
    val parquetExecutor = Executors.newFixedThreadPool(Math.min(20,parquetDirArray.size))
    while(parquetPathQueue.size()!=0){
      val path = parquetPathQueue.poll()
      val parquetCallable = new ParquetProcessCallable(sparkSession,path)
      parquetExecutor.submit(parquetCallable)
    }

    //json
    val jsonPathQueue = new ConcurrentLinkedQueue[String]
    val jsonExecutor = Executors.newFixedThreadPool(Math.min(20,parquetDirArray.size))
    jsonDirArray.foreach(path=>{
      jsonPathQueue.add(path)
    })
    while(jsonPathQueue.size()!=0){
      val path = jsonPathQueue.poll()
      val jsonCallable = new JsonProcessCallable(sparkSession,path)
      jsonExecutor.submit(jsonCallable)
    }

    //关闭线程池
    parquetExecutor.shutdown()
    parquetExecutor.awaitTermination(1, TimeUnit.MINUTES)
    println("parquetExecutor shutdown....")
    //关闭线程池

    jsonExecutor.shutdown()
    jsonExecutor.awaitTermination(1, TimeUnit.MINUTES)
    println("jsonExecutor shutdown....")
  }

  def computeRecordNum3(sparkSession: SparkSession,parquetDirArray:Array[String],jsonDirArray:Array[String]): Unit ={
    //parquet
    val parquetExecutor = Executors.newFixedThreadPool(Math.min(30,parquetDirArray.size))
    val parquetFutures = parquetDirArray.map(path => {
      val parquetCallable = new ParquetProcessCallable(sparkSession,path)
      parquetExecutor.submit(parquetCallable)
    })

    //json
    val jsonExecutor = Executors.newFixedThreadPool(Math.min(30,parquetDirArray.size))
    val jsonFutures = jsonDirArray.map(path => {
      val jsonCallable = new JsonProcessCallable(sparkSession,path)
      jsonExecutor.submit(jsonCallable)
    })

    val procTimeOut = 3600
    //关闭线程池
    parquetFutures.foreach(future=>{
      val ret = future.get(procTimeOut,TimeUnit.SECONDS)
      println("parquetRet : "+ret)
    })
    parquetExecutor.shutdown()
    parquetExecutor.awaitTermination(3, TimeUnit.MINUTES)
    println("parquetExecutor shutdown....")

    //关闭线程池
    jsonFutures.foreach(future=>{
      val ret = future.get(procTimeOut,TimeUnit.SECONDS)
      println("jsonRet : "+ret)
    })
    jsonExecutor.shutdown()
    jsonExecutor.awaitTermination(3, TimeUnit.MINUTES)
    println("jsonExecutor shutdown....")

    println("###########Executor##########:"+jsonMap.size)
    println("###########Executor##########:"+parquetMap.size)

  }

  def computeRecordNum(sparkSession: SparkSession,parquetDirArray:Array[String],jsonDirArray:Array[String]): Unit ={
    //parquet
    val executor: ExecutorService = Executors.newFixedThreadPool(Math.min(20,parquetDirArray.size))
    val parquetFutures = parquetDirArray.map(path => {
      val parquetCallable = new ParquetProcessCallable(sparkSession,path)
      executor.submit(parquetCallable)
    })

    //json
    val jsonFutures = jsonDirArray.map(path => {
      val jsonCallable = new JsonProcessCallable(sparkSession,path)
      executor.submit(jsonCallable)
    })

    val procTimeOut = 36000
    //关闭线程池
    parquetFutures.foreach(future=>{
      val ret = future.get(procTimeOut,TimeUnit.SECONDS)
      println("parquetRet : "+ret)
    })

    //关闭线程池
    jsonFutures.foreach(future=>{
      val ret = future.get(procTimeOut,TimeUnit.SECONDS)
      println("jsonRet : "+ret)
    })

    executor.shutdown()
    executor.awaitTermination(5, TimeUnit.MINUTES)
    println("executor shutdown....")

    println("###########json##########:"+jsonMap.size)
    println("###########parquet##########:"+parquetMap.size)

  }

  private class ParquetProcessCallable(sparkSession:SparkSession,path:String) extends Callable[Long]{
    override def call():Long= {
      val count = sparkSession.read.parquet(path).count()
      parquetMap += (path->count)
      count
    }
  }
  private class JsonProcessCallable(sparkSession:SparkSession,path:String) extends Callable[Long]{
    override def call():Long ={
      val count = sparkSession.read.json(path).count()
      jsonMap +=(path->count)
      count
    }
  }
}
