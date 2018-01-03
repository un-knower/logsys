package cn.whaley.bi.fresh

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SparkSession}

/**
  * Created by guohao on 2018/1/2.
  */
object Start2 {
  def main(args: Array[String]): Unit = {
    val config = new Configuration()
    config.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.get(config)
    val pathExp = args(0)
//    val pathExp = "/tmp/log/whaley/parquet/20160610/*/"
    println(s"path ${pathExp}")
    val fileStatus = fs.globStatus(new Path(pathExp))
   val paths =  fileStatus.map(fs=>{
      val path = fs.getPath.toString
      if(!path.contains("/bak")){
        val targetPath = path.substring(0,path.lastIndexOf("/")).replace("/tmp","")
        val tmpOut = targetPath.replace("hans","hans/guohao")
        s"${path}|${targetPath}|${tmpOut}"
      }else{
        null
      }
    }).filter(path=>{
      path != null
    }).toList

    if(paths.size == 0){
      println(s"path length is 0")
      System.exit(-1)
    }
    val sparkConf = new SparkConf()
    //区分大小写，方能处理SSID，ssid同时存在于一条记录的情况
    sparkConf.set("spark.sql.caseSensitive", "true")
//    sparkConf.setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val executor = Executors.newFixedThreadPool(Math.min(100,paths.size))
    val futures = paths.map(path=>{
      val callable = new ProcessCallable(sparkSession,fs,path)
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


  private class ProcessCallable(sparkSession: SparkSession,fs:FileSystem,path:String) extends Callable[String]{
    override def call(): String = {
      try {
        val srcPath =  path.split("\\|")(0)
        val targetPath =  path.split("\\|")(1)
        val targetParentPath =  targetPath.substring(0,targetPath.lastIndexOf("/"))
        val tmpOut =  path.split("\\|")(2)
        val tmpParentPath = tmpOut.substring(0,tmpOut.lastIndexOf("/"))
        val df = sparkSession.read.parquet(srcPath)
        val fieldMames = df.schema.fieldNames.toList
        if(fieldMames.contains("status")){
//          if(true){
          //移动文件
            if(!fs.exists(new Path(tmpParentPath))){
              fs.mkdirs(new Path(tmpParentPath))
            }
          val status1 = fs.rename(new Path(targetPath),new Path(tmpParentPath))
            if(status1){
              val status2 = fs.rename(new Path(srcPath),new Path(targetParentPath))
              println(s"status ... ${status2}     -> srcPath ... ${srcPath}")
            }
          s"srcPath $srcPath is sucess  ... \n targetPath is ${targetPath} \n targetParentPath is ${targetParentPath} \n tmpParentPath is ${tmpParentPath} \n status1 is ${status1} "
        }else{
          //不做
          s"$srcPath not contains status ..."
        }
      }catch {
        case e:Exception =>{
          e.printStackTrace()
          s"$path is error ..."
        }
      }
    }


  }

}
