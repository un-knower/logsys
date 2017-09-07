package cn.whaley.bi.logsys.batchforest

import java.util

import cn.whaley.bi.logsys.batchforest.MainObj.{handleMessage, handleTime, verificationFormat}
import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}
import cn.whaley.bi.logsys.batchforest.util._
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


/**
  * Created by guohao on 2017/8/28.
  * 程序入口
  */

object MainObjTest extends NameTrait with LogTrait{

  def main(args: Array[String]): Unit = {
    try{
      val sparkConf = new SparkConf()
      sparkConf.setAppName(this.name)
      val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      val sparkContext = sparkSession.sparkContext
      //路径处理
      val inputPath = args(0)
      val key_day =args(1)
      val key_hour =args(2)
      val config = new Configuration()
      val fs = FileSystem.get(config)
      val fileStatus = fs.listStatus(new Path(s"$inputPath/key_day=${key_day}/key_hour=${key_hour}"))
      val paths = new ArrayBuffer[String]()
      fileStatus.foreach(f=>{
        val path = f.getPath.toString
        if(path.contains("boikgpokn78sb95k") && !path.contains("boikgpokn78sb95kbqei6cc98dc5mlsr")){
          paths.append(path)
        }
      })
      var inputRdd:RDD[String ] = sparkContext.textFile(paths(0))
      for(i<- 1 to paths.size -1){
        val rdd = sparkContext.textFile(paths(i))
        inputRdd = inputRdd.union(rdd)
      }
      //1.decode
      val decodeRdd = inputRdd.map(line=>{
        MsgDecoder.decode(line)
      })
      //2.验证日志格式
      val formatRdd = decodeRdd.filter(f=>{
        if(f.isEmpty){
          false
        }else{
          true
        }
      }).map(r=>{
        val line = r.get
        try{
          //非json格式数据丢弃
          val message = JSON.parseObject(line)
          verificationFormat(message)
        }catch {
          case e:Exception=>{
            // LOG.error(s"日志格式异常log=>$line ,e => $e")
            None
          }
        }
      })

//      println(s"a .... ${formatRdd.count()}")

     val result =  formatRdd.filter(f=> {
        val msgIds = Array("AAABXjZUO48KE2uqLvGbAQAB","AAABXjZUp38KE2uqLvT6YQAB","AAABXjZYt0EKEy3gA5MPVAAB","AAABXjZYtzAKEy3gA5MPRwAB")
        if(!f.isEmpty){
//          if("AAABXjZUO48KE2uqLvGbAQAB".equalsIgnoreCase(f.get.getString("msgId"))){
            if(msgIds.contains(f.get.getString("msgId"))){
            true
          }else{
            false
          }
        }else{
          false
        }

      })




      result.collect().foreach(f=>{
        println(f.get.toJSONString)
      })


    }catch {
      case e:Exception=>{
        println(e.getMessage)
      }
        throw e
    }

  }

}
