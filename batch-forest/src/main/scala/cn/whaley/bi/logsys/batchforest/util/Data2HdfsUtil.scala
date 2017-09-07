package cn.whaley.bi.logsys.batchforest.util

import java.util.UUID

import com.alibaba.fastjson.JSONObject
import org.apache.commons.compress.compressors.CompressorOutputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import scala.collection.mutable

/**
  * Created by guohao on 2017/8/31.
  */
object Data2HdfsUtil {
  def saveAsJson(rdd:RDD[JSONObject],key_day:String,key_hour:String)
                (implicit myAccumulator:MyAccumulator = new MyAccumulator): Unit ={
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val outputPath = s"/data_warehouse/ods_origin.db/log_origin"
    val jsonDir = s"${outputPath}"
    val tmpDir = s"${outputPath}/${key_day}${key_hour}_tmp"
    fs.mkdirs(new Path((tmpDir)))
    //重复执行删除历史记录
    val deletePath = s"$outputPath/key_appId2=*/key_day=${key_day}/key_hour=${key_hour}"
    fs.deleteOnExit(new Path(deletePath))
//    rdd.repartition(20)
    rdd.foreachPartition(partition=>{
        val partId = UUID.randomUUID().toString
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        //写临时文件
        val fsMap = new mutable.HashMap[String,(Path,Path,FSDataOutputStream, CompressorOutputStream,Long)]()
        partition.foreach(log=>{
          myAccumulator.add("outputRecord")
          var appId = log.getString("appId")
//          if(appId == null){
//            appId = "boikgpokn78sb95k0000000000000000"
//            log.put("appId",appId)
//          }
          val outpathPath = s"${tmpDir}/${appId}"
          val info = fsMap.getOrElseUpdate(outpathPath,{
            val tmpFilePath = new Path(s"${outpathPath}/${partId}.json.bz2")
            val jsonFilePath = new Path(s"${jsonDir}/key_appId2=${appId}/key_day=${key_day}/key_hour=${key_hour}/${appId}_${partId}.json.bz2")
            fs.createNewFile(tmpFilePath)
            val stream = fs.append(tmpFilePath)
            val compressionOutputStream = new BZip2CompressorOutputStream(stream)
            (tmpFilePath, jsonFilePath,stream,compressionOutputStream, System.currentTimeMillis())
          })
          val stream = info._4
          val bytes = log.toJSONString.getBytes("utf-8")
          stream.write(bytes)
          stream.write('\n')
        })

        //关闭临时文件，并移动到json目录
        fsMap.foreach(item=>{
          val tmpFilePath = item._2._1
          val jsonFilePath = item._2._2
          val stream = item._2._3
          val compressionOutputStream = item._2._4
          compressionOutputStream.close()
          stream.close()
          val jsonFileDir = jsonFilePath.getParent
          if (!fs.exists(jsonFileDir)) {
            fs.mkdirs(jsonFileDir)
          }
          fs.rename(tmpFilePath, jsonFilePath)
        })
      })
      fs.deleteOnExit(new Path(tmpDir))
  }

}
