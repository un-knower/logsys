package cn.whaley.bi.logsys.batchforest.util

import java.io.OutputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}

import com.alibaba.fastjson.JSONObject
import net.jpountz.lz4.{LZ4BlockOutputStream, LZ4Factory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by guohao on 2017/8/31.
  */
object Data2HdfsAsLz4Util {
  def saveAsJson(rdd:RDD[JSONObject],key_day:String,key_hour:String)
                (implicit myAccumulator:MyAccumulator = new MyAccumulator): Unit ={
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val outputPath = s"/data_warehouse/ods_origin.db/log_origin"
    val jsonDir = s"${outputPath}"
    val tmpDir = s"${outputPath}/${key_day}${key_hour}_tmp"
    fs.mkdirs(new Path((tmpDir)))
    //重复执行删除历史记录,
    val deletePath = s"$outputPath/key_appId2=*/key_day=${key_day}/key_hour=${key_hour}/"
    if(fs.exists(new Path(deletePath))){
      fs.delete(new Path(deletePath),true)
    }
    rdd.repartition(40).
      foreachPartition(partition=>{
        val partId = UUID.randomUUID().toString
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        //写临时文件
        val fsMap = new mutable.HashMap[String,(Path,Path,FSDataOutputStream, OutputStream,Long)]()
        partition.foreach(log=>{
          myAccumulator.add("outputRecord")
          val appId = log.getString("appId")
          //根据logTime切割输出路径
          val logTime = log.getLong("logTime")
          val date = new Date()
          date.setTime(logTime)
          val outpathPath = s"${tmpDir}/${appId}"
          val info = fsMap.getOrElseUpdate(outpathPath,{
            val tmpFilePath = new Path(s"${outpathPath}/${partId}.json.lz4")
            val jsonFilePath = new Path(s"${jsonDir}/key_appId2=${appId}/key_day=${key_day}/key_hour=${key_hour}/${appId}_${partId}.json.lz4")
            fs.createNewFile(tmpFilePath)
            val stream = fs.append(tmpFilePath)

            val factory = LZ4Factory.fastestInstance
            val lz4Compressor = factory.fastCompressor()
            val compressionOutputStream = new LZ4BlockOutputStream(stream,8192,lz4Compressor)
            (tmpFilePath, jsonFilePath,stream,compressionOutputStream, System.currentTimeMillis())
          })
          val compressionOutputStream = info._4
          val bytes = log.toJSONString.getBytes("utf-8")
          compressionOutputStream.write(bytes)
          compressionOutputStream.write('\n')
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

    if(fs.exists(new Path(tmpDir))){
      fs.delete(new Path(tmpDir),true)
    }
  }

  def dateFormat(date:String,offset: Int = 0):String={
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyyMMddHH")
    cal.setTime(sdf.parse(date))
    cal.add(Calendar.HOUR_OF_DAY,offset)
    sdf.format(cal.getTime)
  }

}
