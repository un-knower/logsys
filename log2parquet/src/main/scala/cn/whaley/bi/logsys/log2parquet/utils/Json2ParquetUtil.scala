package cn.whaley.bi.logsys.log2parquet.utils

import java.io.File
import java.util.{Date, UUID}
import java.util.concurrent.{Callable, Executors, TimeUnit}

import cn.whaley.bi.logsys.log2parquet.constant.Constants
import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

import scala.collection.mutable

/**
 * Created by michael on 2017/7/2.
 */
object Json2ParquetUtil {

    def saveAsParquet(logRdd: RDD[(String, JSONObject)],time:String,sparkSession: SparkSession) = {
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        //time字段，用来多个azkaban任务一起运行时，区分不同任务写入的目录
        val outputPathTmp = s"${Constants.ODS_VIEW_HDFS_OUTPUT_PATH_TMP}${File.separator}${time}"
        val jsonDir = s"${outputPathTmp}_json"
        val tmpDir = s"${outputPathTmp}_tmp"
        //运行任务之前重建临时文件目录
        fs.deleteOnExit(new Path(jsonDir))
        fs.deleteOnExit(new Path(tmpDir))
        if(!fs.exists(new Path(jsonDir))){
            fs.mkdirs(new Path(jsonDir))
        }
        if(!fs.exists(new Path(tmpDir))){
            fs.mkdirs(new Path(tmpDir))
        }
        /*********  写json文件阶段 ************/
        logRdd.repartition(50).foreachPartition(iter => {
            val partId = UUID.randomUUID().toString
            println(s"process partition $partId")
            val conf = new Configuration()
            val fs = FileSystem.get(conf)

            //写临时文件
            val fsMap = new mutable.HashMap[String, (Path, Path, FSDataOutputStream, Long)]()
            iter.foreach(item => {
                val outputPath = item._1.replace(File.separator,"#")
                val logData = item._2
                if(null!=logData && !logData.isEmpty){
                val info = fsMap.getOrElseUpdate(outputPath, {
                    val tmpFilePath = new Path(s"${tmpDir}/${outputPath}/${partId}.tmp")
                    val jsonFilePath = new Path(s"${jsonDir}/${outputPath}/${partId}.json")
                    fs.createNewFile(tmpFilePath)
                    val stream = fs.append(tmpFilePath)
                    val ts = System.currentTimeMillis()
                    println(s"create tmp file[${ts}]: $tmpFilePath")
                    (tmpFilePath, jsonFilePath, stream, System.currentTimeMillis())
                })
                val stream = info._3
                val bytes = logData.toJSONString.getBytes("utf-8")
                stream.write(bytes)
                stream.write('\n')
                }
            })
            //关闭临时文件，并移动到json目录
            fsMap.foreach(item => {
                val tmpFilePath = item._2._1
                val jsonFilePath = item._2._2
                val stream = item._2._3
                stream.close()

                val tmpSize = fs.getContentSummary(tmpFilePath).getLength
                val jsonFileDir = jsonFilePath.getParent
                if (!fs.exists(jsonFileDir)) {
                    fs.mkdirs(jsonFileDir)
                }
                fs.rename(tmpFilePath, jsonFilePath)
                val ts = System.currentTimeMillis() - item._2._4
                println(s"finished tmp file[${ts},${tmpSize / 1024L}k]: $tmpFilePath -> $jsonFilePath")
            })
        })

        /** *******  写parquet文件阶段 ************/

        //按照outputPathType对文件进行分组
        val status = fs.listStatus(new Path(s"$jsonDir"))
        val fileGroups = status.map(item => {
            val outputPathType = item.getPath.getName
            val outputPathTypeSize = fs.getContentSummary(item.getPath).getLength
            val inPath = s"$jsonDir/${outputPathType}"
            val outPath = Constants.DATA_WAREHOUSE + File.separator + outputPathType.replace("#",File.separator)
            (outputPathTypeSize, inPath, outPath)
        })

        println("fileGroups size:"+fileGroups.length+",all files:")
        fileGroups.foreach(println)

        val executor = Executors.newFixedThreadPool(Math.min(100,fileGroups.size))
        //每类outputPathType用一个任务处理
        val futures = fileGroups.filter(_._1 > 0).map(group => {
            println(s"submit task : $group")
            val callable = new ProcessCallable(group._1, group._2, group._3, sparkSession, fs, null)
            executor.submit(callable)
        })
        //等待执行结果
        //val jsonProcTimeOut = params.paramMap.getOrElse("jsonProcTimeOut", "900").toInt
        val jsonProcTimeOut = 3600
        futures.foreach(future => {
            val ret = future.get(jsonProcTimeOut, TimeUnit.SECONDS)
            println(ret)
        })
        //关闭线程池
        println("shutdown....")
        executor.shutdown()
        executor.awaitTermination(1, TimeUnit.MINUTES)
        println("shutdown.")

        //删除json文件
        val preserveJsonDir = false
        val preserveTmpDir = false
       if (!preserveTmpDir) {
           fs.delete(new Path(tmpDir), true)
           println(s"delete dir:$tmpDir")
       }else{
           val files=fs.listStatus(new Path(tmpDir))
           if(files.length==0){
               fs.delete(new Path(tmpDir), true)
               println(s"delete empty dir:$tmpDir")
           }
       }

      if (!preserveJsonDir) {
           fs.delete(new Path(jsonDir), true)
           println(s"delete dir:$jsonDir")
       }else{
           val files=fs.listStatus(new Path(jsonDir))
           if(files.length==0){
               fs.delete(new Path(jsonDir), true)
               println(s"delete empty dir:$jsonDir")
           }
       }
    }
}

private class ProcessCallable(inputSize: Long, inputPath: String, outputPath: String, sparkSession: SparkSession, fs: FileSystem, paramMap: Map[String, String]) extends Callable[String] {
    override def call(): String = {
        val tsFrom = System.currentTimeMillis()
        try {
            //json文件分片计算
           // val jsonSplitSize = paramMap.getOrElse("jsonSplitSize", (128 * 1024 * 1024).toString).toLong
            val jsonSplitSize = (128 * 1024 * 1024).toLong
            val jsonSplitNum = Math.ceil(inputSize.toDouble / jsonSplitSize.toDouble).toInt
            //jons转parquet
            println(s"process file[${jsonSplitNum},${jsonSplitSize}]: $inputPath -> $outputPath")
            sparkSession.read.json(inputPath).coalesce(jsonSplitNum).write.mode(SaveMode.Overwrite).parquet(outputPath)
            println(s"write file: $outputPath")
            //删除输入目录
            //fs.delete(new Path(inputPath), true)
            val outputSize = fs.getContentSummary(new Path(outputPath)).getLength
            s"convert file: $inputPath(${inputSize / 1024}k) -> $outputPath(${outputSize / 1024}k),ts:${System.currentTimeMillis() - tsFrom}"

        }
        catch {
            case e: Throwable => {
                SendMail.post(e, s"[Log2Parquet][Json2ParquetUtil][${inputPath}]任务执行失败", Array("app-bigdata@whaley.cn"))
                e.printStackTrace()
                println( s"convert file error[$inputPath(${inputSize / 1024}k) -> $outputPath)] : paramMap -> null, exception -> ${e.getMessage}")
                s"convert file error[$inputPath(${inputSize / 1024}k) -> $outputPath)] : paramMap -> null, exception -> ${e.getMessage}"
            }
        }

    }
}
