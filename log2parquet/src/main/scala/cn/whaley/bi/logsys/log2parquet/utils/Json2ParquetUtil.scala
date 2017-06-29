package cn.whaley.bi.logsys.log2parquet.utils

import java.util.UUID
import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.collection.mutable

/**
 * Created by kassell on 2017/1/19.
 */
object Json2ParquetUtil {

    def saveAsParquet(logRdd: RDD[(String, String)], sqlContext: SQLContext /*,params: Params, outputDate: String*/) = {
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        /*********  写json文件阶段 ************/
        logRdd.foreachPartition(iter => {
            val partId = UUID.randomUUID().toString
            println(s"process partition $partId")
            val conf = new Configuration()
            val fs = FileSystem.get(conf)

            //写临时文件
            val fsMap = new mutable.HashMap[String, (Path, Path, FSDataOutputStream, Long)]()
            iter.foreach(item => {
                val outputPath = item._1
                val logData = item._2
                val info = fsMap.getOrElseUpdate(outputPath, {
                    val tmpFilePath = new Path(s"${outputPath}/tmp/${partId}.tmp")
                    val jsonFilePath = new Path(s"${outputPath}/tmp/${partId}.json")

                    //如果文件存在则删除文件
                    if(fs.exists(tmpFilePath)){
                        fs.delete(tmpFilePath,true)
                    }
                    if(fs.exists(jsonFilePath)){
                        fs.delete(jsonFilePath,true)
                    }

                    fs.createNewFile(tmpFilePath)
                    val stream = fs.append(tmpFilePath)
                    val ts = System.currentTimeMillis()
                    println(s"create tmp file[${ts}]: $tmpFilePath")
                    (tmpFilePath, jsonFilePath, stream, System.currentTimeMillis())
                })
                val stream = info._3
                val bytes = logData.getBytes("utf-8")
                stream.write(bytes)
                stream.write('\n')
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

        //按照logType对文件进行分组

        val outputPathRdd=logRdd.map(e=>e._1).distinct()
        val fileGroups= outputPathRdd.map(outputPath=>{
            val inPath=s"${outputPath}/tmp"
            val logTypeSize = fs.getContentSummary(new Path(outputPath)).getLength
            (logTypeSize, inPath, outputPath)
        })



       /* val status = fs.listStatus(new Path(s"$jsonDir"))
        val fileGroups = status.map(item => {
            val logType = item.getPath.getName
            val logTypeSize = fs.getContentSummary(item.getPath).getLength
            val inPath = s"$jsonDir/${logType}"
            val outPath = s"$outputPathValue/$logType"
            (logTypeSize, inPath, outPath)
        })*/

        println("all files:")
        fileGroups.foreach(println)

        //val jsonProcCount = params.paramMap.getOrElse("jsonProcCount", Math.min(100, fileGroups.length).toString).toInt
        //val jsonProcCount =Math.min(100, fileGroups.count())
        val executor = Executors.newFixedThreadPool(50)
        //每类logType用一个任务处理
        val futures = fileGroups.filter(_._1 > 0).map(group => {
            println(s"submit task : $group")
            val callable = new ProcessCallable(group._1, group._2, group._3, sqlContext, fs, null)
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

        //删除json文件 TODO
        /*val preserveJsonDir = false
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
       }*/

    }
}

private class ProcessCallable(inputSize: Long, inputPath: String, outputPath: String, sqlContext: SQLContext, fs: FileSystem, paramMap: Map[String, String]) extends Callable[String] {
    override def call(): String = {
        val tsFrom = System.currentTimeMillis()

        try {
            //json文件分片计算
           // val jsonSplitSize = paramMap.getOrElse("jsonSplitSize", (128 * 1024 * 1024).toString).toLong
            val jsonSplitSize = (128 * 1024 * 1024).toLong
            val jsonSplitNum = Math.ceil(inputSize.toDouble / jsonSplitSize.toDouble).toInt
            //jons转parquet
            println(s"process file[${jsonSplitNum},${jsonSplitSize}]: $inputPath -> $outputPath")
            sqlContext.read.json(inputPath).coalesce(jsonSplitNum).write.mode(SaveMode.Overwrite).parquet(outputPath)
            println(s"write file: $outputPath")
            //删除输入目录
            // TODO fs.delete(new Path(inputPath), true)
            val outputSize = fs.getContentSummary(new Path(outputPath)).getLength
            s"convert file: $inputPath(${inputSize / 1024}k) -> $outputPath(${outputSize / 1024}k),ts:${System.currentTimeMillis() - tsFrom}"

        }
        catch {
            case e: Throwable => {
                //SendMail.post(e, s"[Log2Parquet][Json2ParquetUtil][${inputPath}]任务执行失败", Array("app-bigdata@whaley.cn"))
                e.printStackTrace()
                s"convert file error[$inputPath(${inputSize / 1024}k) -> $outputPath)] : paramMap -> null, exception -> ${e.getMessage}"
            }
        }
    }
}
