package cn.whaley.bi.logsys.log2parquet.utils

import java.io.File
import java.util.{Date, UUID}
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.util.regex.Pattern

import cn.whaley.bi.logsys.log2parquet.constant.Constants
import cn.whaley.bi.logsys.metadata.entity.LogBaseInfoEntity
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

    def saveAsParquet(logRdd: RDD[(String, JSONObject)],time:String,sparkSession: SparkSession, appLogSecialEntities:Seq[(String, String, String, String, Int)],baseInfoEntities:List[LogBaseInfoEntity]) = {
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        //time字段，用来多个azkaban任务一起运行时，区分不同任务写入的目录
        val outputPathTmp = s"${Constants.ODS_VIEW_HDFS_OUTPUT_PATH_TMP}${File.separator}${time}"
        val jsonDir = s"${outputPathTmp}_json"
        val tmpDir = s"${outputPathTmp}_tmp"
        //运行任务之前重建临时文件目录

        if(fs.exists(new Path(jsonDir))){
            fs.delete(new Path(jsonDir),true)
        }
        if(fs.exists(new Path(tmpDir))){
            fs.delete(new Path(tmpDir),true)
        }

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
            val callable = new ProcessCallable(group._1, group._2, group._3, sparkSession, fs, null,appLogSecialEntities,baseInfoEntities)
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

        //删除json文件
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
       }*/

     /* if (!preserveJsonDir) {
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

private class ProcessCallable(inputSize: Long, inputPath: String, outputPath: String, sparkSession: SparkSession, fs: FileSystem, paramMap: Map[String, String], appLogSecialEntities:Seq[(String, String, String, String, Int)],baseInfoEntities:List[LogBaseInfoEntity]) extends Callable[String] {
    override def call(): String = {
        val tsFrom = System.currentTimeMillis()
        try {
            //json文件分片计算
            val jsonSplitSize = (1000 * 1024 * 1024).toLong
            val jsonSplitNum = Math.ceil(inputSize.toDouble / jsonSplitSize.toDouble).toInt
            //jons转parquet
            println(s"process file[${jsonSplitNum},${jsonSplitSize}]: $inputPath -> $outputPath")
            val tabName = outputPath.split("/")(3)
            val productCode = tabName.split("_")(1)
            val df = sparkSession.read.json(inputPath)
            val fieldList = df.schema.map(schema=>schema.name).toList
            val (fieldSchema,flag) = getDfSchema(fieldList,tabName,productCode,appLogSecialEntities,baseInfoEntities)
            df.selectExpr(fieldSchema:_*).coalesce(jsonSplitNum).write.mode(SaveMode.Overwrite).parquet(outputPath)
            //删除输入目录
            if(flag){
                fs.delete(new Path(inputPath), true)
            }
            val outputSize = fs.getContentSummary(new Path(outputPath)).getLength
            s"convert file: $inputPath(${inputSize / 1024}k) -> $outputPath(${outputSize / 1024}k),ts:${System.currentTimeMillis() - tsFrom}"
        }
        catch {
            case e: Throwable => {
                SendMail.post(e, s"[Log2Parquet new][Json2ParquetUtil][${inputPath}]任务执行失败", Array("app-bigdata@whaley.cn"))
                e.printStackTrace()
                println( s"convert file error[$inputPath(${inputSize / 1024}k) -> $outputPath)] : paramMap -> null, exception -> ${e.getMessage}")
                s"convert file error[$inputPath(${inputSize / 1024}k) -> $outputPath)] : paramMap -> null, exception -> ${e.getMessage}"
            }
        }

    }
    def getDfSchema(list:List[String],tableName:String,productCode:String, appLogSecialEntities:Seq[(String, String, String, String, Int)],baseInfoEntities:List[LogBaseInfoEntity]): (List[String],Boolean) ={
        //先rename 在filter
        var fields = list
        //该产品下或者全日志或者表级别
        val appLogSpecialFieldEntities = appLogSecialEntities.filter(entity=>entity._1 == productCode || entity._1 == "log" || entity._1 == tableName )
        //字段重命名: Seq[(源字段名,字段目标名)]
        //1.baseinfo 白名单 重命名
        val baseInfoFieldMap = mutable.Map[String,String]()
        val baseInfoFields = baseInfoEntities.filter(item=>{
            item.isDeleted == false && item.getProductCode == productCode
        }).map(item => item.getFieldName)
        baseInfoFields.foreach(baseInfoField=>{
            fields.foreach(field=>{
                //字段只有忽略忽略大小写在baseInfo中，需要重命名
                if(!baseInfoField.equals(field) && baseInfoField.equalsIgnoreCase(field)){
                    baseInfoFieldMap.put(field,field+"_r")
                }
            })
        })
        fields = fields.filter(field => !baseInfoFieldMap.keySet.contains(field))
//        println(s"baseInfoFieldMap ${baseInfoFieldMap}")
        //2.黑名单重命名
        //2.1表级别
        val tabRenameMap= handleRename(appLogSpecialFieldEntities,fields,tableName)
        fields = fields.filter(field => !tabRenameMap.keySet.contains(field))
//        println(s"tabRenameMap ${tabRenameMap}")
        //2.2产品线级别
        val productRenameMap= handleRename(appLogSpecialFieldEntities,fields,productCode)
        fields = fields.filter(field => !productRenameMap.keySet.contains(field))
//        println(s"productRenameMap ${productRenameMap}")
        //2.3全局级别
        val logRenameMap= handleRename(appLogSpecialFieldEntities,fields,"log")
        fields = fields.filter(field => !productRenameMap.keySet.contains(field))
//        println(s"logRenameMap ${logRenameMap}")
        val whiteRenameMap = baseInfoFieldMap.toMap ++ tabRenameMap ++ productRenameMap ++ logRenameMap
//        println(s"whiteRenameMap ${whiteRenameMap}")
        //字段过滤器: Seq[源字段名]
        //字段删除黑名单
        val fieldFilterList = appLogSpecialFieldEntities.filter(conf => conf._3 == "fieldFilter").flatMap(conf => {
            val specialValue = conf._4
            val fieldPattern = if (specialValue.charAt(0) == '1') {
                Pattern.compile(conf._2, Pattern.CASE_INSENSITIVE)
            }
            else {
                Pattern.compile(conf._2)
            }
            val isReserve = specialValue.charAt(1) == '0'
            fields.filter(field => fieldPattern.matcher(field).find()).map(field => (field, isReserve))
        })
        //剔除白名单字段
        val whiteList = fieldFilterList.filter(_._2)
        val fieldBlackFilter = fieldFilterList.filter(item => {
            val field = item._1
            !whiteList.exists(p => p._1 == field)
        }).map(item => item._1)
      if(fieldBlackFilter.size> 0){
        println("字段删除 :"+fieldBlackFilter)
      }
        fields = fields.filter(field=> !fieldBlackFilter.contains(field))
        //处理fields中相同的字段
        val set = fields.toStream.map(field=>field.toLowerCase).toSet
        //字段列表中含有字段模糊，需要重命名
        val renameMap = mutable.Map[String,String]()
        if(fields.size != set.size){
            val dealRenameList =  fields
            var i = 1
            dealRenameList.foreach(dealField=>{
                fields = fields.filter(field=>{
                    val flag = !field.equals(dealField) && field.equalsIgnoreCase(dealField)
                    if(flag && !whiteRenameMap.values.toSet.contains(field)){
                        renameMap.put(field,s"${field}_${i}")
                        i=i+1
                        false
                    }else{
                        true
                    }
                })
            })
        }
        //flag 中间结果json文件是否删除，如果没有重命名，怎删除为true
        var flag = true
        if(renameMap.size !=0 ){
            flag=false
            val e = new RuntimeException(renameMap.toString())
            SendMail.post(e, s"[Log2Parquet new][Json2ParquetUtil][${inputPath}] field rename", Array("app-bigdata@whaley.cn"))
        }
//        val allRenameMap = whiteRenameMap ++ renameMap.toMap
        val allRenameMap = whiteRenameMap
        //把重命名字段做到field list中
        allRenameMap.foreach(f=>{
            val oldName = f._1
            val newName = f._2
          //newName如存在原始的list中需要特殊处理
          if(!fields.contains(newName)){
            fields = fields.::(s"`$oldName` as `$newName`")
          }else{
            fields = fields.filter(field=>field !=newName)
            fields = fields.::(s"(case when `$newName` is null or `$newName` == '' then `$oldName` else `$newName` end ) as `$newName`")
          }
        })
        (fields,flag)
    }


    /**
      * 处理重命名
      * @param appLogSpecialFieldEntities
      * @param fields
      * @param logPathReg
      * @return renameMap 处理重命名，leftFields剩下的字段
      */
    def handleRename(appLogSpecialFieldEntities:Seq[(String, String, String, String, Int)],fields:List[String],logPathReg:String): Map[String,String] ={
        val renameMap = appLogSpecialFieldEntities.filter(conf => conf._3 == "rename" && conf._1 == logPathReg).flatMap(conf => {
            fields.filter(field => conf._2.r.findFirstMatchIn(field).isDefined).map(field => (field, conf._4))
        }).toMap
        renameMap
    }

}
