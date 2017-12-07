import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.collection.mutable

/**
  * Created by guohao on 2017/12/6.
  * 对比 规则更改后的字段个数
  */
class CompareSchema {
  @Test
  def test(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val path1Exp = "/data_warehouse/ods_view.db/log_medusa*/key_day=20171206/key_hour=09"
    val path2Exp = "/data_warehouse/ods_view.db2/log_medusa*/key_day=20171206/key_hour=09"
    val config = new Configuration()
    val fs = FileSystem.get(config)
    val onlineSchema = getTabFields(fs, sparkSession, path1Exp)
    val testSchema = getTabFields(fs, sparkSession, path2Exp)
    println(s"onlineSchema ${onlineSchema.size}")
    println(s"testSchema ${testSchema.size}")

    testSchema.foreach(f=>{
      val tabName = f._1
      val testFields = f._2
      val onlineFields = onlineSchema.getOrElse(tabName,List[String]())
      if(testFields.size != onlineFields.size){
        System.err.println(s"$tabName testFields:${testFields.size} onlineFields:${onlineFields.size} ")
      }else{
        System.out.println(s"$tabName testFields:${testFields.size} onlineFields:${onlineFields.size} ")
      }
    })
  }

  def getTabFields(fs: FileSystem, sparkSession: SparkSession, pathExp: String): Map[String, List[String]] = {
    val fileStatus = fs.globStatus(new Path(pathExp))
    val map = mutable.Map[String, List[String]]()
    fileStatus.foreach(fs => {
      val size = fs.getBlockSize
      println(s"size ${size}")
      val path = fs.getPath.toString
      val tabName = path.split("/")(5)
      val fileds = sparkSession.read.parquet(path).schema.fieldNames.toList
      map.put(tabName,fileds)
    })
    map.toMap
  }


  @Test
  def test22(): Unit ={
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val path1Exp = "/data_warehouse/ods_view.db/log_whaleytv_main_play/key_day=20171206/key_hour=09"
    val config = new Configuration()
    val fs = FileSystem.get(config)
    val size = fs.getContentSummary(new Path(path1Exp)).getLength
    println(size/(1024*1024))
  }







}
