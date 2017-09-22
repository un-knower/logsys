package cn.whaley.bi.logsys.batchforest

import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



/**
  * Created by guohao on 2017/8/28.
  * 程序入口
  */

object MainObj2 extends NameTrait with LogTrait{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.name)
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    val inputPath1 = "/data_warehouse/ods_view.db/log_medusa_main3x_*/key_day=20170918/*"
    val inputPath2 = "/data_warehouse/ods_view.db/log_medusa_main3x_default/key_day=20170918/*"
    val inputPath3 = "/data_warehouse/ods_view.db/log_whaleytv_main_*/key_day=20170918/*"
    val inputPath4 = "/data_warehouse/ods_view.db/log_whaleytv_main_default/key_day=20170918/*"
    val cn1 = sparkContext.textFile(inputPath1).count()
    println(s"cn1 $cn1")
    val cn2 = sparkContext.textFile(inputPath2).count()
    println(s"cn2 $cn2")
    val cn3 = sparkContext.textFile(inputPath3).count()
    println(s"cn3 $cn3")
    val cn4 = sparkContext.textFile(inputPath4).count()
    println(s"cn4 $cn4")
  }

}
