package cn.whaley.bi.fresh

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/**
  * Created by guohao on 2018/1/2.
  */
object ParquetSchema {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.sql.caseSensitive", "true")
    sparkConf.setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val inputPath = "/data_warehouse/ods_view.db/log_medusa_main3x_medusa_launcher_area_quit/key_day=20180102/key_hour=08"

    val df = sparkSession.read.parquet(inputPath)
    for (field <- df.schema) {
      val name = field.name
      val dataType = field.dataType
      buildFormattedString(dataType)
    }

  }


  def buildFormattedString( dataType: DataType
                      ): Unit = {
    dataType match {
      case array: ArrayType =>
        println(s"name ${dataType.typeName}")
//        dataType.typeName.
        buildFormattedArray(dataType)
      case struct: StructType =>  println(s"name ${dataType.typeName}")
      case map: MapType =>
      case _ =>
    }
  }


  def buildFormattedArray(dataType:DataType): Unit = {
    println(s"--element: ${dataType.typeName}")
  }




}
