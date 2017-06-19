package cn.whaley.bi.logsys.log2parquet.service

import cn.whaley.bi.logsys.log2parquet.utils.{DateFormatUtils, ParamsParseUtil, Params}
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.{SparkConf}

/**
  * Created by michael on 2017/6/14.
  */
trait BaseClass {
  val config = new SparkConf()
  var sparkSession:SparkSession=null

  /**
    * 程序入口
    * @param args
    */
  def main(args: Array[String]) {
    println("init start ....")
    init()
    println("init success ....")

    println("execute start ....")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        if (p.startDate != null&&p.startHour!=null) {
          val appID = p.appID
          var date = p.startDate
          var hour = p.startHour
          p.paramMap.put("appID", appID)
          p.paramMap.put("date", date)
          p.paramMap.put("hour", hour)
          execute(p)
          while (p.endDate != null && date < p.endDate) {
            //todo need to handle data loop
            date = DateFormatUtils.enDateAdd(date, 1)
            hour = DateFormatUtils.enHourAdd(hour, 1)
            p.paramMap.put("date", date)
            p.paramMap.put("hour", hour)
            execute(p)
          }
        } else {
          execute(p)
        }
      }
      case None => {
        throw new RuntimeException("parameters wrong")
      }
    }
    println("execute end ....")
    destroy()
  }

  /**
    * 全局变量初始化
    */
  def init(): Unit = {
    //sparkSession=SparkSession.builder().config(config).enableHiveSupport.getOrCreate()
    sparkSession=SparkSession.builder().config(config).getOrCreate()

  }


  /**
    * release resource
    */
  def destroy(): Unit = {
    if (sparkSession != null) {
      sparkSession.close()
     }
  }

  /**
    * this method do not complete.Sub class that extends BaseClass complete this method
    */
  def execute(params: Params): Unit


}
