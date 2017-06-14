package cn.whaley.bi.logsys.log2parquet.main

import cn.whaley.bi.logsys.log2parquet.service.BaseClass
import cn.whaley.bi.logsys.log2parquet.utils.{Params, PhoenixUtils}
import org.apache.spark.sql.DataFrame
import cn.whaley.bi.logsys.log2parquet.utils.PathUtil

/**
  * Created by michael on 2017/6/13.
  */
object Log2Parquet extends BaseClass {
  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def extract(params: Params): DataFrame = {
    //val repartitionNum = params.paramMap.getOrElse("repartitionNum", 2000)
    val odsPath = PathUtil.getOdsOriginPath(params.appID, params.startDate, params.startHour)
    //val df=sparkSession.read.load(odsPath)
    val rdd_original = sparkSession.sparkContext.textFile(odsPath, 2000)
    rdd_original.map(line=>{


    })

//.filter(_ != null).flatMap(x => x)


    val test = sparkSession.read.load("")
    test
  }


  override def transform(params: Params, df: DataFrame): DataFrame = {


    df
  }

  override def load(params: Params, df: DataFrame) = {
    PathUtil.getOdsViewPath(params.appID, params.startDate, params.startHour, "logType", "eventID")

  }


}

