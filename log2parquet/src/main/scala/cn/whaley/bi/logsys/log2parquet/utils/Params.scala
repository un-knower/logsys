package cn.whaley.bi.logsys.log2parquet.utils

import scala.collection.mutable

/**
  * Created by michael on 2017/6/14.
  */
case class Params (
                    appID: String = null,
                    startDate: String = null,
                    endDate: String = null,
                    startHour: String = null,
                    endHour: String = null,
                    paramMap: mutable.Map[String, String] = mutable.Map[String,String]() //其他非命令行参数
                  )