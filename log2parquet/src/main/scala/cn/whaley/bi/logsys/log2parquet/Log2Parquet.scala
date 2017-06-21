package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.log2parquet.service.BaseClass
import cn.whaley.bi.logsys.log2parquet.utils.{Params, PathUtil}
import com.alibaba.fastjson.JSON

/**
  * Created by michael on 2017/6/13.
  */
object Log2Parquet extends BaseClass {

  override  def execute(params: Params):Unit={
    /** TODO 获得某个路径下的字段属性：黑名单、白名单或重命名
      * 从 METADATA.APPLOG_SPECIAL_FIELD_DESC 表获得信息
      * */

    //val repartitionNum = params.paramMap.getOrElse("repartitionNum", 2000)
    //加载数据
    val odsPath = PathUtil.getOdsOriginPath(params.appID, params.startDate, params.startHour)
    val rdd_original = sparkSession.sparkContext.textFile(odsPath, 2000)
    //按logType和eventId分类
    rdd_original.map(line=>{
      //
      val json = JSON.parseObject(line)

    })

//.filter(_ != null).flatMap(x => x)

    val outputPath=PathUtil.getOdsViewPath(params.appID, params.startDate, params.startHour, "logType", "eventID")




  }



}

