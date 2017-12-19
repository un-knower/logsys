package cn.whaley.bi.logsys.batchforest.util

/**
  * Created by guohao on 2017/12/19.
  */
object LogFields {

  /**
    * 字段白名单，log中如果定义该字段，重命名
    */
  val whiteFields = Array("logSignFlag","logVersion","msgFormat","msgId","msgSignFlag",
                            "msgSite","msgSource","msgVersion",
                            "logTime","date","day","logTimestamp","datetime","appId","logId")

}
