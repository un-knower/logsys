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

  val crashAppId = Array("boikgpokn78sb95kjhfrendo8dc5mlsr","boikgpokn78sb95ktmsc1bnkechpgj9l",
    "boikgpokn78sb95k7id7n8ebqmihnjmg","boikgpokn78sb95ktmsc1bnk63fdh9g6")

}
