package cn.whaley.bi.logsys.metadataManage.common

/**
  * Created by guohao on 2017/11/6.
  */
case class Params(path:String="",
                  db_name:String="",
                  tab_prefix:String="",
                  product_code:String="",
                  app_code:String="",
                  realLogType:String="",
                  key_day:String="",
                  key_hour:String="",
                  offset:String="",
                  deleteOld:String="",
                  paramMap:Map[String,String] =  Map[String,String]()) {

}
