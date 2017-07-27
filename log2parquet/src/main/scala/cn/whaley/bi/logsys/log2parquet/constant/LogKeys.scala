package cn.whaley.bi.logsys.log2parquet.constant

/**
 * Created by michael on 2017/6/22.
 */
class LogKeys {

}

object LogKeys {

    val LOG_TYPE = "logType"
    val PRODUCT_SN = "productSN"
    val DATE = "date"
    val DATETIME = "datetime"
    val ACCOUNT_ID = "accountId"
    val DURATION = "duration"
    val SPEED = "speed"
    val SIZE = "size"
    val EVENT = "event"
    val PRE_MEMORY = "preMemory"
    val POST_MEMORY = "postMemory"
    val CONTENT_TYPE = "contentType"
    val ACCESS_AREA = "accessAera"
    val ACCESS_LOCATION = "accessLocation"
    val LOG_POST_LOGS = "logs"
    val LOG_BASEINFO = "baseInfo"
    val LOG_HOST = "host"
    val LOG_PARAMS = "params"
    val LOG_PLAYQOS = "playqos"



    //---用来log2parquet使用,start
    //----------
    val LOG_APP_ID = "appId"
    val LOG_LOG_ID = "logId"
    val LOG_LOG_VERSION = "logVersion"
    val LOG_LOG_TIME = "logTime"
    val LOG_LOG_SYNC = "_sync"
    val SVR_RECEIVE_TIME = "svr_receive_time"
    val LOG = "log"

    //平展化logBody字段
    val LOG_BODY = "logBody"

    //新建key为_msg的的json结构体，将如下字段放入json结构体中
    val LOG_SIGN_FLAG = "logSignFlag"
    val LOG_MSG_SOURCE = "msgSource"
    val LOG_MSG_VERSION = "msgVersion"
    val LOG_MSG_SITE = "msgSite"
    val LOG_MSG_SIGN_FLAG = "msgSignFlag"
    val LOG_MSG_ID = "msgId"
    val LOG_MSG_FORMAT = "msgFormat"
    //新建key,_msg
    val LOG_MSG_MSG = "_msg"

    //----------logBody里字段定义
    val LOG_BODY_LOG_TYPE = "logType"
    val LOG_BODY_EVENT = "event"
    val LOG_BODY_EVENT_ID = "eventId"
    val LOG_BODY_START_END = "start_end"
    val LOG_BODY_ACTION_ID = "actionId"
    val LOG_BODY_METHOD = "svr_req_method"







  //----------拼接输出路径使用
   //logTime计算出来的字段
    val LOG_KEY_DAY = "key_day"
    val LOG_KEY_HOUR = "key_hour"
    val LOG_OUTPUT_PATH = "output_path"
  //逻辑处理得到的字段
    val LOG_BODY_REAL_LOG_TYPE = "realLogType"




  val LOG_BODY_STACK_TRACE="STACK_TRACE"
    //---用来log2parquet使用,end

}