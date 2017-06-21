package cn.whaley.bi.logsys.log2parquet.entity

import com.alibaba.fastjson.JSONObject

/**
 * Created by fj on 17/5/2.
 */
class PostMsgBodyEntity(from: JSONObject) extends MsgBodyEntity(from) {

    class PostBody(from: JSONObject) extends JSONObject(from) {

        def ts(): String = {
            this.getString(PostMsgBodyEntity.KEY_FOR_BODY_TS)
        }

        def md5(): String = {
            this.getString(PostMsgBodyEntity.KEY_FOR_BODY_MD5)
        }

        def removeSignInfo(): Unit = {
            this.remove(PostMsgBodyEntity.KEY_FOR_BODY_TS)
            this.remove(PostMsgBodyEntity.KEY_FOR_BODY_MD5)
        }


        def version(): String = {
            this.getString(PostMsgBodyEntity.KEY_FOR_BODY_VERSION)
        }

        def baseInfo(): Object = {
            this.get(PostMsgBodyEntity.KEY_FOR_BODY_BASE_INFO)
        }

        def removeBaseInfo(): Unit = {
            this.remove(PostMsgBodyEntity.KEY_FOR_BODY_BASE_INFO)
        }

        def logs(): Object = {
            this.get(PostMsgBodyEntity.KEY_FOR_BODY_LOGS)
        }

        def removeLogs(): Unit = {
            this.remove(PostMsgBodyEntity.KEY_FOR_BODY_LOGS)
        }
    }

    def bodyObj(): PostBody = {
        new PostBody(this.body.asInstanceOf[JSONObject])
    }

    //平展body
    override def normalize(): Seq[JSONObject] = {
        val logs = super.normalize()
        // 平展化params信息
        logs.map(log => {
            if (log.containsKey(PostMsgBodyEntity.KEY_FOR_BODY_PARAMS)) {
                // 处理params非json格式的情形
                val paramsV = log.get(PostMsgBodyEntity.KEY_FOR_BODY_PARAMS)
                if (paramsV.isInstanceOf[String]) {
                    val paramsInfo = paramsV.asInstanceOf[String]
                    if (paramsInfo.contains(",")) {
                        val parmasSplit = paramsInfo.split(",")
                        if (parmasSplit.length >= 1) {
                            (0 until parmasSplit.length).foreach(i => {
                                val parameterInfo = parmasSplit(i).trim
                                if (parameterInfo.contains("=")) {
                                    val parameterSplit = parameterInfo.split("=")
                                    if (parameterSplit.length == 2) {
                                        log.put(parameterSplit(0), parameterSplit(1))
                                    }
                                }
                            })
                        }
                    }
                } else if (paramsV.isInstanceOf[JSONObject]) {
                    val paramsInfo = paramsV.asInstanceOf[JSONObject]
                    log.asInstanceOf[java.util.Map[String, Object]].putAll(paramsInfo)
                }
                log.remove(PostMsgBodyEntity.KEY_FOR_BODY_PARAMS)
            }
            log
        })
    }
}


object PostMsgBodyEntity {
    val KEY_FOR_BODY_PARAMS = "params"
    val KEY_FOR_BODY_TS = "ts"
    val KEY_FOR_BODY_MD5 = "md5"
    val KEY_FOR_BODY_VERSION = "version"
    val KEY_FOR_BODY_BASE_INFO = "baseInfo"
    val KEY_FOR_BODY_LOGS = "logs"
}