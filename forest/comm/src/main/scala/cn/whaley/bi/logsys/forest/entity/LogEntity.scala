package cn.whaley.bi.logsys.forest.entity

import com.alibaba.fastjson.JSONObject

/**
 * Created by fj on 16/11/9.
 *
 * 应用层日志消息实体对象
 */
class LogEntity(from: JSONObject) extends JSONObject(from) {

    def appId: String = {
        this.getString("appId")
    }

    def logId: String = {
        this.getString("logId")
    }

    def logVersion: String = {
        this.getString("logVersion")
    }

    def updateLogId(value: String): Unit = {
        this.put("logId", value)
    }


}

