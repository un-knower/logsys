package cn.whaley.bi.logsys.forest.actionlog

import cn.whaley.bi.logsys.forest.entity.LogEntity
import com.alibaba.fastjson.JSONObject


/**
 * Created by fj on 16/11/10.
 */
class ActionLogPostEntity(from: JSONObject) extends LogEntity(from) {

    val host: String = {
        this.getString("host")
    }

    val url: String = {
        this.getString("url")
    }

    val method: String = {
        this.getString("method")
    }

    val contentType: String = {
        this.getString("contentType")
    }

    val realIP: String = {
        this.getString("realIP")
    }

    val receiveTime: Long = {
        this.getLong("receiveTime")
    }

    val msgSignFlag: Int = {
        this.getInteger("msgSignFlag")
    }

    val ts: String = {
        this.getString("ts")
    }

    val md5: String = {
        this.getString("md5")
    }

    def removeSignInfo(): Unit = {
        this.remove("ts")
        this.remove("md5")
        this.remove("msgSignFlag")
    }


    val version: String = {
        this.getString("version")
    }

    def baseInfo: Object = {
        this.get("baseInfo")
    }

    def removeBaseInfo(): Unit = {
        this.remove("baseInfo")
    }

    def logs: Object = {
        this.get("logs")
    }

    def removeLogs(): Unit = {
        this.remove("logs")
    }

}
