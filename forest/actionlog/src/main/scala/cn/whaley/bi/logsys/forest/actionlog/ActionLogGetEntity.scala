package cn.whaley.bi.logsys.forest.actionlog

import cn.whaley.bi.logsys.forest.entity.LogEntity
import com.alibaba.fastjson.JSONObject

/**
 * Created by fj on 16/11/13.
 */
class ActionLogGetEntity(from: JSONObject) extends LogEntity(from) {
    val host: String = {
        this.getString("host")
    }

    def url: String = {
        this.getString("url")
    }

    def updateUrl(url: String) = {
        this.put("url", url)
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

    val receiveTime: String = {
        this.getString("receiveTime")
    }

}
