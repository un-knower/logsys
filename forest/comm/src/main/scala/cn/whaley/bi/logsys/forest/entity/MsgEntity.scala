package cn.whaley.bi.logsys.forest.entity

import com.alibaba.fastjson.JSONObject

/**
 * Created by fj on 16/11/8.
 *
 * 归集层消息实体对象
 */
class MsgEntity(from: JSONObject) extends JSONObject(from) {

    def msgId: String = {
        this.getString("msgId")
    }

    def msgVersion: String = {
        this.getString("msgVersion")
    }

    def msgSite: String = {
        this.getString("msgSite")
    }

    def msgSource: String = {
        this.getString("msgSource")
    }

    def msgFormat: String = {
        this.getString("msgFormat")
    }

    def msgBody: Object = {
        this.get("msgBody")
    }
}
