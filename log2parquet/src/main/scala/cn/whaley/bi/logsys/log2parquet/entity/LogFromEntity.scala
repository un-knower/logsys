package cn.whaley.bi.logsys.log2parquet.entity

import com.alibaba.fastjson.JSONObject

/**
  * Created by michael on 17/6/21.
  *
  * 电视猫3.x消息实体对象【处理前】
  */
class LogFromEntity(from: JSONObject) extends JSONObject(from) {

    def msgId: String = {
        this.getString(LogFromEntity.KEY_MSG_ID)
    }

    def msgVersion: String = {
        this.getString(LogFromEntity.KEY_MSG_VERSION)
    }

    def msgSite: String = {
        this.getString(LogFromEntity.KEY_MSG_SITE)
    }

    def msgSource: String = {
        this.getString(LogFromEntity.KEY_MSG_SOURCE)
    }

    def msgFormat: String = {
        this.getString(LogFromEntity.KEY_MSG_FORMAT)
    }

    def msgSignFlag: Int = {
        this.getIntValue(LogFromEntity.KEY_MSG_SIGN_FLAG)
    }

    def msgBody: JSONObject = {
        this.getJSONObject(LogFromEntity.KEY_MSG_BODY)
    }

    def updateMsgBody(value: JSONObject): Unit = {
        if (this.containsKey(LogFromEntity.KEY_MSG_BODY)) {
            this.getJSONObject(LogFromEntity.KEY_MSG_BODY).asInstanceOf[java.util.Map[String, Object]].putAll(value)
        } else {
            this.put(LogFromEntity.KEY_MSG_BODY, value)
        }
    }


    def removeMsgBody: Unit = {
        this.remove(LogFromEntity.KEY_MSG_BODY)
    }


    def updateMsgId(value: String): Unit = {
        this.put(LogFromEntity.KEY_MSG_ID, value)
    }


    def updateMsgVersion(value: String): Unit = {
        this.put(LogFromEntity.KEY_MSG_VERSION, value)
    }


    def updateMsgSite(value: String): Unit = {
        this.put(LogFromEntity.KEY_MSG_SITE, value)
    }


    def updateMsgSource(value: String): Unit = {
        this.put(LogFromEntity.KEY_MSG_SOURCE, value)
    }


    def updateMsgFormat(value: String): Unit = {
        this.put(LogFromEntity.KEY_MSG_FORMAT, value)
    }


    def updateMsgSignFlag(value: Int): Unit = {
        this.put(LogFromEntity.KEY_MSG_SIGN_FLAG, value)
    }

    //将obj中的key对应的属性平展到obj,如果属性值不是JSONObject,则不做任何处理
    def extractObj(obj: JSONObject, key: String): JSONObject = {
        LogFromEntity.translateProp(obj, key, this, "")
    }

    def extractObj(key: String): JSONObject = {
        LogFromEntity.translateProp(this, key, this, "")
    }


    def msgBodyObj(): MsgBodyEntity = {
        new MsgBodyEntity(this.msgBody)
    }


}


object LogFromEntity {
    val KEY_APP_ID = "appId"
    val KEY_LOG_ID = "logId"
    val KEY_LOG_VERSION = "logVersion"
    val KEY_LOG_TIME = "logTime"

    //将被展开
    val KEY_LOG_BODY = "logBody"


    //保留原有json结构体
    val KEY_SYNC = "_sync"

    //下面以KEY_MSG_为前缀的字段将被放入key为_msg的json结构体内
    val KEY_MSG_LOG_SIGIN_FLAG = "logSignFlag"
    val KEY_MSG_MSG_SOURCE = "msgSource"
    val KEY_MSG_MSG_VERSION = "msgVersion"
    val KEY_MSG_MSG_SITE = "msgSite"
    val KEY_MSG_MSG_SIGN_FLAG = "msgSignFlag"
    val KEY_MSG_MSG_ID = "msgId"
    val KEY_MSG_MSG_FORMAT = "msgFormat"



    val KEY_MSG_ID = "msgId"
    val KEY_MSG_VERSION = "msgVersion"
    val KEY_MSG_SITE = "msgSite"
    val KEY_MSG_SOURCE = "msgSource"
    val KEY_MSG_FORMAT = "msgFormat"
    val KEY_MSG_SIGN_FLAG = "msgSignFlag"
    val KEY_MSG_BODY = "msgBody"

    def create(obj: JSONObject): LogFromEntity = {
        new LogFromEntity(obj)
    }

    def copy(obj: JSONObject): LogFromEntity = {
        val copyObj = new JSONObject()
        copyObj.asInstanceOf[java.util.Map[String, Object]].putAll(obj)
        new LogFromEntity(copyObj)
    }


    //转移并展开属性值
    //如果fromProp为空,则不进任何处理,直接返回to
    //如果fromProp为JSONObject,如果toKey为空,则展开fromProp到to,否则展开到toProp
    //如果fromProp为非JSONObject,则要求toKey不能为空,且将toProp设置为fromProp
    def translateProp(from: JSONObject, fromKey: String, to: JSONObject, toKey: String): JSONObject = {
        val fromProp = from.get(fromKey)
        from.remove(fromKey)

        //如果fromProp为空,则不做任何处理
        if (fromProp == null || (fromProp.isInstanceOf[String] && fromProp.asInstanceOf[String].isEmpty)) {
            return to
        }

        if (fromProp.isInstanceOf[JSONObject]) {
            if (toKey != null && !toKey.isEmpty) {
                val toProp = if (to.get(toKey) != null && to.get(toKey).isInstanceOf[JSONObject]) {
                    to.getJSONObject(toKey)
                } else {
                    new JSONObject()
                }
                toProp.asInstanceOf[java.util.Map[String, Object]].putAll(fromProp.asInstanceOf[JSONObject])
                to.put(toKey, toProp)
            } else {
                to.asInstanceOf[java.util.Map[String, Object]].putAll(fromProp.asInstanceOf[JSONObject])
            }
        } else {
            assert(toKey != null && !toKey.isEmpty)
            to.put(toKey, fromProp)
        }

        return to
    }
}

