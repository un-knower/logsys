package cn.whaley.bi.logsys.forest.entity

import com.alibaba.fastjson.JSONObject

/**
 * Created by fj on 16/11/8.
 *
 * 归集层消息实体对象
 */
class MsgEntity(from: JSONObject) extends JSONObject(from) {

    def msgId: String = {
        this.getString(MsgEntity.KEY_MSG_ID)
    }

    def msgVersion: String = {
        this.getString(MsgEntity.KEY_MSG_VERSION)
    }

    def msgSite: String = {
        this.getString(MsgEntity.KEY_MSG_SITE)
    }

    def msgSource: String = {
        this.getString(MsgEntity.KEY_MSG_SOURCE)
    }

    def msgFormat: String = {
        this.getString(MsgEntity.KEY_MSG_FORMAT)
    }

    def msgSignFlag: Int = {
        this.getIntValue(MsgEntity.KEY_MSG_SIGN_FLAG)
    }

    def msgBody: JSONObject = {
        this.getJSONObject(MsgEntity.KEY_MSG_BODY)
    }

    def updateMsgBody(value: JSONObject): Unit = {
        if (this.containsKey(MsgEntity.KEY_MSG_BODY)) {
            this.getJSONObject(MsgEntity.KEY_MSG_BODY).asInstanceOf[java.util.Map[String, Object]].putAll(value)
        } else {
            this.put(MsgEntity.KEY_MSG_BODY, value)
        }
    }


    def removeMsgBody: Unit = {
        this.remove(MsgEntity.KEY_MSG_BODY)
    }


    def updateMsgId(value: String): Unit = {
        this.put(MsgEntity.KEY_MSG_ID, value)
    }


    def updateMsgVersion(value: String): Unit = {
        this.put(MsgEntity.KEY_MSG_VERSION, value)
    }


    def updateMsgSite(value: String): Unit = {
        this.put(MsgEntity.KEY_MSG_SITE, value)
    }


    def updateMsgSource(value: String): Unit = {
        this.put(MsgEntity.KEY_MSG_SOURCE, value)
    }


    def updateMsgFormat(value: String): Unit = {
        this.put(MsgEntity.KEY_MSG_FORMAT, value)
    }


    def updateMsgSignFlag(value: Int): Unit = {
        this.put(MsgEntity.KEY_MSG_SIGN_FLAG, value)
    }

    //将obj中的key对应的属性平展到obj,如果属性值不是JSONObject,则不做任何处理
    def extractObj(obj: JSONObject, key: String): JSONObject = {
        MsgEntity.translateProp(obj, key, this, "")
    }

    def extractObj(key: String): JSONObject = {
        MsgEntity.translateProp(this, key, this, "")
    }


    def msgBodyObj(): MsgBodyEntity = {
        new MsgBodyEntity(this.msgBody)
    }


}


object MsgEntity {
    val KEY_MSG_ID = "msgId"
    val KEY_MSG_VERSION = "msgVersion"
    val KEY_MSG_SITE = "msgSite"
    val KEY_MSG_SOURCE = "msgSource"
    val KEY_MSG_FORMAT = "msgFormat"
    val KEY_MSG_SIGN_FLAG = "msgSignFlag"
    val KEY_MSG_BODY = "msgBody"

    def create(obj: JSONObject): MsgEntity = {
        new MsgEntity(obj)
    }

    def copy(obj: JSONObject): MsgEntity = {
        val copyObj = new JSONObject()
        copyObj.asInstanceOf[java.util.Map[String, Object]].putAll(obj)
        new MsgEntity(copyObj)
    }


    //转移并展开属性值
    //如果fromProp为空,则不进任何处理,直接返回to
    //如果fromProp为JSONObject,如果toKey为空,则展开fromProp到to,否则展开到toProp
    //如果fromProp为非JSONObject,则要求toKey不能为空,且将toProp设置为fromProp
    def translateProp(from: JSONObject, fromKey: String, to: JSONObject, toKey: String): JSONObject = {
        val fromProp = from.get(fromKey)
        from.remove(fromKey)

        //如果fromProp为空,则不做任何处理
        if (fromProp == null) {
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