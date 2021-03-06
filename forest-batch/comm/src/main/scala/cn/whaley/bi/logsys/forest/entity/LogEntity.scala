package cn.whaley.bi.logsys.forest.entity

import cn.whaley.bi.logsys.forest.StringUtil
import com.alibaba.fastjson.{JSONObject}

/**
 * Created by fj on 16/11/9.
 *
 * 应用层日志消息实体对象
 */
class LogEntity(from: MsgEntity) extends MsgEntity(from) {

    def appId: String = {
        this.getString(LogEntity.KEY_APP_ID)
    }

    def updateAppId(value: String): Unit = {
        this.put(LogEntity.KEY_APP_ID, value)
    }

    def logId: String = {
        this.getString(LogEntity.KEY_LOG_ID)
    }

    def updateLogId(value: String): Unit = {
        this.put(LogEntity.KEY_LOG_ID, value)
    }

    def logVersion: String = {
        this.getString(LogEntity.KEY_LOG_VERSION)
    }

    def updateLogVersion(value: String): Unit = {
        this.put(LogEntity.KEY_LOG_VERSION, value)
    }

    def logTime: Long = {
        this.getLongValue(LogEntity.KEY_LOG_TIME)
    }

    def updateLogTime(value: Long): Unit = {
        this.put(LogEntity.KEY_LOG_TIME, value)
    }

    def logSignFlag: Int = {
        this.getIntValue(LogEntity.KEY_LOG_SIGN_FLAG)
    }

    def updateLogSignFlag(value: Int): Unit = {
        this.put(LogEntity.KEY_LOG_SIGN_FLAG, value)
    }

    def logBody: JSONObject = {
        this.getJSONObject(LogEntity.KEY_LOG_BODY)
    }

    def updateLogBody(value: JSONObject): Unit = {
        if (this.containsKey(LogEntity.KEY_LOG_BODY)) {
            this.getJSONObject(LogEntity.KEY_LOG_BODY).asInstanceOf[java.util.Map[String, Object]].putAll(value)
        } else {
            this.put(LogEntity.KEY_LOG_BODY, value)
        }
    }

    //平展化msgBody
    def normalizeMsgBodyObj(): Seq[JSONObject] = {
        msgBodyObj.normalize()
    }


    //格式平展化
    def normalize(): Seq[LogEntity] = {
        //展开msgBody
        val normalizeMsgBody = normalizeMsgBodyObj()
        this.removeMsgBody
        val size = normalizeMsgBody.size
        val entities = for (i <- 0 to size - 1) yield {
            //只有一个消息体则可避免一次不必要的复制
            val entity = if (i == 0) this else LogEntity.copy(this)
            val logId = this.msgId + StringUtil.fixLeftLen(Integer.toHexString(i), '0', 4)
            entity.updateLogId(logId)
            entity.updateLogBody(normalizeMsgBody(i))

            //提升公共字段
            MsgEntity.translateProp(entity.logBody, LogEntity.KEY_APP_ID, entity, LogEntity.KEY_APP_ID)
            MsgEntity.translateProp(entity.logBody, LogEntity.KEY_LOG_VERSION, entity, LogEntity.KEY_LOG_VERSION)
            MsgEntity.translateProp(entity.logBody, LogEntity.KEY_LOG_SIGN_FLAG, entity, LogEntity.KEY_LOG_SIGN_FLAG)
            MsgEntity.translateProp(entity.logBody, LogEntity.KEY_LOG_TIME, entity, LogEntity.KEY_LOG_TIME)

            entity
        }
        entities
    }

}

object LogEntity {
    val KEY_APP_ID = "appId"
    val KEY_LOG_ID = "logId"
    val KEY_LOG_VERSION = "logVersion"
    val KEY_LOG_TIME = "logTime"
    val KEY_LOG_SIGN_FLAG = "logSignFlag"
    val KEY_LOG_BODY = "logBody"


    val VAL_SIGN_NO = 0
    val VAL_SIGN_PASS = 1
    val VAL_SIGN_ERR = -1

    def create(obj: JSONObject): LogEntity = {
        new LogEntity(new MsgEntity(obj))
    }

    def copy(obj: JSONObject): LogEntity = {
        new LogEntity(MsgEntity.copy(obj))
    }

}

