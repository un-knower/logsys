package cn.whaley.bi.logsys.forest.entity

import java.util.Date

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.commons.lang.time.DateFormatUtils

/**
 * Created by fj on 17/5/2.
 */
class MsgBodyEntity(from: JSONObject) extends JSONObject(from) {
    def host(): String = {
        this.getString(MsgBodyEntity.KEY_SVR_HOST)
    }

    def url(): String = {
        this.getString(MsgBodyEntity.KEY_SVR_REQ_URL)
    }

    def setUrl(value: String): Unit = {
        this.put(MsgBodyEntity.KEY_SVR_REQ_URL, value)
    }

    def method(): String = {
        this.getString(MsgBodyEntity.KEY_SVR_REQ_METHOD)
    }

    def contentType(): String = {
        this.getString(MsgBodyEntity.KEY_SVR_CONTENT_TYPE)
    }

    def receiveTime(): Long = {
        this.getLong(MsgBodyEntity.KEY_SVR_RECEIVE_TIME)
    }

    def body(): Object = {
        this.get(MsgBodyEntity.KEY_BODY)
    }

    def setBody(value: Object): Unit = {
        this.put(MsgBodyEntity.KEY_BODY, value)
    }

    //平展body
    def normalize(): Seq[JSONObject] = {

        if (body == null) {
            return Array(this)
        }
        val logTime = this.getLong("svr_receive_time")
        if(logTime != null) {
            try {
                val date = new Date(logTime)
                val dateStr = DateFormatUtils.format(date, "yyyy-MM-dd")
                val datetime = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss")
                this.put("date", dateStr)
                this.put("day", dateStr)
                this.put("datetime", datetime)
            }catch {
                case e: Exception =>
            }
        }
        if (body.isInstanceOf[JSONObject]) {
            this.asInstanceOf[java.util.Map[String, Object]].putAll(body.asInstanceOf[JSONObject])
            this.remove(MsgBodyEntity.KEY_BODY)
            Array(this)
        } else if (body.isInstanceOf[JSONArray]) {
            val array = body.asInstanceOf[JSONArray]
            this.remove(MsgBodyEntity.KEY_BODY)
            for (i <- 0 to array.size() - 1) yield {
                val item = array.get(i).asInstanceOf[JSONObject]
                item.asInstanceOf[java.util.Map[String, Object]].putAll(this)
                item
            }
        } else {
            Array(this)
        }
    }

}


object MsgBodyEntity {


    val KEY_SVR_HOST = "svr_host";
    val KEY_SVR_REQ_METHOD = "svr_req_method";
    val KEY_SVR_REQ_URL = "svr_req_url";
    val KEY_SVR_CONTENT_TYPE = "svr_content_type";
    val KEY_SVR_RECEIVE_TIME = "svr_receive_time"
    val KEY_BODY = "body";


}