package cn.whaley.bi.logsys.log2parquet.entity

import com.alibaba.fastjson.JSONObject


/**
 * Created by michael on 2017/6/22.
 */
class ActionLogPostEntity(from: LogEntity) extends LogEntity(from) {

    def postMsgBodyObj(): PostMsgBodyEntity = {
        new PostMsgBodyEntity(this.msgBodyObj)
    }

    override def normalizeMsgBodyObj(): Seq[JSONObject] = {
        postMsgBodyObj.normalize()
    }
}
