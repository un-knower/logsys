package cn.whaley.bi.logsys.forest.actionlog

import cn.whaley.bi.logsys.forest.entity.LogEntity
import com.alibaba.fastjson.JSONObject


/**
 * Created by fj on 16/11/10.
 */
class ActionLogPostEntity(from: LogEntity) extends LogEntity(from) {

    def postMsgBodyObj(): PostMsgBodyEntity = {
        new PostMsgBodyEntity(this.msgBodyObj)
    }

    override def normalizeMsgBodyObj(): Seq[JSONObject] = {
        postMsgBodyObj.normalize()
    }
}
