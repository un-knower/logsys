package cn.whaley.bi.logsys.forest.processor

import cn.whaley.bi.logsys.common.{StringDecoder, ConfManager}
import cn.whaley.bi.logsys.forest.Traits.NameTrait
import cn.whaley.bi.logsys.forest.{ProcessResultCode, ProcessResult, StringUtil}
import cn.whaley.bi.logsys.forest.entity.MsgEntity
import com.alibaba.fastjson.JSON

/**
 * Created by fj on 16/11/17.
 *
 * 通用归集层消息解码器
 */
class GenericMsgDecoder extends MsgDecodeTrait with NameTrait {

    val decoder = new StringDecoder()

    /**
     * 消息解码，从字节数组解析为归集层消息体格式
     * @return
     */
    override def decode(bytes: Array[Byte]): ProcessResult[MsgEntity] = {
        val str = new String(bytes)
        try {
            var value = str
            var fbTime = ""
            if (filebeatDecode) {
                val json = JSON.parseObject(str)
                value = json.getString("message")
                fbTime = json.getString("@timestamp")
            }
            if (ngxLogDecode) {
                value = decoder.decodeToString(value)
            }
            val msg = new MsgEntity(JSON.parseObject(value))
            if (filebeatDecode) {
                msg.msgBody.put("svr_fb_Time", fbTime)
            }
            new ProcessResult(this.name, ProcessResultCode.processed, "", Some(msg))
        } catch {
            case e: Throwable => {
                new ProcessResult(this.name, ProcessResultCode.exception, "JSON解析异常:" + new String(bytes), None, Some(e))
            }
        }

    }

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        ngxLogDecode = confManager.getConfOrElseValue(this.name, "ngxLogDecode", "true").toBoolean
        filebeatDecode = confManager.getConfOrElseValue(this.name, "filebeatDecode", "true").toBoolean
    }

    private var ngxLogDecode: Boolean = true
    private var filebeatDecode: Boolean = true
}
