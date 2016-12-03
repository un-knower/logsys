package cn.whaley.bi.logsys.forest.processor

import cn.whaley.bi.logsys.common.ConfManager
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
    /**
     * 消息解码，从字节数组解析为归集层消息体格式
     * @return
     */
    override def decode(bytes: Array[Byte]): ProcessResult[MsgEntity] = {
        val decodeStr = new String(bytes)
        val str =
            if (ngxLogDecode) {
                val ngxStr=StringUtil.decodeNgxStrToString(decodeStr)
                //处理nginx的POST消息体为空的问题
                ngxStr.replace("\"body\":-","\"body\":{}")
            } else {
                decodeStr
            }
        try {
            val msg = new MsgEntity(JSON.parseObject(str))
            new ProcessResult(this.name, ProcessResultCode.processed, "", Some(msg))
        } catch {
            case e: Throwable => {
                new ProcessResult(this.name, ProcessResultCode.exception, "JSON解析异常:" + str, None, Some(e))
            }
        }

    }

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        ngxLogDecode = confManager.getConfOrElseValue(this.name, "ngxLogDecode", "true").toBoolean
    }

    private var ngxLogDecode: Boolean = true
}
