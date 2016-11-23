package cn.whaley.bi.logsys.forest.processor

import cn.whaley.bi.logsys.forest.Traits.{NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest.ProcessResult
import cn.whaley.bi.logsys.forest.entity.{LogEntity, MsgEntity}

/**
 * Created by fj on 16/11/10.
 *
 * 归集层消息体msgBody转换Trait
 */
trait MsgDecodeTrait extends InitialTrait with NameTrait {

    /**
     * 消息解码，从字节数组解析为归集层消息体格式
     * @return
     */
    def decode(bytes: Array[Byte]): ProcessResult[MsgEntity]

}
