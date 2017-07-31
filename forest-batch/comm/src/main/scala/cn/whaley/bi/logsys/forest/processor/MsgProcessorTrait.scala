package cn.whaley.bi.logsys.forest.processor

import cn.whaley.bi.logsys.forest.Traits.{NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest.ProcessResult
import cn.whaley.bi.logsys.forest.entity.{LogEntity, MsgEntity}

/**
 * Created by fj on 16/11/9.
 *
 * 归集层消息处理器Trait
 */
trait MsgProcessorTrait extends InitialTrait with NameTrait  {

    /**
     * 解析日志消息体
     * @return
     */
    def process(msgEntity: MsgEntity): ProcessResult[Seq[LogEntity]]
}
