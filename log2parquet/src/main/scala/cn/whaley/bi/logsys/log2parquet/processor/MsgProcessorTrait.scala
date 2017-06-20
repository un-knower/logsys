package cn.whaley.bi.logsys.log2parquet.processor

import cn.whaley.bi.logsys.log2parquet.ProcessResult
import cn.whaley.bi.logsys.log2parquet.entity.{LogEntity, MsgEntity}
import cn.whaley.bi.logsys.log2parquet.traits.{NameTrait, InitialTrait}

/**
 * Created by fj on 16/11/9.
 *
 * 归集层消息处理器Trait
 */
trait MsgProcessorTrait extends InitialTrait with NameTrait  {

    /**
     * 解析日志消息体
      *
      * @return
     */
    def process(msgEntity: MsgEntity): ProcessResult[Seq[LogEntity]]
}
