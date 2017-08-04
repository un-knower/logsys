package cn.whaley.bi.logsys.forest.processor

import cn.whaley.bi.logsys.forest.Traits.{NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest.entity.LogEntity
import cn.whaley.bi.logsys.forest.ProcessResult

/**
 * Created by fj on 16/11/9.
 *
 * 应用层日志处理器Trait
 */
trait LogProcessorTrait extends InitialTrait with NameTrait {

    /**
     * 解析日志消息体
     * @return
     */
    def process(log: LogEntity): ProcessResult[Seq[LogEntity]]
}
