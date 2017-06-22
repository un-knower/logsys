package cn.whaley.bi.logsys.log2parquet.processor

import cn.whaley.bi.logsys.log2parquet.ProcessResult
import cn.whaley.bi.logsys.log2parquet.entity.LogEntity
import cn.whaley.bi.logsys.log2parquet.traits.{NameTrait, InitialTrait}

/**
 * Created by michael on 2017/6/22.
 *
 * 应用层日志处理器Trait
 */
trait LogProcessorTrait extends InitialTrait with NameTrait {

    /**
     * 解析日志消息体
      *
      * @return
     */
    def process(log: LogEntity): ProcessResult[LogEntity]
}
