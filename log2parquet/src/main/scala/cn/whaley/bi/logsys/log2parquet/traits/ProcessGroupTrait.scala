package cn.whaley.bi.logsys.log2parquet.traits

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.ProcessResult
import cn.whaley.bi.logsys.log2parquet.entity.LogEntity

/**
  * Created by michael on 2017/6/22.
  */
trait ProcessGroupTrait extends InitialTrait with LogTrait with NameTrait{
    def init(confManager: ConfManager): Unit
    def process(log:LogEntity): ProcessResult[LogEntity]
}
