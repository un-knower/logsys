package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.entity.LogEntity
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTrait
import cn.whaley.bi.logsys.log2parquet.traits.{ProcessGroupTrait, InitialTrait, LogTrait, NameTrait}


/**
 * Created by michael on 2017/6/22.
 */
class Medusa3xProcessGroup extends ProcessGroupTrait {

    var logProcessor: LogProcessorTrait = null

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        val logProcName = confManager.getConf(this.name, "Medusa3xLogProcessingUnits")
        logProcessor = instanceFrom(confManager, logProcName).asInstanceOf[LogProcessorTrait]
    }


    override def process(log:LogEntity): ProcessResult[LogEntity] = {
        val logPost = logProcessor.process(log)
        logPost
    }

}


