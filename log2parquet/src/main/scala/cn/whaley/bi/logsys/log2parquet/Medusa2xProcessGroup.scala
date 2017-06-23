package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.entity.{LogFromEntity}
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTrait
import cn.whaley.bi.logsys.log2parquet.traits.{ProcessGroupTrait}


/**
 * Created by michael on 2017/6/22.
 */
class Medusa2xProcessGroup extends ProcessGroupTrait  {

    var logProcessor: LogProcessorTrait = null

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        val logProcName = confManager.getConf(this.name, "Medusa2xLogProcessingUnits")
        logProcessor = instanceFrom(confManager, logProcName).asInstanceOf[LogProcessorTrait]
    }


    override def process(log:LogFromEntity): ProcessResult[LogFromEntity] = {
        val logPost = logProcessor.process(log)
        logPost
    }

}


