package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.ProcessGroupTraitV2
import com.alibaba.fastjson.JSONObject


/**
 * Created by michael on 2017/6/22.
 */
class Medusa2xProcessGroupV3 extends ProcessGroupTraitV2  {

    var logProcessor: LogProcessorTraitV2 = null

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        val logProcName = confManager.getConf(this.name, "Medusa2xLogProcessingUnits")
        logProcessor = instanceFrom(confManager, logProcName).asInstanceOf[LogProcessorTraitV2]
    }


    override def process(log:JSONObject): ProcessResult[JSONObject] = {
        val logPost = logProcessor.process(log)
        logPost
    }

}


