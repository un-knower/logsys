package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.entity.LogEntity
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTrait
import cn.whaley.bi.logsys.log2parquet.traits.{InitialTrait, LogTrait, NameTrait}
import com.alibaba.fastjson.JSONObject


/**
 * Created by fj on 16/11/10.
 */
class GenericProcessorChain extends InitialTrait with LogTrait with NameTrait {

    var logProcessor: LogProcessorTrait = null

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        val logProcName = confManager.getConf(this.name, "medusa3xLogProcessor")
        logProcessor = instanceFrom(confManager, logProcName).asInstanceOf[LogProcessorTrait]
    }


    def process(log:LogEntity): ProcessResult[LogEntity] = {
        val logPost = logProcessor.process(log)
        logPost
    }

}


