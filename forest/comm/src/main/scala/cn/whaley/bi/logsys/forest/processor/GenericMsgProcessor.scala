package cn.whaley.bi.logsys.forest.processor

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.{ProcessResultCode, ProcessResult}
import cn.whaley.bi.logsys.forest.entity.{LogEntity, MsgEntity}

/**
 * Created by fj on 16/10/30.
 *
 * 通用归集层消息处理器
 */
class GenericMsgProcessor extends MsgProcessorTrait {

    //归集层消息处理器链
    var processes: Array[MsgProcessorTrait] = null


    /**
     * 初始化
     */
    override def init(confManager: ConfManager) = {
        processes = {
            val procStr = confManager.getConf(this.name, "processors").split(",")
            procStr.map(item => {
                val confKeyPrefix = item.trim.replace("\n", "").replace("\r", "")
                instanceFrom(confManager, confKeyPrefix).asInstanceOf[MsgProcessorTrait]
            })
        }
    }

    /**
     * 消息处理过程
     * @param msg
     * @return
     */
    override def process(msg: MsgEntity): ProcessResult[Seq[LogEntity]] = {
        val length = processes.length
        for (i <- 0 to length - 1) {
            val result = processes(i).process(msg)
            if (result.code == ProcessResultCode.processed) {
                return result
            }
        }
        new ProcessResult(this.name, ProcessResultCode.skipped, "", None)
    }

}

