package cn.whaley.bi.logsys.forest

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.entity.LogEntity
import com.alibaba.fastjson.JSON

/**
 * Created by fj on 16/11/10.
 */
class LogOriginProcessorChain extends GenericProcessorChain {

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
    }


    override def process(bytes: Array[Byte]): ProcessResult[Seq[LogEntity]] = {
        val jsonObj = JSON.parseObject(new String(bytes))
        val logEntity = LogEntity.create(jsonObj)
        logEntity.put("odsTs", System.currentTimeMillis())
        val logs = logEntity :: Nil
        new ProcessResult(this.name, ProcessResultCode.processed, "", Some(logs))
    }

}


