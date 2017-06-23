package cn.whaley.bi.logsys.log2parquet.processor

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.{ProcessResult, ProcessResultCode}
import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ArrayBuffer

/**
 * Created by michael on 2017/6/22.
 *
 * 处理器集合
 */
class LogProcessingUnits extends LogProcessorTraitV2 {

    var processes: Array[LogProcessorTraitV2] = null

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        processes = {
            val procStr = confManager.getConf(this.name, "processors").split(",")
            procStr.map(item => {
                val confKeyPrefix = item.trim.replace("\n", "").replace("\r", "")
                instanceFrom(confManager, confKeyPrefix).asInstanceOf[LogProcessorTraitV2]
            })
        }
    }

    /**
     * 解析日志消息体
      *
      * @return
     */
    override def process(log: JSONObject): ProcessResult[JSONObject] = {
        //处理结果
        var logs: ArrayBuffer[JSONObject] = ArrayBuffer(log)
        //记录处理路由
        val route = new ArrayBuffer[String]
        //链式处理，前一处理器输出为后一处理器输入
        val length = processes.length
        for (i <- 0 to (length - 1)) {
            val process = processes(i)
            route.append(process.name)
            //当前处理结果
            var currLogs = new ArrayBuffer[JSONObject]
            //记录流程是否应该中断，只有所有消息都要求中断的情况下才进行中断
            var isBreak = false
            logs.foreach(curr => {
                val result = {
                    try {
                        processes(i).process(curr)
                    } catch {
                        case e: Throwable => {
                            new ProcessResult(route.mkString("->"), ProcessResultCode.exception, "throw exception:" + e.getMessage, None, Some(e))
                        }
                    }
                }
                if (result.hasErr) {
                    return new ProcessResult(route.mkString("->"), result.code, result.message, None, result.ex)
                }
                if (result.code != ProcessResultCode.discard && result.code != ProcessResultCode.silence) {
                    //当前处理器跳过的消息，继续交由下游处理器
                    if (result.code == ProcessResultCode.skipped) {
                        currLogs.append(curr)
                    } else {
                        currLogs ++= result.result
                    }
                } else {
                    if (result.code != ProcessResultCode.silence) {
                        println(s"discard log [${result.message}]: " + curr.toJSONString)
                    }
                }
                if (result.code != ProcessResultCode.break) {
                    isBreak = false
                }
            })
            //中断点的结果即为整个处理链的结果
            if (isBreak) {
                return new ProcessResult(route.mkString("->"), ProcessResultCode.processed, "", Some(log))
            }
            logs = currLogs
        }
        new ProcessResult(route.mkString("->"), ProcessResultCode.processed, "", Some(log))
    }

}
