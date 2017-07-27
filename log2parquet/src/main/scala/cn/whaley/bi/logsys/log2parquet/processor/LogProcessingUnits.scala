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
      println("-----所有处理器为：")
      procStr.map(item => {
        println("处理器:"+item)
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
    //记录处理路由
    val route = new ArrayBuffer[String]
    //processes链式处理，前一处理器输出为后一处理器输入
    for (process <-  processes) {
      route.append(process.name)
      val result = try {
        process.process(log)
      } catch {
        case e: Throwable => {
          new ProcessResult(route.mkString("->"), ProcessResultCode.exception, "throw exception:" + e.getMessage, None, Some(e))
        }
      }

      if (result.hasErr) {
        return new ProcessResult(route.mkString("->"), result.code, result.message, None, result.ex)
      }

      if (result.code == ProcessResultCode.discard) {
        return new ProcessResult(route.mkString("->"), result.code, result.message, None, result.ex)
      }

      if (result.code == ProcessResultCode.break) {
        return new ProcessResult(route.mkString("->"), result.code, result.message, Some(log))
      }
    }

    new ProcessResult(route.mkString("->"), ProcessResultCode.processed, "", Some(log))
  }
}
