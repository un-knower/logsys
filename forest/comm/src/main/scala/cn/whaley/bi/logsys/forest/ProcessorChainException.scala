package cn.whaley.bi.logsys.forest

import cn.whaley.bi.logsys.forest.entity.{LogEntity, MsgEntity}

/**
 *
 *
 *
 */

/**
 * Created by fj on 16/11/20.
 * 处理器链异常类
 * @param msgIdAndInfo 消息ID和消息相关的字符串信息
 * @param results 处理结果
 * @tparam A
 */
case class ProcessorChainException[A <: AnyRef](msgIdAndInfo: (String, String), results: Seq[ProcessResult[A]]) extends Exception {

}
