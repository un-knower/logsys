package cn.whaley.bi.logsys.log2parquet

/**
 *
 *
 *
 */

/**
 * Created by michael on 2017/6/22.
 * 处理器链异常类
 * @param msgIdAndInfo 消息ID和消息相关的字符串信息
 * @param results 处理结果
 * @tparam A
 */
case class ProcessorChainException[A <: AnyRef](msgIdAndInfo: (String, String), results: Seq[ProcessResult[A]]) extends Exception {
    override def getMessage(): String = {
        var msg=""
        results.map(result=>s"[${result.source};${result.code.toString};${result.message}]").mkString("|")
    }
}
