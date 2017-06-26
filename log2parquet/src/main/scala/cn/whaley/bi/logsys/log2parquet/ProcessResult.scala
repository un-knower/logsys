package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.log2parquet.ProcessResultCode.ProcessCode


/**
 * Created by michael on 2017/6/22.
 */

case class ProcessResult[T](source: String, code: ProcessCode, message: String = "", result: Option[T] = None, ex: Option[Throwable] = None) {
    def hasErr: Boolean = {
        (code == ProcessResultCode.formatFailure
            || code == ProcessResultCode.signFailure
            || code == ProcessResultCode.exception)
    }
}

object ProcessResultCode extends Enumeration {
    type ProcessCode = Value

    /**
     * 发生了异常
     */
    val exception = Value(-3)

    /**
     * 签名校验失败
     */
    val signFailure = Value(-2)
    /**
     * 消息体格式解析失败
     */
    val formatFailure = Value(-1)
    /**
     * 消息处理完毕，是否交由下游处理，由应用方决定
     */
    val processed = Value(0)
    /**
     * 消息处理过程被跳过，交由下游处理器继续处理
     */
    val skipped = Value(1)
    /**
     * 消息被丢弃，并且不会交由下游处理器处理
     */
    val discard = Value(2)
    /**
     * 消息处理过程被中断，下游处理器应当停止处理
     */
    val break = Value(3)

    /**
     * 消息被丢弃，并且不会交由下游处理器处理,且提示下游处理应当静默处理不留日志信息
     */
    val silence = Value(4)
}

