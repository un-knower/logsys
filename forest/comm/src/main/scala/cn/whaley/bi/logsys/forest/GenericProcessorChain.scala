package cn.whaley.bi.logsys.forest

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.Traits.{LogTrait, NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest.entity.{MsgEntity, LogEntity}
import cn.whaley.bi.logsys.forest.processor._
import com.alibaba.fastjson.JSONObject


/**
 * Created by fj on 16/11/10.
 */
class GenericProcessorChain extends InitialTrait with LogTrait with NameTrait {

    var msgDecoder: MsgDecodeTrait = null

        var msgProcessor: MsgProcessorTrait = null

    var logProcessor: LogProcessorTrait = null


    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        val msgDecoderName = confManager.getConf(this.name, "msgDecoder")
        msgDecoder = instanceFrom(confManager, msgDecoderName).asInstanceOf[MsgDecodeTrait]

        val msgProcName = confManager.getConf(this.name, "msgProcessor")
        msgProcessor = instanceFrom(confManager, msgProcName).asInstanceOf[MsgProcessorTrait]

        val logProcName = confManager.getConf(this.name, "logProcessor")
        logProcessor = instanceFrom(confManager, logProcName).asInstanceOf[LogProcessorTrait]
    }


    def process(bytes: Array[Byte]): ProcessResult[Seq[LogEntity]] = {
        val decRet = msgDecoder.decode(bytes)

        //解码错误
        if (decRet.hasErr) {
            ProcessorChainException(("NONE", new String(bytes)), Array(decRet))
            return new ProcessResult(this.name, decRet.code, decRet.message, None, decRet.ex)
        }

        val msgEntity = decRet.result.get
        val msgRet = msgProcessor.process(msgEntity)

        //归集层消息处理错误
        if (msgRet.hasErr) {
            val decRet = msgDecoder.decode(bytes)
            val exception = Some(ProcessorChainException((msgEntity.msgId, decRet.result.get.toJSONString), Array(msgRet)))
            return new ProcessResult(this.name, ProcessResultCode.exception, "归集层消息处理错误", None, exception)
        }

        val logRet =
            msgRet.result.get.map(item => {
                val logRet = logProcessor.process(item)
                logRet
            })

        //应用层消息处理错误
        val errRets = logRet.filter(item => item.hasErr)
        if (errRets.length > 0) {
            val decRet = msgDecoder.decode(bytes)
            val exception = Some(ProcessorChainException((msgEntity.msgId, decRet.result.get.toJSONString), errRets))
            return new ProcessResult(this.name, ProcessResultCode.exception, "应用层消息处理错误", None, exception)
        }

        //返回结果
        val logs =
            logRet.flatMap(item => {
                item.result.get
            })

        new ProcessResult(this.name, ProcessResultCode.processed, "", Some(logs))
    }

}


