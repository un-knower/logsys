package cn.whaley.bi.logsys.forest.sinker


import java.util

import cn.whaley.bi.logsys.forest.{ProcessorChainException, ProcessResultCode, ProcessResult}
import cn.whaley.bi.logsys.forest.entity.LogEntity
import com.alibaba.fastjson.serializer.SerializeFilter
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Created by fj on 16/10/30.
 */
trait MsgSinkTrait {
    type KafkaMessage = ConsumerRecord[Array[Byte], Array[Byte]]

    /**
     * 停止服务
     */
    def stop(): Unit

    /**
     * 保存处理后的数据
     * @param procResults
     * @return (SuccCount,ErrCount)
     */
    def saveProcMsg(procResults: Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])]): (Int, Int)

    /**
     * 从目标中获取某个源topic最后写入的offset信息
     * @param sourceTopic 源topic
     * @param sourceLatestOffset 源topic的最后偏移信息
     * @param maxMsgCount 检索目标时,每个partition最多读取的消息数量
     * @return 源topic相关的offse信（partition，offset），如果没有则返回空Map对象
     */
    def getTopicLastOffset(sourceTopic: String, sourceLatestOffset: Map[Int, Long], maxMsgCount: Int): Map[Int, Long]


    /**
     * 构建同步信息
     * @param source
     * @return
     */
    def buildSyncInfo(source: KafkaMessage): JSONObject = {
        val keyObj = new JSONObject()
        keyObj.put("odsTs", System.currentTimeMillis())
        val keyBytes = source.key()
        if (keyBytes != null && keyBytes.length > 0) {
            val keyStr = new String(keyBytes)
            try {
                val obj = JSON.parseObject(keyStr)
                keyObj.asInstanceOf[java.util.Map[String, Object]].putAll(obj)
            } catch {
                case ex: Throwable => {
                    println("[WARN] parse keyObj error:" + keyStr)
                }
            }
        }
        if (!keyObj.containsKey("rawTopic")) {
            keyObj.put("rawTopic", source.topic())
            keyObj.put("rawParId", source.partition())
            keyObj.put("rawOffset", source.offset())
            keyObj.put("rawTs", source.timestamp())
        } else if (!keyObj.containsKey("oriTopic")) {
            keyObj.put("oriTopic", source.topic())
            keyObj.put("oriParId", source.partition())
            keyObj.put("oriOffset", source.offset())
            keyObj.put("oriTs", source.timestamp())
        }
        keyObj
    }

    /**
     * 构建错误信息
     * @param message
     * @param errResult
     * @return
     */
    def buildErrorData(message: KafkaMessage, errResult: ProcessResult[Seq[LogEntity]]): JSONObject = {
        val syncInfo = buildSyncInfo(message)
        val errData = new JSONObject()
        errData.put("_sync", syncInfo)
        if (message.key() != null) {
            errData.put("msg_key", new String(message.key()))
        }

        val msgErr = new JSONObject()
        msgErr.put("source", errResult.source)
        msgErr.put("code", errResult.code.toString)
        msgErr.put("message", errResult.message)

        if (errResult.ex.isDefined && errResult.ex.get != null) {
            val ex=errResult.ex.get
            msgErr.put("ex", ex.getMessage)
            if(ex.getClass.getName.indexOf("ProcessorChainException")>=0){
                val msgValue=ex.asInstanceOf[ProcessorChainException[AnyRef]].msgIdAndInfo.toString()
                errData.put("msg_value",msgValue)
            }
        }
        if(!errData.containsKey("msg_value")){
            errData.put("msg_value", new String(message.value()))
        }
        errData.put("msg_err", msgErr.toJSONString)
        errData
    }

}
