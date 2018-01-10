package cn.whaley.bi.logsys.forest.sinker

import java.util.concurrent.CountDownLatch

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.ProcessResult
import cn.whaley.bi.logsys.forest.Traits.{InitialTrait, LogTrait, NameTrait}
import cn.whaley.bi.logsys.forest.entity.LogEntity
import com.alibaba.fastjson.JSONObject

/**
 * Created by fj on 2017/6/27.
 * 该类是一个组合sink, 组合调用多个sink, 并返回第一个sink的返回值
 */
class CombinedMsgSink extends MsgSinkTrait with InitialTrait with NameTrait with LogTrait {
    /**
     * 停止服务
     */
    override def stop(): Unit = {
        val latch = new CountDownLatch(msgSinks.size)
        msgSinks.foreach(sink => {
            val thread = new Thread(new Runnable {
                def run() {
                    sink.stop()
                    latch.countDown()
                }
            });
            thread.start()
        })
        latch.await()
    }

    /**
     * 从目标中获取某个源topic最后写入的offset信息
     * @param sourceTopic 源topic
     * @param sourceLatestOffset 源topic的最后偏移信息
     * @param maxMsgCount 检索目标时,每个partition最多读取的消息数量
     * @return 源topic相关的offse信（partition，offset），如果没有则返回空Map对象
     */
    override def getTopicLastOffset(sourceTopic: String, sourceLatestOffset: Map[Int, Long], maxMsgCount: Int): Map[Int, Long] = {
        throw new UnsupportedOperationException
    }

    /**
      * 保存监控数据
      *
      * @param monitorInfo 监控数据
      */
    override def saveMonitorInfo(monitorInfo: JSONObject): Unit = {}

    /**
     * 保存处理后的数据
     * @param procResults
     * @return (SuccCount,ErrCount)
     */
    override def saveProcMsg(procResults: Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])]): (Int, Int) = {
        val results = msgSinks.map(sink => sink.saveProcMsg(procResults))
        results.head
    }

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        val sinkStr = confManager.getConf(this.name, "sinks").split(",")
        msgSinks = sinkStr.map(item => {
            val confKeyPrefix = item.trim.replace("\n", "").replace("\r", "")
            instanceFrom(confManager, confKeyPrefix).asInstanceOf[MsgSinkTrait]
        })
    }

    //sink链
    var msgSinks: Seq[MsgSinkTrait] = null
}
