package cn.whaley.bi.logsys.backuper.front

import java.util.concurrent._

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.KafkaMsgSource
import cn.whaley.bi.logsys.forest.Traits.{InitialTrait, LogTrait, NameTrait}
import kafka.message.MessageAndMetadata
import cn.whaley.bi.logsys.backuper.front.global.Constants._
import com.alibaba.fastjson.JSONObject
import org.slf4j.LoggerFactory

/**
 * Created by will on 2016/11/29.
 */
class MsgBatchManager extends InitialTrait with NameTrait with LogTrait {

    type KafkaMessage = MessageAndMetadata[Array[Byte], Array[Byte]]

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager = new ConfManager(Array("MsgBatchManager.xml"))): Unit = {
        val msgSourceName = confManager.getConf(this.name, "msgSource")
        msgSource = instanceFrom(confManager, msgSourceName).asInstanceOf[KafkaMsgSource]


    }

    /**
     * 启动
     */
    def start(): Unit = {

        val topicAndQueue = msgSource.getTopicAndQueueMap()

        msgSource.start()
        //等待1s，以便msgSource对消息队列进行初始填充
        Thread.sleep(1000)

        //对每个topic启动一个处理线程
        processThreads =
            topicAndQueue.map(item => {
                val thread = new BatchProcessThread(item._1, item._2)
                thread
            }).toSeq

        processThreads.foreach(item => item.start())
    }

    /**
     * 关停
      *
      * @param waiting 指示是否在处理线程完全停止后才返回
     */
    def shutdown(waiting: Boolean): Unit = {
        processThreads.foreach(item => {
            item.stopProcess(waiting)
            LOG.info(s"${item.getName} shutdown")
        })
        procThreadPool.shutdown()
        procThreadPool.awaitTermination(30, TimeUnit.SECONDS)
        processThreads = Array[BatchProcessThread]()
    }



    /**
     * 负责一个topic消息的消费线程
      *
      * @param topic
     * @param queue
     */
    class BatchProcessThread(topic: String, queue: LinkedBlockingQueue[KafkaMessage]) extends Thread {

        val logger = LoggerFactory.getLogger(topic)

        val consumerTopic = topic

        this.setName(s"process-$topic")

        /**
         * 如果线程处于运行状态，则栓锁大于0
         */
        val runningLatch = new CountDownLatch(1)



        /**
         * 请求停止
          *
          * @param waiting 指示是否在完全停止才返回
         * @return
         */
        def stopProcess(waiting: Boolean): Unit = {


            if (waiting) {
                LOG.info("waiting to task thread complete.")

            }
        }

        override def run(): Unit = {

                while (!SIGNAL_SHOULD_STOP) {
                    val msg = queue.take()
                    val strMsg = new String(msg.message(),"utf-8")
                    val json = new JSONObject()
                    json.put(KEY_MSG,strMsg)
                    json.put(KEY_TOPIC,topic)
                    val offset = msg.offset
                    val partition = msg.partition
                    json.put(KEY_OFFSET,offset)
                    json.put(KEY_PARTITION,partition)
                    json.put(KEY_TIMESTAMP,System.currentTimeMillis())
                    logger.info(strMsg)
                }

            runningLatch.countDown()


            LOG.info(s"${this.getName} exit")
        }

        override def toString: String = {
            s"BatchProcessThread[${this.getName}]"
        }


    }

    //消息处理线程池
    private val procThreadPool = Executors.newCachedThreadPool()
    //消费线程，每个topic一个线程
    private var processThreads: Seq[BatchProcessThread] = null
    private var msgSource: KafkaMsgSource = null



}
