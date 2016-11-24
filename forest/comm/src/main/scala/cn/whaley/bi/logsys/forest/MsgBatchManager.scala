package cn.whaley.bi.logsys.forest

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent._

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.Traits.{LogTrait, NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest.entity.{LogEntity}

import kafka.message.MessageAndMetadata
import scala.collection.JavaConversions._

/**
 * Created by fj on 16/10/30.
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

        val msgSinkName = confManager.getConf(this.name, "msgSink")
        msgSink = instanceFrom(confManager, msgSinkName).asInstanceOf[KafkaMsgSink]

        val processorChainName = confManager.getConf(this.name, "processorChain")
        processorChain = instanceFrom(confManager, processorChainName).asInstanceOf[GenericProcessorChain]

        val topicNameMapperName = confManager.getConf(this.name, "topicNameMapper")
        topicNameMapper = instanceFrom(confManager, topicNameMapperName).asInstanceOf[GenericTopicMapper]


        batchSize = confManager.getConfOrElseValue(this.name, "batchSize", defaultBatchSize.toString).toInt
        recoverSourceOffsetFromSink = confManager.getConfOrElseValue(this.name, "recoverSourceOffsetFromSink", "1").toInt


    }

    /**
     * 启动
     */
    def start(): Unit = {

        val topicAndQueue = msgSource.getTopicAndQueueMap()

        if (recoverSourceOffsetFromSink == 1) {
            //获取topic的最后处理offset
            val metadatas = msgSource.getTopicMetadatas()
            val offsetInfo =
                topicAndQueue.map(item => {
                    val topic = item._1
                    //从所有目标topic中获取源topic的offset信息
                    val map = topicNameMapper.getTargetTopicMap(topic)
                    val partitionCount = if (metadatas.get(topic).isDefined) metadatas.get(topic).get.size else 0
                    if (map.isDefined && partitionCount > 0) {
                        val msgCount = batchSize * partitionCount
                        val seq = map.get
                        val offset: Map[Int, Long] =
                            seq.flatMap(targetTopic => {
                                msgSink.getTopicLastOffset(topic, targetTopic, msgCount)
                            }).groupBy(arrItem => arrItem._1)
                                .map(arrItem => {
                                (arrItem._1, arrItem._2.maxBy(_._2)._2)
                            })
                        (topic, offset)
                    } else {
                        LOG.info(s"topic[${topic}] can not get offset,targetTopic:${map.getOrElse("" :: Nil).mkString(",")},sourcePartitionCount:${partitionCount}")
                        (topic, Map[Int, Long]())
                    }
                }).filter(item => item._2.size > 0)

            if (offsetInfo.size > 0) {
                LOG.info(s"commitOffset:${offsetInfo}")
                msgSource.commitOffset(offsetInfo)
            } else {
                LOG.info(s"commitOffset:None")
            }
        }

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
     * 处于消费状态的topic列表
     * @return
     */
    def consumingTopics: Seq[String] = {
        processThreads.filter(item => !item.isWaiting).map(item => item.consumerTopic)
    }

    /**
     * 负责一个topic消息的消费线程
     * @param topic
     * @param queue
     */
    class BatchProcessThread(topic: String, queue: LinkedBlockingQueue[KafkaMessage]) extends Thread {

        @volatile private var keepRunning: Boolean = true
        @volatile private var pIsWaiting = false

        val consumerTopic = topic

        this.setName(s"process-${topic}")

        /**
         * 如果线程处于运行状态，则栓锁大于0
         */
        val runningLatch = new CountDownLatch(1)

        /**
         * 指示当前是否正等待消息队列数据
         * @return
         */
        def isWaiting: Boolean = {
            pIsWaiting
        }

        /**
         * 请求停止
         * @param waiting 指示是否在完全停止才返回
         * @return
         */
        def stopProcess(waiting: Boolean): Unit = {

            keepRunning = false
            LOG.info("stop process...")
            if (isWaiting) {
                LOG.info("interrupt task thread")
                this.interrupt()
            }
            if (waiting) {
                LOG.info("waiting to task thread complete.")
                runningLatch.await()
            }
        }

        override def run(): Unit = {

            val monitor = new TaskMonitor()

            var callId = 0
            var msgCount = 0
            var lastTaskTime = System.currentTimeMillis()

            while (keepRunning) {

                //从消息队列中获取一批数据，如果队列为空，则进行等待,获取到数据后，等待100ms以累积数据
                val list = new util.ArrayList[KafkaMessage]()

                if (queue.peek() == null) {
                    pIsWaiting = true
                    val ts = System.currentTimeMillis() - lastTaskTime
                    lastTaskTime = System.currentTimeMillis()
                    LOG.info(s"queue[${topic}] is empty.total ${msgCount} message processed. ts:${ts} .taking...")
                    try {
                        val probeObj = queue.take()
                        Thread.sleep(100)
                        list.add(probeObj)
                    } catch {
                        case e: InterruptedException => {
                            runningLatch.countDown()
                            LOG.info(s"${this.getName} is interrupted. keepRunning:${keepRunning}")
                            return
                        }
                    }
                    pIsWaiting = false
                }
                queue.drainTo(list, batchSize)


                callId = callId + 1
                val callable = new ProcessCallable(callId, list)
                val future = procThreadPool.submit(callable)

                LOG.info(s"${topic}-taskSubmit:${monitor.checkStep()}")

                val procResults = future.get._2

                LOG.info(s"${topic}-msgProcess:${monitor.checkStep()}")

                //错误处理
                val errorDatas =
                    procResults.filter(result => result._2.hasErr == true)
                        .flatMap(result => {
                        val message = result._1
                        val procResult = result._2
                        //打印错误日志
                        logMsgProcChainErr(message, procResult)
                        val errTopics = topicNameMapper.getErrTopic(message.topic, null)
                        errTopics.map(errorTopic => {
                            (errorTopic, message)
                        })
                    })

                if (errorDatas.length > 0) {
                    msgSource.saveErrorMsg(errorDatas)
                    LOG.info(s"${topic}-saveErr(${errorDatas.length}):${monitor.checkStep()}")
                }

                //发送处理成功的数据
                val procDatas =
                    procResults.filter(result => result._2.hasErr == false)
                        .flatMap(result => {
                        val message = result._1
                        val procResult = result._2.result
                        procResult.get.flatMap(item => {
                            val targetTopics = topicNameMapper.getTargetTopic(message.topic, item)
                            targetTopics.map(targetTopic => (targetTopic, message, item))
                        })
                    })
                msgSink.send(procDatas)

                //保存topic映射，以便后续从目标topic恢复源topic的offset
                topicNameMapper.saveTargetTopicMap()

                LOG.info(s"${topic}-sendSink(${procDatas.length}):${monitor.checkStep()}")

                //offset
                val offset =
                    procResults.map(result => {
                        val message = result._1
                        ((message.topic, message.partition), message.offset)
                    }).groupBy(item => item._1)
                        .map(item => {
                        val topicAndPartition = item._1
                        val lastOffset = item._2.maxBy(offset => offset._2)._2
                        val firstOffset = item._2.minBy(offset => offset._2)._2
                        (topicAndPartition, firstOffset, lastOffset)
                    })

                msgCount = msgCount + list.size()
                LOG.info(s"${topic}-done(${list.size()}):${monitor.checkDone()}:${offset}")
            }

            runningLatch.countDown()

            LOG.info(s"${this.getName} exit")
        }

        override def toString: String = {
            s"BatchProcessThread[${this.getName}]"
        }


        /**
         * 任务监视类，按步骤统计任务执行时间
         */
        class TaskMonitor() {
            private var taskFrom = System.currentTimeMillis()
            private var stepFrom = System.currentTimeMillis()
            private var step = 0

            private def formatDate(ts: Long): String = {
                val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                format.format(new Date(ts))
            }

            def getTaskFrom() = taskFrom

            /**
             * 检查单步
             * @return (单步序号，单步起始时间，单步耗时)
             */
            def checkStep(): (Int, Long, String) = {
                step = step + 1
                val info = (step, System.currentTimeMillis() - stepFrom, formatDate(stepFrom))
                stepFrom = System.currentTimeMillis()
                info
            }

            /**
             * 检查完成
             * @return (单步序号，任务起始时间，任务耗时)
             */
            def checkDone(): (Int, Long, String) = {
                step = step + 1
                val info = (step, System.currentTimeMillis() - taskFrom, formatDate(taskFrom))
                reset
                info
            }

            /**
             * 复位
             */
            def reset: Unit = {
                taskFrom = System.currentTimeMillis()
                stepFrom = System.currentTimeMillis()
                step = 0

            }
        }

    }


    /**
     * 消息处理Callable封装，提交到消息处理线程池中执行
     * @param id
     * @param data
     */
    class ProcessCallable(id: Int, data: Seq[KafkaMessage]) extends Callable[(Int, Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])])] {
        override def call(): (Int, Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])]) = {
            val result =
                data.map(item => {
                    val message = item.message()
                    (item, processorChain.process(message))
                })
            (id, result)
        }
    }

    private def logMsgProcChainErr[T <: AnyRef](message: KafkaMessage, procResult: ProcessResult[T]): Unit = {

        if (!procResult.ex.isDefined || !procResult.ex.get.isInstanceOf[ProcessorChainException[AnyRef]]) {
            val info = s"process error (${message.topic},${message.partition},${message.offset}):${procResult}"
            LOG.error(info, procResult.ex.getOrElse(null))
            return
        }

        val chainException = procResult.ex.get.asInstanceOf[ProcessorChainException[AnyRef]]
        val msgId = chainException.msgIdAndInfo._1
        val msgInfo = chainException.msgIdAndInfo._2
        val results = chainException.results
        LOG.error(s"process error[$msgId](${message.topic},${message.partition},${message.offset}):\t${procResult.message}\t${msgInfo}")
        results.foreach(item => {
            val info = s"process result[$msgId]:\t${item.source}\t${item.code}\t${item.message}]"
            if (item.ex.isDefined) {
                LOG.error(info, item.ex.get)
            } else {
                LOG.error(info)
            }
        })
    }


    //消息处理线程池
    private val procThreadPool = Executors.newCachedThreadPool()
    //消费线程，每个topic一个线程
    private var processThreads: Seq[BatchProcessThread] = null
    //是否从消息目标系统中恢复消息源系统的offset
    private var recoverSourceOffsetFromSink: Int = 1
    private var msgSource: KafkaMsgSource = null
    private var msgSink: KafkaMsgSink = null
    private var processorChain: GenericProcessorChain = null
    private var topicNameMapper: GenericTopicMapper = null
    private var batchSize: Int = defaultBatchSize
    private val defaultBatchSize: Int = 1000


}
