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
import scala.collection.mutable

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


        batchSize = confManager.getConfOrElseValue(this.name, "batchSize", "4000").toInt
        callableSize = confManager.getConfOrElseValue(this.name, "callableSize", "2000").toInt
        recoverSourceOffsetFromSink = confManager.getConfOrElseValue(this.name, "recoverSourceOffsetFromSink", "1").toInt


    }

    /**
     * 启动
     */
    def start(): Unit = {

        val topicAndQueue = msgSource.getTopicAndQueueMap()

        val offsetMap = new mutable.HashMap[String, Map[Int, Long]]()

        //默认offset
        topicAndQueue.foreach(item => {
            val offset = msgSource.getDefaultOffset(item._1)
            offsetMap.put(item._1, offset)
        })

        //从目标topic恢复offset
        if (recoverSourceOffsetFromSink == 1) {
            //获取topic的最后处理offset
            val metadatas = msgSource.getTopicMetadatas()
            topicAndQueue.map(item => {
                val topic = item._1
                //从所有目标topic中获取源topic的offset信息
                val map = topicNameMapper.getTargetTopicMap(topic)
                val partitionCount = if (metadatas.get(topic).isDefined) metadatas.get(topic).get.size else 0
                if (map.isDefined && partitionCount > 0) {
                    //理论上源topic的partition是均衡分布的，那么在目标topic中读取一批的处理数据，
                    //就能恢复源topic每个partition的offset，设置为2倍，以保留一定的余量
                    //如果源topic的partition数据不均衡，那么可能存在部分源topic的partition的offset信息不能恢复
                    //这些partition的offset依然沿用kafka的自动offset管理的值
                    val msgCount = (batchSize / partitionCount) * 2
                    val seq = map.get
                    val offset: Map[Int, Long] =
                        seq.flatMap(targetTopic => {
                            msgSink.getTopicLastOffset(topic, targetTopic, msgCount)
                        }).groupBy(arrItem => arrItem._1)
                            .map(arrItem => {
                            (arrItem._1, arrItem._2.maxBy(_._2)._2)
                        })
                    //合并默认offset
                    val defaultOffset = offsetMap.get(topic)
                    if (defaultOffset.isDefined) {
                        val newMap = new mutable.HashMap[Int, Long]()
                        newMap.putAll(defaultOffset.get)
                        newMap.putAll(offset)
                        offsetMap.put(topic, newMap.toMap)
                    } else {
                        offsetMap.put(topic, offset)
                    }
                } else {
                    LOG.info(s"topic[${topic}] can not get offset,targetTopic:${map.getOrElse("" :: Nil).mkString(",")},sourcePartitionCount:${partitionCount}")

                }
            })
        }

        if (offsetMap.size > 0) {
            LOG.info(s"commitOffset:${offsetMap}")
            msgSource.commitOffset(offsetMap.toMap)
        } else {
            LOG.info(s"commitOffset:None")
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

                val listSize = list.size()

                //分批次提交消息处理任务
                val callableCount = (listSize - 1) / callableSize + 1
                val futures =
                    for (i <- 0 to callableCount - 1) yield {
                        callId = callId + 1
                        val indexFrom = i * callableSize
                        val indexTo = Math.min(listSize, (i + 1) * callableSize)
                        val taskList = list.subList(indexFrom, indexTo)
                        val callable = new ProcessCallable(callId, taskList)
                        val future = procThreadPool.submit(callable)
                        future
                    }

                LOG.info(s"${topic}-taskSubmit(${callableCount},${callableSize}):${monitor.checkStep()}")

                //等待所有消息处理任务执行完毕
                val procResults = futures.map(_.get).sortBy(_._1).flatMap(_._2)

                LOG.info(s"${topic}-msgProcess(${procResults.size}}):${monitor.checkStep()}")

                //错误处理
                val errorResults =
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

                if (errorResults.length > 0) {
                    msgSink.saveErrorMsg(errorResults)
                    LOG.info(s"${topic}-saveErr(${errorResults.length}):${monitor.checkStep()}")
                }

                //发送处理成功的数据
                val okResults =
                    procResults.filter(result => result._2.hasErr == false)
                        .flatMap(result => {
                        val message = result._1
                        val procResult = result._2.result
                        procResult.get.flatMap(item => {
                            val targetTopics = topicNameMapper.getTargetTopic(message.topic, item)
                            targetTopics.map(targetTopic => (targetTopic, message, item))
                        })
                    })
                msgSink.saveProcMsg(okResults)

                //保存topic映射，以便后续从目标topic恢复源topic的offset
                topicNameMapper.saveTargetTopicMap()

                LOG.info(s"${topic}-sendSink(${okResults.length}):${monitor.checkStep()}")

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
                LOG.info(s"${topic}-done(${listSize}):${monitor.checkDone()}:${offset}")
            }

            runningLatch.countDown()

            LOG.info(s"${this.getName} exit")
        }

        override def toString: String = {
            s"BatchProcessThread[${this.getName}]"
        }

        //private def


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
    private var batchSize: Int = 4000
    //每次消息处理过程调用所处理的消息数量
    private var callableSize: Int = 2000


}
