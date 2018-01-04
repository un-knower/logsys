package cn.whaley.bi.logsys.forest

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent._

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.Traits.{InitialTrait, LogTrait, NameTrait}
import cn.whaley.bi.logsys.forest.entity.LogEntity
import cn.whaley.bi.logsys.forest.sinker.MsgSinkTrait
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * Created by fj on 16/10/30.
  */
class MsgBatchManager extends InitialTrait with NameTrait with LogTrait {

    type KafkaMessage = ConsumerRecord[Array[Byte], Array[Byte]]


/**
  * 初始化方法

     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager = new ConfManager(Array("MsgBatchManager.xml"))): Unit = {
        val msgSourceName = confManager.getConf(this.name, "msgSource")
        msgSource = instanceFrom(confManager, msgSourceName).asInstanceOf[KafkaMsgSource]

        val msgSinkName = confManager.getConf(this.name, "msgSink")
        msgSink = instanceFrom(confManager, msgSinkName).asInstanceOf[MsgSinkTrait]

        val processorChainName = confManager.getConf(this.name, "processorChain")
        processorChain = instanceFrom(confManager, processorChainName).asInstanceOf[GenericProcessorChain]


        batchSize = confManager.getConfOrElseValue(this.name, "batchSize", "4000").toInt
        callableSize = confManager.getConfOrElseValue(this.name, "callableSize", "2000").toInt
        recoverSourceOffsetFromSink = confManager.getConfOrElseValue(this.name, "recoverSourceOffsetFromSink", "1").toInt
        resetOffsetToLatest = confManager.getConfOrElseValue(this.name, "resetOffsetToLatest", "0").toInt
        callableWaitSize = confManager.getConfOrElseValue(this.name, "callableWaitSize", callableWaitSize.toString).toInt
        callableWaitSec = confManager.getConfOrElseValue(this.name, "callableWaitSec", callableWaitSec.toString).toInt

    }

    /**
     * 启动
     */
    def start(): Unit = {
        msgSource.addMsgQueueAddedListener(this.msgQueueAddedFn)
        msgSource.launch()
    }

    /**
     * 关停
     * @param waiting 指示是否在处理线程完全停止后才返回
     */
    def shutdown(waiting: Boolean): Unit = {

        //停止数据源读取操作
        LOG.info("## stopping msgSource.")
        msgSource.stop()
        LOG.info("## stopped msgSource.")

        //停止消息处理线程
        LOG.info("## stopping processThreads.")
        processThreads.foreach(item => {
            item.stopProcess(waiting)
            LOG.info(s"## ${item.getName} shutdown")
        })
        LOG.info("## stopped processThreads.")

        //关闭消息处理线程池
        LOG.info("## stopping procThreadPool.")
        procThreadPool.shutdown()
        procThreadPool.awaitTermination(30, TimeUnit.SECONDS)
        LOG.info("## stopped procThreadPool.")
        processThreads.clear()

        //停止数据写入操作
        LOG.info("## stopping msgSink.")
        msgSink.stop()
        LOG.info("## stopped msgSink.")

        shutdownLatch.countDown()
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
    class BatchProcessThread(topic: String, queue: LinkedBlockingQueue[KafkaMessage], exF: (BatchProcessThread, Throwable) => Boolean) extends Thread {

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
            LOG.info(s"${this.getName} stopping process...")
            keepRunning = false
            this.interrupt()
            if (waiting) {
                LOG.info(s"${this.getName} waiting to task thread complete.")
                runningLatch.await()
                LOG.info(s"${this.getName} task thread complete.")
            }
            LOG.info(s"${this.getName} stopped process...")
        }

        override def run(): Unit = {

            val monitor = new TaskMonitor()

            var callId = 0
            var msgCount = 0
            var lastTaskTime = System.currentTimeMillis()

            val processFn = () => {
                //从消息队列中获取一批数据，如果队列为空，则进行等待,获取到数据后，等待100ms以累积数据
                val list = new util.ArrayList[KafkaMessage]()

                if (keepRunning) {
                    if (queue.peek() == null) {
                        pIsWaiting = true
                        val ts = System.currentTimeMillis() - lastTaskTime
                        lastTaskTime = System.currentTimeMillis()
                        LOG.info(s"queue[${topic}] is empty.total ${msgCount} message processed. ts:${ts} .taking...")
                        try {
                            val probeObj = queue.take()
                            list.add(probeObj)
                        } catch {
                            case e: InterruptedException => {
                                LOG.info(s"${this.getName} is interrupted. keepRunning:${keepRunning}")
                            }
                        }
                        pIsWaiting = false
                    }
                    var c = queue.drainTo(list, batchSize)
                    if (c <= callableWaitSize) {
                        try{
                            Thread.sleep(callableWaitSec * 1000);
                        }catch{
                            case ex:Throwable=>{}
                        }
                        val moreC = queue.drainTo(list, batchSize);
                        LOG.debug(s"tak $c message. wait $callableWaitSec sec, take $moreC message")
                        c = c + moreC
                    }
                    LOG.info(s"take $c message.")
                } else {
                    //在停止之前处理队列中存量数据
                    val c = queue.drainTo(list)
                    LOG.info(s"take last $c message.")
                }

                val listSize = list.size()

                if (listSize == 0) {
                    LOG.info("message list is empty.")
                } else {
                    val monitorMsg = new JSONObject()
                    monitorMsg.put("topic", topic)
                    monitorMsg.put("messageSize", listSize)
                    monitorMsg.put("machineId", machineId)
                    monitorMsg.put("time", DateFormatUtils.format(monitor.getTaskFrom(), "yyyy-MM-dd HH:mm:ss"))

                    //分批次提交消息处理任务,利用多线程加快处理进度
                    val callableCount: Int = Math.max(1, Math.ceil(listSize * 1.0 / callableSize).toInt)
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

                    val submitStep = monitor.checkStep()
                    LOG.info(s"${topic}-taskSubmit(${callableCount},${callableSize}):${submitStep}")
                    monitorMsg.put("submitStepMillis", submitStep._2)
                    monitorMsg.put("processCallableCount", callableCount)

                    //等待所有消息处理任务执行完毕,sink端采用单线程模式
                    val procResults = futures.map(future => {
                        future.get
                    }).sortBy(_._1).flatMap(_._2)
                    val processStep = monitor.checkStep()
                    LOG.info(s"${topic}-msgProcess(${procResults.size}):${processStep}")
                    monitorMsg.put("processStepMillis", processStep._2)
                    monitorMsg.put("msgProcessSize", procResults.size)
                    monitorMsg.put("msgProcessErrSize", procResults.count(_._2.hasErr))

                    //发送处理成功的数据
                    val ret = msgSink.saveProcMsg(procResults)
                    val saveStep = monitor.checkStep()
                    LOG.info(s"${topic}-msgSave(${ret._1},${ret._2}):${saveStep}")
                    monitorMsg.put("saveStepMillis", saveStep._2)
                    monitorMsg.put("msgSaveSize", ret._1)

                    //打印错误日志
                    val errorResults = procResults.filter(result => result._2.hasErr == true).toList
                    val errorCount = errorResults.size
                    if (errorCount > 0) {
                        LOG.error(s"${errorCount} messages processed failure.")
                        errorResults.foreach(err => {
                            logMsgProcChainErr(err._1, err._2)
                        })
                    }


                    //打印offset日志信息
                    val offset = procResults.map(result => {
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
                    val doneStep = monitor.checkDone()
                    LOG.info(s"${topic}-done(${listSize}):${doneStep}:${offset}")
                    monitorMsg.put("overallMsgSize", msgCount)
                    monitorMsg.put("doneStepMillis", doneStep._2)
                    msgSink.saveMonitorInfo(monitorMsg)
                }
            }

            while (keepRunning || (!keepRunning && queue.peek() != null)) {
                try {
                    processFn()
                } catch {
                    case e: Throwable => {
                        val ret = exF(this, e)
                        if (!ret) {
                            LOG.info(s"${this.getName} throw exception. exit...")
                            keepRunning = false
                        }
                    }
                }
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
             * @return (单步序号，单步耗时，单步起始时间)
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
                    try {
                        val message = item.value()
                        (item, processorChain.process(message))
                    } catch {
                        case e: Throwable => {
                            (item, new ProcessResult[Seq[LogEntity]](s"ProcessCallable[${item}]", ProcessResultCode.exception, "throw exception:" + e.getMessage, None, Some(e)))
                        }
                    }
                })
            (id, result)
        }
    }

    //消息源队列增加监听函数
    private def msgQueueAddedFn(topicAndQueue: Seq[(String, LinkedBlockingQueue[KafkaMessage])]): Unit = {

        val offsetMap = new mutable.HashMap[String, Map[Int, Long]]()

        //从目标topic恢复offset
        if (recoverSourceOffsetFromSink == 1) {
            //获取topic的最后处理offset
            topicAndQueue.foreach(item => {
                val topic = item._1
                val sourceLatestOffset = msgSource.getLatestOffset(topic)
                //从目标中获取源topic的offset信息, 存在源topic已被监听,但尚未创建的情况,此时分区数为0
                val partitions = msgSource.getTopicPartitions(topic)
                if (partitions.isDefined) {
                    //理论上源topic的partition是均衡分布的，那么在目标topic中读取一批的处理数据，
                    //就能恢复源topic每个partition的offset，设置为2倍，以保留一定的余量
                    //如果源topic的partition数据不均衡，那么可能存在部分源topic的partition的offset信息不能恢复
                    //这些partition的offset依然沿用kafka的自动offset管理的值
                    val msgCount = (batchSize / partitions.get.size) * 2
                    val info = msgSink.getTopicLastOffset(topic, sourceLatestOffset, msgCount)
                    LOG.info(s"getTopicLastOffset[${topic},${msgCount}]:${info.mkString(",")}")

                    val offset: Map[Int, Long] = info.groupBy(arrItem => arrItem._1)
                        .map(arrItem => {
                        (arrItem._1, arrItem._2.maxBy(_._2)._2)
                    })
                    LOG.info(s"offsetInfo:[${offset.mkString(",")}]")
                    if(offset.nonEmpty) {
                        offsetMap.put(topic, offset)
                    }
                }
            })
            if (offsetMap.size > 0) {
                LOG.info(s"commitOffset:${offsetMap}")
                msgSource.seekOffset(offsetMap.toMap)
            } else {
                LOG.info(s"commitOffset:None")
            }
        } else if (resetOffsetToLatest == 1) {
            msgSource.seekOffsetToEnd()
        }

        //启动消费线程
        topicAndQueue.foreach(item => {
            val topic = item._1
            msgSource.start(topic)
        })

        //等待1s，以便msgSource对消息队列进行初始填充
        Thread.sleep(1000)

        //处理线程异常回调函数，如果返回false，则线程应该退出
        val exF = (thread: BatchProcessThread, ex: Throwable) => {
            LOG.error(s"${thread.getName},throw exception", ex)
            //停止相应topic数据源线程的读取
            msgSource.stop(thread.consumerTopic)
            //错误计数，如果全部线程都出错，则shutdown
            processThreadErr = processThreadErr + 1
            if (processThreadErr >= processThreads.length) {
                LOG.error("all thread throw exception.task exit...")
                shutdown(false)
            }
            false
        }

        //对每个topic启动一个处理线程
        topicAndQueue.map(item => {
            val thread = new BatchProcessThread(item._1, item._2, exF)
            thread.start()
            processThreads.add(thread)
        })

    }

    //打印错误日志
    private def logMsgProcChainErr[T <: AnyRef](message: KafkaMessage, procResult: ProcessResult[T]): Unit = {

        if (!procResult.ex.isDefined || !procResult.ex.get.isInstanceOf[ProcessorChainException[AnyRef]]) {
            val info = s"process error (${message.topic},${message.partition},${message.offset})"
            LOG.debug(info)
            LOG.debug(s"ERROR:${procResult}")
            return
        }

        val chainException = procResult.ex.get.asInstanceOf[ProcessorChainException[AnyRef]]
        val msgId = chainException.msgIdAndInfo._1
        val msgInfo = chainException.msgIdAndInfo._2
        val results = chainException.results
        LOG.debug(s"process error[$msgId](${message.topic},${message.partition},${message.offset})")
        LOG.debug(s"ERROR:${procResult.message}\t${msgInfo}")
        results.foreach(item => {
            val info = s"process result[$msgId]:\t${item.source}\t${item.code}\t]"
            LOG.debug(info)
            LOG.debug(s"ERROR:${item.message}")
        })
    }

    //消息处理线程池
    private val procThreadPool = Executors.newCachedThreadPool()
    //处理线程，每个topic一个线程
    private val processThreads: ArrayBuffer[BatchProcessThread] = new ArrayBuffer[BatchProcessThread]()
    //处理线程计数，如果所有处理线程均出错，则应该退出
    private var processThreadErr: Int = 0
    //是否从消息目标系统中恢复消息源系统的offset
    private var recoverSourceOffsetFromSink: Int = 1
    //是否重置为从最新开始消费
    private var resetOffsetToLatest: Int = 0
    private var msgSource: KafkaMsgSource = null
    private var msgSink: MsgSinkTrait = null
    private var processorChain: GenericProcessorChain = null
    private var batchSize: Int = 4000
    //每次消息处理过程调用所处理的消息数量
    private var callableSize: Int = 2000
    private var callableWaitSize: Int = 100
    private var callableWaitSec: Int = 1
    private val machineId = ConfigUtils.getMachineId

    val shutdownLatch = new CountDownLatch(1)


}
