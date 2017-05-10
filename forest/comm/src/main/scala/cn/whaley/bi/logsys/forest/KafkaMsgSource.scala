package cn.whaley.bi.logsys.forest


import java.util.{TimerTask, Timer, Properties}
import java.util.concurrent.{LinkedBlockingQueue}

import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import cn.whaley.bi.logsys.forest.Traits.{LogTrait, NameTrait, InitialTrait}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}
import org.apache.kafka.common.{TopicPartition, PartitionInfo}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable}
import scala.util.matching.Regex

/**
 * Created by fj on 16/10/30.
 *
 * 消息源，以队列的方式对外提供数据
 */
class KafkaMsgSource extends InitialTrait with NameTrait with LogTrait {

    //kafka消息
    type KafkaMessage = ConsumerRecord[Array[Byte], Array[Byte]]

    //消息源队列增加监听器(topic,queue)
    type MsgQueueAddedListener = Seq[(String, LinkedBlockingQueue[KafkaMessage])] => Unit

    var confManager: ConfManager = null
    var consumerConf: Properties = null

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {

        this.confManager = confManager
        this.consumerConf = confManager.getAllConf("kafka-consumer", true)

        groupId = consumerConf.getProperty("group.id")
        topicRegex = confManager.getConf(this.name, "topicRegex").r
        topicScanIntervalSec = confManager.getConfOrElseValue(this.name, "topicScanIntervalSec", topicScanIntervalSec.toString).toInt
        queueCapacity = confManager.getConfOrElseValue(this.name, "queueCapacity", defaultQueueCapacity.toString).toInt
        logPerMsgCount = confManager.getConfOrElseValue(this.name, "logPerMsgCount", "500").toInt

        initKafkaUtil(confManager)
        initDefaultOffset(confManager)


    }

    /**
     * 启动
     */
    def launch(): Unit = {
        if (topicScanIntervalSec > 0) {
            //定期扫描,以匹配任务启动后创建的topic
            val timer = new Timer()
            timer.scheduleAtFixedRate(new TimerTask {
                override def run(): Unit = {
                    initProcessThreadAuto()
                }
            }, 0, topicScanIntervalSec * 1000)
        } else {
            initProcessThreadAuto()
        }
    }

    /**
     * 增加消息队列增加监听器
     * @param listener
     */
    def addMsgQueueAddedListener(listener: MsgQueueAddedListener): Unit = {
        this.msgQueueAddedListeners.append(listener)
    }

    /**
     * 启动数据源读取线程
     */
    def start(topic: String = ""): Unit = {
        if (topic == "") {
            this.initProcessThreadAuto()
        } else {
            var threads = consumerThreads.filter(_.consumerTopic == topic).toSeq
            if (threads.size == 0) {
                threads = initProcessThread(Array(topic))
            }
            threads.head.start()
        }
    }

    /**
     * 停止全部或特定数据源读取线程
     */
    def stop(topic: String = ""): Unit = {
        consumerThreads.foreach(item => {
            if (topic == "" || item.consumerTopic == topic) {
                item.stopProcess()
            }
        })
    }

    /**
     * 提交偏移信息
     * @param offsetInfo
     */
    def commitOffset(offsetInfo: Map[String, Map[Int, Long]]): Unit = {
        offsetInfo.map(item => {
            val topic = item._1
            val offset = item._2
            consumerThreads.filter(_.consumerTopic == topic).foreach(thread => {
                val consumer = thread.kafkaConsumer
                //如果线程没有启动,则consumer可能尚未绑定任何分区,此时执行一次poll来解决
                if (!thread.isStarted) {
                    consumer.poll(1000)
                }
                offset.filter(item => consumer.assignment().exists(_.partition() == item._1))
                    .foreach(item => {
                    consumer.seek(new TopicPartition(topic, item._1), item._2)
                    LOG.info(s"seek offset:[($topic,${item._1}),${item._2}]")
                })
                //对不在offset之中的分区设置默认offset
                if (defaultOffset.contains(topic)) {
                    consumer.assignment().filter(item => !offset.contains(item.partition()))
                        .foreach(item => {
                        val defaultValue = defaultOffset.get(topic).get
                        if (defaultValue == -2) {
                            consumer.seekToBeginning(new TopicPartition(topic, item.partition()) :: Nil)
                        } else if (defaultValue == -1) {
                            consumer.seekToEnd(new TopicPartition(topic, item.partition()) :: Nil)
                        } else {
                            require(defaultValue >= 0)
                            consumer.seek(new TopicPartition(topic, item.partition()), defaultValue)
                        }
                    })
                }
            })

        })
    }

    /**
     * 获取最后偏移量
     * @param topic
     * @return
     */
    def getLatestOffset(topic: String): Map[Int, Long] = {
        kafkaUtil.getLatestOffset(topic)
    }

    /**
     * 获取当前数据源topic的分区信息
     * @return
     */
    def getTopicPartitions(topic: String): Option[List[PartitionInfo]] = {
        topicPartitions.get(topic)
    }

    /**
     * 获取topic及其对应的消息队列
     * @return
     */
    def getTopicAndQueueMap(): Map[String, LinkedBlockingQueue[KafkaMessage]] = {
        msgQueueMap.toMap
    }

    //扫描配置项匹配的topic,如果topic没有初始化相应的处理线程,则创建并通知监听者
    private def initProcessThreadAuto(): Seq[MsgConsumerThread] = {
        val topics = kafkaUtil.getTopics()
            .filter(topic => (topic.startsWith("__") == false && topicRegex.findFirstMatchIn(topic).isDefined))
        val threads = initProcessThread(topics)
        if (threads.length > 0) {
            LOG.info(s"scan topics. topicRegex:${topicRegex}, new consumer thread: ${threads.map(_.consumerTopic).mkString(",")}.")
        }
        threads
    }

    //为每个topic初始化一个读线程,并通知监听者,返回本次初始化的线程
    private def initProcessThread(topics: Seq[String]): Seq[MsgConsumerThread] = {
        this.synchronized {
            var index = consumerThreads.size
            val threads = topics.filter(topic => !consumerThreads.exists(thread => thread.consumerTopic == topic)
            ).map(topic => {
                val partitionInfo = kafkaUtil.getPartitionInfo(topic)
                topicPartitions.put(topic, partitionInfo)
                val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerConf)
                consumer.subscribe(topic :: Nil)
                index = index + 1
                val queue = new LinkedBlockingQueue[ConsumerRecord[Array[Byte], Array[Byte]]](queueCapacity)
                msgQueueMap.put(topic, queue)
                new MsgConsumerThread(topic, index, queue, consumer)
            })

            if (threads.length > 0) {
                consumerThreads = consumerThreads ++ threads
                //通知监听者
                if (msgQueueAddedListeners.size > 0) {
                    val events = threads.map(thread => (thread.consumerTopic, thread.msgQueue))
                    msgQueueAddedListeners.foreach(listener => {
                        listener(events)
                    })
                }
                threads
            } else {
                new Array[MsgConsumerThread](0)
            }
        }
    }

    /**
     * 初始化默认偏移，逗号分隔的配置项： ${topic正则表达式-partition}:${offset}
     * @param confManager
     */
    private def initDefaultOffset(confManager: ConfManager): Unit = {
        val defaultOffsetStr = StringUtil.splitStr(confManager.getConfOrElseValue(this.name, "defaultOffset", ""), ",")
        defaultOffset =
            defaultOffsetStr.map(item => {
                val values = item.split(":")
                (values(0), values(1).toLong)
            }).toMap
    }

    /**
     * 初始化KafkaUtil
     * @param confManager
     */
    private def initKafkaUtil(confManager: ConfManager): Unit = {
        val bootstrapStr = consumerConf.getProperty("bootstrap.servers")
        this.kafkaUtil = KafkaUtil(bootstrapStr, this.groupId)
    }

    private val defaultQueueCapacity = 2000
    private var groupId: String = null

    private var kafkaUtil: KafkaUtil = null
    private var topicRegex: Regex = null
    //topic定期扫描间隔(秒),以监听新创建的且匹配topicRegex的topic,如果小于等于0,则不进行扫描
    private var topicScanIntervalSec: Int = 0
    private var logPerMsgCount = 100
    private var defaultOffset: Map[String, Long] = null

    private var queueCapacity: Int = defaultQueueCapacity

    private val topicPartitions: mutable.Map[String, List[PartitionInfo]] = new mutable.HashMap[String, List[PartitionInfo]]()
    private val msgQueueMap: mutable.Map[String, LinkedBlockingQueue[KafkaMessage]] = new mutable.HashMap[String, LinkedBlockingQueue[KafkaMessage]]()
    private var consumerThreads: ArrayBuffer[MsgConsumerThread] = new ArrayBuffer[MsgConsumerThread]
    private val msgQueueAddedListeners: ArrayBuffer[MsgQueueAddedListener] = new ArrayBuffer[MsgQueueAddedListener]


    class MsgConsumerThread(topic: String, index: Int, queue: LinkedBlockingQueue[KafkaMessage], consumer: KafkaConsumer[Array[Byte], Array[Byte]]) extends Thread {

        @volatile private var keepRunning: Boolean = true
        @volatile var isStarted = false
        @volatile var msgCount = 0

        val consumerTopic = topic
        val msgQueue = queue
        val kafkaConsumer = consumer

        this.setName(s"consumer-${topic}")

        def stopProcess(): Unit = {
            keepRunning = false
            this.interrupt()
            LOG.info(s"MsgConsumerThread[${this.getName}] stopped")
        }

        override def run(): Unit = {
            this.setName(s"${topic}/${index}")
            this.isStarted = true

            LOG.info(s"MsgConsumerThread[${this.getName}] started")
            while (keepRunning) {
                try {
                    var timeout = 100
                    val records = consumer.poll(timeout)
                    if (records.count() > 0) {
                        records.foreach(record => {
                            msgQueue.put(record)
                        })
                        timeout = 100
                        msgCount = msgCount + 1
                        if (msgCount % logPerMsgCount == 0) {
                            LOG.info(s"MsgConsumerThread[${this.getName}] msgCount: ${msgCount}")
                        }
                    } else {
                        timeout = 1000
                    }
                } catch {
                    case e: InterruptedException => {
                        LOG.info(s"${this.getName} is interrupted. keepRunning:${keepRunning}")
                        return
                    }
                }
            }
        }

        override def toString: String = {
            s"MsgConsumerThread[${this.getName}]"
        }

    }

}
