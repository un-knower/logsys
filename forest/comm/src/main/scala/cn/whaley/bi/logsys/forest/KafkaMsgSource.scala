package cn.whaley.bi.logsys.forest


import java.util.{TimerTask, Timer, Properties}
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}

import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import cn.whaley.bi.logsys.forest.Traits.{LogTrait, NameTrait, InitialTrait}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}
import org.apache.kafka.common.PartitionInfo
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
        topicRegexs = StringUtil.splitStr(confManager.getConf(this.name, "topics"), ",").map(item => item.r)
        queueCapacity = confManager.getConfOrElseValue(this.name, "queueCapacity", defaultQueueCapacity.toString).toInt
        logPerMsgCount = confManager.getConfOrElseValue(this.name, "logPerMsgCount", "500").toInt

        initKafkaUtil(confManager)
        initDefaultOffset(confManager)

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
    def start(): Unit = {
        //定期扫描,以匹配任务启动后创建的topic
        val timer = new Timer()
        timer.scheduleAtFixedRate(new TimerTask {
            override def run(): Unit = {
                LOG.info(s"scan topics. topicRegexs:${topicRegexs}")
                launch()
            }
        }, 300 * 1000, 300 * 1000)
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
            val ret = kafkaUtil.setFetchOffset(topic, groupId, offset)
            LOG.info(s"set fetch offset:$topic,$groupId,${offset.mkString(",")},${ret}")
            val currOffset = kafkaUtil.getFetchOffset(topic, groupId)
            LOG.info(s"current offset:${topic},${currOffset}")
        })
    }

    /**
     * 获取默认偏移值，如果没有设置则返回空Map
     * @param topic
     * @return
     */
    def getDefaultOffset(topic: String): Map[Int, Long] = {
        if (topicPartitions.get(topic).isEmpty) {
            return Map[Int, Long]()
        }
        val partitions = topicPartitions.get(topic).get
        partitions.flatMap(item => {
            val partition = item.partition()
            val matched =
                defaultOffset.map(item => {
                    var regStr = item._1
                    if (!regStr.startsWith("^")) regStr = "^" + regStr
                    if (!regStr.endsWith("$")) regStr = regStr + "$"
                    if (regStr.r.findFirstMatchIn(topic + "-" + partition).isDefined) {
                        (0, item)
                    } else if (regStr.r.findFirstMatchIn(topic).isDefined) {
                        (1, item)
                    } else {
                        (-1, null)
                    }
                }).filter(_._1 >= 0).toArray.sortBy(_._1)
            if (matched.size == 0) {
                None
            } else {
                Array((partition, matched(0)._2._2))
            }
        }).toMap
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


    //为每个topic启动一个读线程
    private def launch(): Unit = {
        this.synchronized {
            //通过正则表达式过滤需要订阅的topic列表
            val allTopic = kafkaUtil.getTopics()
            topics = allTopic.filter(topic => {
                (topic.startsWith("__") == false
                    && consumerThreads.exists(thread => thread.consumerTopic == topic) == false
                    && topicRegexs.count(reg => reg.findFirstMatchIn(topic).isDefined) > 0)
            })
            if (topics.length == 0) {
                return
            }

            LOG.info(s"topics:${topics.mkString}")
            val validTopics = topics.map(topic => {
                val partitionInfo = kafkaUtil.getPartitionInfo(topic)
                if (partitionInfo.size == 0) {
                    ""
                } else {
                    topicPartitions.put(topic, partitionInfo)
                    topic
                }
            }).filter(topic => topic != "")
            if (validTopics.length == 0) {
                return
            }

            var index = 0
            val threads = validTopics.map(topic => {
                val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerConf)
                consumer.subscribe(topic :: Nil)
                index = index + 1
                val queue = new LinkedBlockingQueue[ConsumerRecord[Array[Byte], Array[Byte]]](queueCapacity)
                msgQueueMap.put(topic, queue)
                new MsgConsumerThread(topic, index, queue, consumer)
            })

            val threadCount = threads.length
            if (threadCount == 0) {
                return
            }

            val latch = new CountDownLatch(threadCount)
            threads.foreach(item => {
                item.start()
                LOG.info(s"MsgConsumerThread[${item.getName}] started.")
                latch.countDown()
            })
            consumerThreads = consumerThreads ++ threads
            latch.await()

            //通知监听者
            if (msgQueueAddedListeners.size > 0) {
                val events = threads.map(thread => (thread.consumerTopic, thread.msgQueue))
                msgQueueAddedListeners.foreach(listener => {
                    listener(events)
                })
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
    private var topicRegexs: Seq[Regex] = null
    private var topics: Seq[String] = null
    private var logPerMsgCount = 100
    private var defaultOffset: Map[String, Long] = null
    private var queueCapacity: Int = defaultQueueCapacity


    private val topicPartitions: mutable.Map[String, List[PartitionInfo]] = new mutable.HashMap[String, List[PartitionInfo]]()
    private val msgQueueMap: mutable.Map[String, LinkedBlockingQueue[KafkaMessage]] = new mutable.HashMap[String, LinkedBlockingQueue[KafkaMessage]]()
    private var consumerThreads: ArrayBuffer[MsgConsumerThread] = new ArrayBuffer[MsgConsumerThread]
    private var msgQueueAddedListeners: ArrayBuffer[MsgQueueAddedListener] = new ArrayBuffer[MsgQueueAddedListener]


    class MsgConsumerThread(topic: String, index: Int, queue: LinkedBlockingQueue[KafkaMessage], stream: KafkaConsumer[Array[Byte], Array[Byte]]) extends Thread {

        @volatile private var keepRunning: Boolean = true

        val consumerTopic = topic

        val msgQueue = queue

        this.setName(s"consumer-${topic}")

        def stopProcess(): Unit = {
            keepRunning = false
            this.interrupt()
            LOG.info(s"MsgConsumerThread[${this.getName}] stopped")
        }

        override def run(): Unit = {
            this.setName(s"${topic}/${index}")
            var count = 0
            LOG.info(s"MsgConsumerThread[${this.getName}] started")
            while (keepRunning) {
                try {
                    var timeout = 100
                    val records = stream.poll(timeout)
                    if (records.count() > 0) {
                        records.foreach(record => {
                            msgQueue.put(record)
                        })
                        timeout = 100
                        count = count + 1
                        if (count % logPerMsgCount == 0) {
                            LOG.info(s"MsgConsumerThread[${this.getName}] msgCount: ${count}")
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
