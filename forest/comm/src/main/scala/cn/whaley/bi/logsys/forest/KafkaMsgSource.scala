package cn.whaley.bi.logsys.forest


import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}

import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import cn.whaley.bi.logsys.forest.Traits.{LogTrait, NameTrait, InitialTrait}
import kafka.consumer.KafkaStream
import kafka.javaapi.consumer.ConsumerConnector
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.{KafkaProducer}
import scala.collection.JavaConversions._
import scala.collection.{mutable}
import scala.util.matching.Regex

/**
 * Created by fj on 16/10/30.
 *
 * 消息源，以队列的方式对外提供数据
 */
class KafkaMsgSource extends InitialTrait with NameTrait with LogTrait {

    type KafkaMessage = MessageAndMetadata[Array[Byte], Array[Byte]]

    var confManager: ConfManager = null

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {

        this.confManager = confManager

        topicRegexs = StringUtil.splitStr(confManager.getConf(this.name, "topics"), ",").map(item => item.r)
        queueCapacity = confManager.getConfOrElseValue(this.name, "queueCapacity", defaultQueueCapacity.toString).toInt
        logPerMsgCount = confManager.getConfOrElseValue(this.name, "logPerMsgCount", "500").toInt



        //实例化kafka生产者
        val producerConf = confManager.getAllConf("kafka-producer", true)
        kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](producerConf)

        //实例化kafka消费者
        val consumerConf = confManager.getAllConf("kafka-consumer", true)
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(consumerConf))

        val zkServers = consumerConf.get("zookeeper.connect").toString


        initDefaultOffset(confManager)

        //kafkaUtil
        groupId = consumerConf.getProperty("group.id")
        kafkaUtil = KafkaUtil(zkServers, groupId)

        //通过正则表达式过滤需要订阅的topic列表

        val allTopic = KafkaUtil.getTopics(zkServers)
        topics =
            allTopic.filter(topic => {
                if (topic.startsWith("__")) {
                    false
                } else {
                    val c = topicRegexs.count(reg => reg.findFirstMatchIn(topic).isDefined)
                    c > 0
                }
            })
        require(topics.length > 0)
        LOG.info(s"topics:${topics.mkString}")

        //topic元数据信息
        topicMetaInfos = topics.map(topic => {
            (topic, kafkaUtil.getPartitionMetadata(topic))
        }).toMap



        //初始化消息队列Map，每个topic对应一个队列
        msgQueueMap =
            topics.map(item => {
                val queue = new LinkedBlockingQueue[MessageAndMetadata[Array[Byte], Array[Byte]]](queueCapacity)
                (item, queue)
            }).toMap
    }

    /**
     * 启动数据源读取线程
     */
    def start(): Unit = {
        val topicCountMap = getSourceTopicCountMap(topics, confManager)
        LOG.info("topicCountMap:{}", topicCountMap)

        val streams = consumerConnector.createMessageStreams(topicCountMap)

        //特定topic的特定partition对应一个线程
        consumerThreads =
            streams.flatMap(streamItem => {
                val topic = streamItem._1
                val streamList = streamItem._2
                var index = 0
                streamList.map(stream => {
                    LOG.info(s"create MsgConsumerThread[${index}] for ${topic}")
                    val thread = new MsgConsumerThread(topic, index, msgQueueMap(topic), stream)
                    index = index + 1
                    thread
                })
            }).toSeq

        val latch = new CountDownLatch(consumerThreads.length)
        consumerThreads.foreach(item => {
            item.start()
            LOG.info(s"MsgConsumerThread[${item.getName}] started.")
            latch.countDown()
        })
        latch.await()
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
     * 获取源topic及其线程数
     * @return
     */
    def getSourceTopicCountMap(topics: Seq[String], confManager: ConfManager): java.util.HashMap[String, Integer] = {

        val topicCountMap = new java.util.HashMap[String, Integer]()


        val confValue = confManager.getConfOrElseValue(this.name, "topicCount", "")

        //默认情况下，每个partition一个消费线程
        if (confValue == null || confValue.trim.length == 0) {
            topics.map(topic => {
                val metaInfos = topicMetaInfos.get(topic)
                LOG.info(s"metaInfos:${topic},${metaInfos.get.map(item => (item.partitionId, item.leader)).mkString(",")}")
                val ps = topicMetaInfos.get(topic).get.size
                topicCountMap.put(topic, ps)
            })
        } else {
            val topicAndThreadsStr = StringUtil.splitStr(confValue, ",")
            val map = new mutable.HashMap[String, Int]()
            topics.foreach(topic => {
                topicAndThreadsStr.map(str => {
                    val strValues = str.split(":")
                    var regStr = strValues(0)
                    val count = strValues(1).toInt
                    if (!regStr.startsWith("^")) regStr = "^" + regStr
                    if (!regStr.endsWith("$")) regStr = regStr + "$"
                    regStr.r.findFirstMatchIn(topic) match {
                        case Some(m) => {
                            topicCountMap.put(topic, count)
                        }
                        case None =>
                    }
                })
            })
        }

        topicCountMap
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
            LOG.info(s"set fetch offset:${topic},${groupId},${offset.mkString(",")},${ret}")
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
        if (topicMetaInfos.get(topic).isEmpty) {
            return Map[Int, Long]()
        }
        val metaInfos = topicMetaInfos.get(topic).get
        val map = metaInfos.flatMap(metaInfo => {
            val partition = metaInfo.partitionId
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
        map
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
     * 获取当前数据源topic的元数据信息
     * @return
     */
    def getTopicMetadatas(): Map[String, List[kafka.javaapi.PartitionMetadata]] = {
        topicMetaInfos
    }

    /**
     * 获取topic及其对应的消息队列
     * @return
     */
    def getTopicAndQueueMap(): Map[String, LinkedBlockingQueue[KafkaMessage]] = {
        msgQueueMap
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


    private val defaultQueueCapacity = 2000
    private var groupId: String = null
    private var consumerConnector: ConsumerConnector = null
    private var kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]] = null
    private var kafkaUtil: KafkaUtil = null
    private var topicRegexs: Seq[Regex] = null
    private var topics: Seq[String] = null
    private var topicMetaInfos: Map[String, List[kafka.javaapi.PartitionMetadata]] = null
    private var queueCapacity: Int = defaultQueueCapacity
    private var msgQueueMap: Map[String, LinkedBlockingQueue[KafkaMessage]] = null
    private var consumerThreads: Seq[MsgConsumerThread] = null
    private var logPerMsgCount = 100
    private var defaultOffset: Map[String, Long] = null


    class MsgConsumerThread(topic: String, index: Int, queue: LinkedBlockingQueue[KafkaMessage], stream: KafkaStream[Array[Byte], Array[Byte]]) extends Thread {

        @volatile private var keepRunning: Boolean = true

        val consumerTopic = topic

        this.setName(s"consumer-${topic}")

        def stopProcess(): Unit = {
            keepRunning = false
            this.interrupt()
            LOG.info(s"MsgConsumerThread[${this.getName}] stopped")
        }

        override def run(): Unit = {
            this.setName(s"${topic}/${index}")
            var count = 0
            val it = stream.iterator

            LOG.info(s"MsgConsumerThread[${this.getName}] started")
            while (keepRunning) {
                if (it.hasNext()) {
                    try {
                        queue.put(it.next())
                        count = count + 1
                        if (count % logPerMsgCount == 0) {
                            LOG.info(s"MsgConsumerThread[${this.getName}] msgCount: ${count}")
                        }
                    } catch {
                        case e: InterruptedException => {
                            LOG.info(s"${this.getName} is interrupted. keepRunning:${keepRunning}")
                            return
                        }
                    }
                }
            }
        }

        override def toString: String = {
            s"MsgConsumerThread[${this.getName}]"
        }

    }

}
