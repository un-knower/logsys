package cn.whaley.bi.logsys.forest


import java.util.concurrent.LinkedBlockingQueue

import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import cn.whaley.bi.logsys.forest.Traits.{LogTrait, NameTrait, InitialTrait}
import kafka.consumer.KafkaStream
import kafka.javaapi.consumer.ConsumerConnector
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.util.matching.Regex

/**
 * Created by fj on 16/10/30.
 *
 * 消息源，以队列的方式对外提供数据
 */
class KafkaMsgSource extends InitialTrait with NameTrait with LogTrait {

    type KafkaMessage = MessageAndMetadata[Array[Byte], Array[Byte]]


    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {

        topicRegexs = StringUtil.splitStr(confManager.getConf(this.name, "topics"), ",").map(item => item.r)
        queueCapacity = confManager.getConfOrElseValue(this.name, "queueCapacity", defaultQueueCapacity.toString).toInt
        logPerMsgCount = confManager.getConfOrElseValue(this.name, "logPerMsgCount", "500").toInt


        //实例化kafka生产者
        val producerConf = confManager.getAllConf("kafka-producer", true)
        kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](producerConf)

        //实例化kafka消费者
        val consumerConf = confManager.getAllConf("kafka-consumer", true)
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(consumerConf))

        //通过正则表达式过滤需要订阅的topic列表
        val zkServers = consumerConf.get("zookeeper.connect").toString
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

        //kafkaUtil
        groupId = consumerConf.getProperty("group.id")
        kafkaUtil = KafkaUtil(zkServers)

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
        val map = new java.util.HashMap[String, Integer]()
        topics.foreach(item => map.put(item, 3))
        val streams = consumerConnector.createMessageStreams(map)

        //特定topic的特定partition对应一个线程
        consumerThreads =
            streams.flatMap(streamItem => {
                val topic = streamItem._1
                val streamList = streamItem._2
                var index = 0
                streamList.map(stream => {
                    val thread = new MsgConsumerThread(topic, index, msgQueueMap(topic), stream)
                    index = index + 1
                    thread
                })
            }).toSeq

        consumerThreads.foreach(item => {
            item.start()
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
            kafkaUtil.setFetchOffset(topic, groupId, offset)
        })
    }

    /**
     * 获取当前数据源topic的元数据信息
     * @return
     */
    def getTopicMetadatas(): immutable.Map[String, List[kafka.javaapi.PartitionMetadata]] = {
        val metadatas =
            topics.map(topic => {
                (topic, kafkaUtil.getPartitionMetadata(topic))
            }).toMap
        metadatas
    }


    /**
     * 保存错误信息
     * @param datas
     */
    def saveErrorMsg(datas: Seq[(String, KafkaMessage)]) = {
        datas.foreach(item => {
            val errorTopic: String = item._1
            val message: KafkaMessage = item._2
            val key: Array[Byte] = message.key()
            val value: Array[Byte] = message.message()
            val record = new ProducerRecord[Array[Byte], Array[Byte]](errorTopic, key, value)
            kafkaProducer.send(record)
        })
    }

    /**
     * 获取topic及其对应的消息队列
     * @return
     */
    def getTopicAndQueueMap(): Map[String, LinkedBlockingQueue[KafkaMessage]] = {
        msgQueueMap
    }


    private val defaultQueueCapacity = 2000
    private var groupId: String = null
    private var consumerConnector: ConsumerConnector = null
    private var kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]] = null
    private var kafkaUtil: KafkaUtil = null
    private var topicRegexs: Seq[Regex] = null
    private var topics: Seq[String] = null
    private var queueCapacity: Int = defaultQueueCapacity
    private var msgQueueMap: Map[String, LinkedBlockingQueue[KafkaMessage]] = null
    private var consumerThreads: Seq[MsgConsumerThread] = null
    private var logPerMsgCount = 100


    class MsgConsumerThread(topic: String, index: Int, queue: LinkedBlockingQueue[KafkaMessage], stream: KafkaStream[Array[Byte], Array[Byte]]) extends Thread {
        override def run(): Unit = {
            this.setName(s"${topic}/${index}")
            var count = 0
            val it = stream.iterator
            LOG.info(s"MsgConsumerThread[${this.getName}] started")
            while (it.hasNext) {
                count = count + 1
                if (count % logPerMsgCount == 0) {
                    LOG.info(s"MsgConsumerThread[${this.getName}] msgCount: ${count}")
                }
                queue.put(it.next())
            }
        }

        override def toString: String = {
            s"MsgConsumerThread[${this.getName}]"
        }

    }

}
