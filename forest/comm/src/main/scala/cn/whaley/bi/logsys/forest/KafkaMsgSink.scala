package cn.whaley.bi.logsys.forest

import java.net.Socket
import java.util.concurrent.Future

import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import cn.whaley.bi.logsys.forest.Traits.{LogTrait, NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest.entity.LogEntity
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{RecordMetadata, ProducerRecord, KafkaProducer}

import scala.collection.mutable

/**
 * Created by fj on 16/10/30.
 */
class KafkaMsgSink extends InitialTrait with NameTrait with LogTrait {
    type KafkaMessage = ConsumerRecord[Array[Byte], Array[Byte]]

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {

        val kafkaConf = confManager.getAllConf("kafka-producer", true)

        //实例化kafka实用工具
        bootstrapServers = kafkaConf.get("bootstrap.servers").toString
        InitKafkaUtil()
        if (kafkaUtil == null) {
            throw new Exception(s"invalid broker servers : ${bootstrapServers}")
        }

        //实例化kafka生产者
        kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](kafkaConf)

    }

    /**
     * 保存处理后的数据
     * @param datas
     */
    def saveProcMsg(datas: Seq[(String, KafkaMessage, LogEntity)]): Unit = {
        var future: Future[RecordMetadata] = null
        var count = 0
        datas.foreach(item => {
            val targetTopic: String = item._1
            val message: KafkaMessage = item._2
            val log: LogEntity = item._3
            val key: Array[Byte] = getKeyFromSource(message).getBytes()
            val value: Array[Byte] = log.toJSONString.getBytes()
            val record = new ProducerRecord[Array[Byte], Array[Byte]](targetTopic, key, value)
            future = kafkaProducer.send(record)
            count = count + 1
        })
        //最后进行一次同步确认
        if (future != null) {
            future.get()
        }
    }

    /**
     * 保存错误信息
     * @param datas
     */
    def saveErrorMsg(datas: Seq[(String, KafkaMessage)]) = {
        datas.foreach(item => {
            val errorTopic: String = item._1
            val message: KafkaMessage = item._2
            val key: Array[Byte] = getKeyFromSource(message).getBytes()
            val value: Array[Byte] = message.value()
            val record = new ProducerRecord[Array[Byte], Array[Byte]](errorTopic, key, value)
            kafkaProducer.send(record)
        })
    }

    /**
     * 从目标kafka集群中，获取某个源topic最后写入的offset信息
     * @param sourceTopic 源topic
     * @param sourceLatestOffset 源topic的最后偏移信息
     * @param targetTopic 目标topic
     * @param maxMsgCount 目标topic的每个partition最多读取的消息数量
     * @return 源topic相关的offse信（partition，offset），如果没有则返回空Map对象
     */
    def getTopicLastOffset(sourceTopic: String, sourceLatestOffset: Map[Int, Long], targetTopic: String, maxMsgCount: Int): Map[Int, Long] = {
        val offsetMap = new mutable.HashMap[Int, Long]
        val msgs = kafkaUtil.getLatestMessage(targetTopic, maxMsgCount)
        msgs.foreach(msg => {
            msg._2.map(item => {
                val strKey = new String(item.key(),"UTF-8")
                val offsetInfo = getOffsetInfoFromKey(strKey, sourceTopic)
                //LOG.info(s"key:${strKey},offsetInfo:${offsetInfo}")
                if (offsetInfo.isDefined) {
                    val partition = offsetInfo.get._1
                    val fromOffset = offsetInfo.get._2 + 1
                    val oldValue = offsetMap.getOrElse(partition, 0L)
                    val newValue = Math.max(fromOffset, oldValue)
                    val latestOffsetValue = sourceLatestOffset.getOrElse(partition, 0L)
                    val value = Math.min(newValue, latestOffsetValue)
                    offsetMap.put(partition, value)
                    //LOG.info(s"update offsetMap:${partition},${value}")
                }
            })
        })
        LOG.info(s"getTopicLastOffset msgs:${targetTopic};${maxMsgCount};${msgs.map(item => (item._1, item._2.length))};${sourceLatestOffset.mkString(",")};${offsetMap.mkString(",")}")

        offsetMap.toMap
    }

    private def getKeyFromSource(source: KafkaMessage): String = {
        s"${source.topic}/${source.partition}/${source.offset}"
    }

    private def getOffsetInfoFromKey(key: String, sourceTopic: String): Option[(Int, Long)] = {
        if (key.startsWith(sourceTopic)) {
            offsetRegex findFirstMatchIn key.substring(sourceTopic.length) match {
                case Some(m) => {
                    Some(m.group(1).toInt, m.group(2).toLong)
                }
                case None => None
            }
        } else {
            None
        }
    }

    private val offsetRegex = "/(\\d+)/(\\d+)".r
    private var kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]] = null
    private var kafkaUtil: KafkaUtil = null
    private var bootstrapServers: String = null

    private def InitKafkaUtil() = {
        val array = bootstrapServers.split(",")
        val brokerList =
            for (i <- 0 to array.length - 1) yield {
                val hostAndPort = array(i).split(":")
                try {
                    //进行一次网络测试
                    new Socket(hostAndPort(0), hostAndPort(1).toInt)
                    Some((hostAndPort(0), hostAndPort(1).toInt))
                }
                catch {
                    case e: Throwable => {
                        LOG.error(s"broker is invalid:${array(i)},test failure:${e.getMessage},${e.getCause}")
                        None
                    }
                }
            }
        val list = brokerList.filter(_.isDefined).map(_.get)
        require(!list.isEmpty)
        kafkaUtil = new KafkaUtil(list)
    }
}
