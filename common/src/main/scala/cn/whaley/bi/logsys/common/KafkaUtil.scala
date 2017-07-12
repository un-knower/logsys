package cn.whaley.bi.logsys.common

import java.util.{Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata, KafkaConsumer}

import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.{Seq, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions


/**
 * Created by fj on 16/10/30.
 *
 * kafka工具类，提供分区元数据读取、offset操作、末端数据读取等功能
 * 目前实现是基于在0.10.1.0客户端库
 *
 */
class KafkaUtil(brokerList: Seq[(String, Int)], clientId: String = "KafkaUtil") {
    val soTimeout = 5000
    val bufferSize = 4096000

    var defaultConsumer: Option[KafkaConsumer[Array[Byte], Array[Byte]]] = None

    def getDefaultConsumer(): KafkaConsumer[Array[Byte], Array[Byte]] = {
        if (defaultConsumer.isEmpty) {
            val consumer = createConsumer(clientId);
            defaultConsumer = Some(consumer);
        }
        defaultConsumer.get
    }

    def createConsumer(groupId: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
        val props = new Properties()
        val servers = brokerList.map(item => item._1 + ":" + item._2).mkString(",")
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        new KafkaConsumer[Array[Byte], Array[Byte]](props)
    }

    /**
     * 获取特定topic分区中的,从offset开始的第一条消息
     * @param topic
     * @param partition
     * @param offset
     * @return
     */
    def getFirstMsg(topic: String, partition: Int, offset: Long): Option[ConsumerRecord[Array[Byte],Array[Byte]]] = {
        val consumer = getDefaultConsumer()
        consumer.assign(new TopicPartition(topic, partition)::Nil)
        consumer.seek(new TopicPartition(topic, partition), offset)
        val records = consumer.poll(10000)
        while(records.iterator().hasNext){
            val record= records.iterator().next()
            return Some(record);
        }
        return None
    }


    /**
     * 获取topic名称列表
     * @return
     */
    def getTopics(): Seq[String] = {
        val consumer = getDefaultConsumer()
        consumer.listTopics().map(item => item._1).toSeq
    }

    /**
     * 获取partition信息列表
     * @return
     */
    def getPartitionInfo(topic: String): List[PartitionInfo] = {
        val consumer = getDefaultConsumer()
        consumer.listTopics().filter(item => item._1 == topic).map(item => item._2.toList).toList.get(0)
    }

    /**
     * 获取指定topic的各个partition最新的offset
     * @param topic
     * @return partition与offset的map
     */
    def getLatestOffset(topic: String): Map[Int, Long] = {
        val consumer = getDefaultConsumer()
        val topicAndPartitions = consumer.partitionsFor(topic).map(item => new TopicPartition(topic, item.partition())).toList
        val offset = consumer.endOffsets(topicAndPartitions.asJavaCollection)
        val ret = offset.map(item => (item._1.partition(), item._2.toLong)).toMap
        ret
    }

    /**
     * 获取指定topic的各个partition最早的offset
     * @param topic
     * @return partition与offset的map
     */
    def getEarliestOffset(topic: String): Map[Int, Long] = {
        val consumer = getDefaultConsumer()
        val topicAndPartitions = consumer.partitionsFor(topic).map(item => new TopicPartition(topic, item.partition()))
        val offset = consumer.beginningOffsets(topicAndPartitions)
        offset.map(item => (item._1.partition(), item._2.toLong)).toMap
    }

    /**
     * 获取消费组对特定topic的消费偏移
     * @param topic
     * @return
     */
    def getFetchOffset(topic: String, groupId: String, consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null): Map[Int, Long] = {
        val consumerObj = if (consumer == null) {
            createConsumer(groupId)
        } else {
            consumer
        }
        if (consumer == null) {
            consumerObj.subscribe(java.util.Arrays.asList(topic))
        }
        val offset = consumerObj.assignment().filter(_.topic() == topic).map(item => {
            (item.partition(), consumerObj.position(item))
        }).toMap
        if (consumer == null) {
            consumerObj.close()
        }
        offset
    }

    /**
     * 设置消费组对特定topic的消费偏移
     * @param topic
     * @param groupId
     * @param offsets partition及其offset
     * @return （是否全部成功，（partition,status))
     */
    def setFetchOffset(topic: String, groupId: String, offsets: Map[Int, Long], consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null): Unit = {
        val consumerObj = if (consumer == null) createConsumer(groupId) else consumer
        //过滤consumer监听的分区
        val partitions = consumerObj.assignment().filter(_.topic() == topic)
        val reqOffset = offsets.filter(offset => {
            partitions.exists(p => p.partition() == offset._1)
        }).map(item => (new TopicPartition(topic, item._1), new OffsetAndMetadata(item._2)))

        if (reqOffset.size > 0) {
            consumerObj.commitSync(reqOffset)
            if (consumer == null) {
                consumerObj.close()
            }
        }

    }

    /**
     * 从某个topic最后的开始，往前读取一定数量的消息
     * @param topic
     * @param msgCount 从topic的每个partition最末端往前读取的消息数量
     * @param msgAvgSize 消息体平均大小, 合理调整大小，在读取次数和每次读取的字节数之间平衡
     * @param maxFetchSize 最多读取的数据大小，默认50M
     * @return （partition,MessageAndOffset集合）
     */
    def getLatestMessage(topic: String, msgCount: Int, msgAvgSize: Int = 2048, maxFetchSize: Int = 1024 * 1024 * 50): Map[Int, List[ConsumerRecord[Array[Byte], Array[Byte]]]] = {
        val latestOffset = this.getLatestOffset(topic)
        val earliestOffset = this.getEarliestOffset(topic);

        val fromOffsets = latestOffset.map(item => {
            val pos = item._2 - msgCount
            val earliest = earliestOffset.get(item._1).get
            (item._1, Math.max(pos, earliest))
        })

        val groupId = clientId + "-" + System.currentTimeMillis()
        val consumer = createConsumer(groupId)
        this.getPartitionInfo(topic).map(item => {
            val topicPartition = new TopicPartition(topic, item.partition())
            val cur = fromOffsets.get(topicPartition.partition()).get
            if (!consumer.assignment().contains(topicPartition)) {
                consumer.assign(topicPartition :: Nil)
            }
            consumer.seek(topicPartition, cur)
        })

        val result = this.getPartitionInfo(topic).map(item => {
            val topicPartition = new TopicPartition(topic, item.partition())
            val last = latestOffset.get(topicPartition.partition()).get
            var cur = fromOffsets.get(topicPartition.partition()).get
            val recordBuf = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]()
            var loop = true
            while (recordBuf.size <= msgCount && loop) {
                val records = consumer.poll(10000)
                if (records.count() > 0) {
                    cur = records.map(record => record.offset()).max
                    recordBuf.appendAll(records)
                }
                loop = cur < (last - 1) && records.count() > 0
            }
            (topicPartition.partition(), recordBuf.toList)
        }).toMap
        consumer.close()
        result
    }

}


object KafkaUtil {

    def apply(brokers: String, clientId: String = ""): KafkaUtil = {
        val bootstrap = brokers.split(",").map(server => {
            val vals = server.split(":")
            (vals(0), vals(1).toInt)
        }).toSeq
        if (clientId == "") {
            new KafkaUtil(bootstrap)
        } else {
            new KafkaUtil(bootstrap, clientId)
        }
    }
}