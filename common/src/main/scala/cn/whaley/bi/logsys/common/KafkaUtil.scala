package cn.whaley.bi.logsys.common

import java.util
import java.util.{Collections, Arrays}

import kafka.admin.TopicCommand.TopicCommandOptions
import kafka.api._
import kafka.common.{BrokerNotAvailableException, OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{Whitelist}
import kafka.javaapi.OffsetCommitResponse
import kafka.javaapi.OffsetFetchRequest
import kafka.javaapi.OffsetFetchResponse
import kafka.javaapi.PartitionMetadata
import kafka.javaapi.TopicMetadata
import kafka.javaapi.TopicMetadataRequest
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import kafka.message.{MessageAndOffset}
import kafka.network.BlockingChannel
import kafka.utils.{ZkUtils}
import org.I0Itec.zkclient.ZkClient

import scala.collection.JavaConversions._
import scala.collection.{immutable, Seq, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions


/**
 * Created by fj on 16/10/30.
 *
 * kafka工具类，提供分区元数据读取、offset操作、末端数据读取等功能
 * 目前实现是基于在0.8.2.2客户端库
 *
 */
class KafkaUtil(brokerList: Seq[(String, Int)], clientId: String = ConsumerMetadataRequest.DefaultClientId) {
    val soTimeout = 5000
    val bufferSize = 4096000

    def getSimpleConsumer(brokeHost: String, brokePort: Int): SimpleConsumer = {
        new SimpleConsumer(brokeHost, brokePort, soTimeout, bufferSize, clientId)
    }

    /**
     * 获取指定topic的某个时间点之前的有效offset集合
     * @param topic
     * @param whichTime 时间点，起始时间：OffsetRequest.EarliestTime | 最新时间：OffsetRequest.LatestTime
     * @param maxNumOffsets 多少个有效的offset，如果设置为大于1的值，一般能依次返回2个：【最新偏移值，起始偏移值】
     * @return partition与offset数组的map
     */
    def getOffset(topic: String, whichTime: Long, maxNumOffsets: Int): Map[Int, Array[Long]] = {
        val offsetMap = new scala.collection.mutable.HashMap[Int, Array[Long]]()
        for (broker <- brokerList) {
            var simpleConsumer: SimpleConsumer = null
            try {
                simpleConsumer = getSimpleConsumer(broker._1, broker._2)
                //获取topic的partition列表，构建每个partition的offset请求
                val request0 = new TopicMetadataRequest(Arrays.asList(topic))
                val response0 = simpleConsumer.send(request0)
                val it = response0.topicsMetadata.get(0).partitionsMetadata.iterator()
                val requestInfo = new util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]()
                while (it.hasNext) {
                    val curr = it.next()
                    val topicAndPartition = new TopicAndPartition(topic, curr.partitionId)
                    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, maxNumOffsets))
                }

                //获取每个partition的offset
                val request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientId)

                val response = simpleConsumer.getOffsetsBefore(request)
                val responseInfoIt = requestInfo.keySet().iterator()
                while (responseInfoIt.hasNext) {
                    val curr = responseInfoIt.next()
                    val offset = response.offsets(curr.topic, curr.partition)
                    if (offset.length > 0) {
                        offsetMap.put(curr.partition, response.offsets(curr.topic, curr.partition))
                    }
                    //println(s"KafkaUtil.getOffset:${topic};${curr.partition};${response.offsets(curr.topic, curr.partition).mkString(",")}")
                }
            } finally {
                if (simpleConsumer != null) {
                    simpleConsumer.close()
                }
            }
        }
        offsetMap.toMap
    }

    /**
     * 获取指定topic的各个partition最新的offset
     * @param topic
     * @return partition与offset的map
     */
    def getLatestOffset(topic: String): Map[Int, Long] = {
        val ret = getOffset(topic, OffsetRequest.LatestTime, 1)
        println(s"KafkaUtil.latestOffset: ${topic};${ret.map(item => (item._1, item._2.mkString(","))).mkString(";")}")
        ret.filter(!_._2.isEmpty).map(item => (item._1, item._2(0)))
    }

    /**
     * 获取指定topic的各个partition最早的offset
     * @param topic
     * @return partition与offset的map
     */
    def getEarliestOffset(topic: String): Map[Int, Long] = {
        val ret = getOffset(topic, OffsetRequest.EarliestTime, 1)
        println(s"KafkaUtil.earliestOffset: ${topic};${ret.map(item => (item._1, item._2.mkString(","))).mkString(";")}")
        ret.filter(!_._2.isEmpty).map(item => (item._1, item._2(0)))
    }

    /**
     * 获取消费组对特定topic的消费偏移
     * @param topic
     * @param clientId
     * @return
     */
    def getFetchOffset(topic: String, groupId: String, clientId: String = clientId): Map[Int, Long] = {
        val offsets = new mutable.HashMap[Int, Long]
        for (broker <- brokerList) {
            var simpleConsumer: SimpleConsumer = null
            try {
                simpleConsumer = getSimpleConsumer(broker._1, broker._2)
                val topicAndPartitions = new util.ArrayList[TopicAndPartition]
                val partitionMetadata = getPartitionMetadata(topic)
                partitionMetadata.foreach(item => {
                    topicAndPartitions.add(new TopicAndPartition(topic, item.partitionId));
                })
                val request: OffsetFetchRequest = new OffsetFetchRequest(groupId, topicAndPartitions, ConsumerMetadataRequest.CurrentVersion, 0, clientId)
                val response: OffsetFetchResponse = simpleConsumer.fetchOffsets(request)

                val offsetsIt = response.offsets.keySet().iterator()
                while (offsetsIt.hasNext) {
                    val curr = offsetsIt.next()
                    offsets.put(curr.partition, response.offsets.get(curr).offset);
                }
            }
            finally {
                if (simpleConsumer != null) {
                    simpleConsumer.close()
                }
            }
        }
        return offsets.toMap
    }

    /**
     * 设置消费组对特定topic的消费偏移
     * @param topic
     * @param groupId
     * @param offsets partition及其offset
     * @return （是否全部成功，（partition,status))
     */
    def setFetchOffset(topic: String, groupId: String, offsets: Map[Int, Long]): (Boolean, Map[Int, Short]) = {

        var corId = 0
        val version = ConsumerMetadataRequest.CurrentVersion
        val clientId = this.clientId

        val partitionMetadatas = getPartitionMetadata(topic)

        def doSetOffset(metadata: PartitionMetadata): (Boolean, Short) = {

            val offsetMap = new util.HashMap[TopicAndPartition, OffsetAndMetadata]
            val tp = new TopicAndPartition(topic, metadata.partitionId)
            if (offsets.get(metadata.partitionId).isDefined) {
                val offset = offsets.get(metadata.partitionId).get
                offsetMap.put(tp, new OffsetAndMetadata(offset))
            }
            val offsetCommitRequest = new kafka.javaapi.OffsetCommitRequest(groupId, offsetMap, corId, clientId, version)
            corId = corId + 1

            val channel = new BlockingChannel(metadata.leader.host, metadata.leader.port, BlockingChannel.UseDefaultBufferSize, BlockingChannel.UseDefaultBufferSize, 5000)
            channel.connect
            channel.send(offsetCommitRequest.underlying)
            val offsetCommitResponse: OffsetCommitResponse = OffsetCommitResponse.readFrom(channel.receive.buffer)
            channel.disconnect

            if (offsetCommitResponse.hasError) {
                (false, offsetCommitResponse.errors.get(tp))
            } else {
                (true, 0)
            }
        }

        var isOK = true
        val result = new mutable.HashMap[Int, Short]()
        partitionMetadatas.foreach(item => {
            val ret = doSetOffset(item)
            isOK = isOK && ret._1
            result.put(item.partitionId, ret._2)
        })

        (isOK, result.toMap)
    }

    /**
     * 从某个topic最后的开始，往前读取一定数量的消息
     * @param topic
     * @param msgCount 从topic的每个partition最末端往前读取的消息数量
     * @param msgAvgSize 消息体平均大小, 合理调整大小，在读取次数和每次读取的字节数之间平衡
     * @param maxFetchSize 最多读取的数据大小，默认50M
     * @return （partition,MessageAndOffset集合）
     */
    def getLatestMessage(topic: String, msgCount: Int, msgAvgSize: Int = 2048, maxFetchSize: Int = 1024 * 1024 * 50): Map[Int, Array[kafka.message.MessageAndOffset]] = {
        val latestOffset = this.getLatestOffset(topic)
        val earliestOffset = this.getEarliestOffset(topic);
        val targetOffsets = latestOffset.map(item => (item._1, item._2 - msgCount))
        var sizeFactor = 1

        println(s"KafkaUtil.getLatestMessage:${topic};${earliestOffset.mkString(",")};${latestOffset.mkString(",")}")

        val metadatas = getPartitionMetadata(topic)

        targetOffsets.map(item => {
            val partition = item._1
            val earliest = earliestOffset.get(partition).get
            val latest = latestOffset.get(partition).get
            //确保fromOffset是一个有效值
            val fromOffset = if (item._2 < earliest) {
                earliest
            } else {
                item._2
            }

            var simpleConsumer: SimpleConsumer = null
            try {
                val leader = metadatas.filter(_.partitionId == partition).map(_.leader).head
                simpleConsumer = getSimpleConsumer(leader.host, leader.port)
                val v: Long = (latest - fromOffset) * msgAvgSize
                val fetchSize: Int = if (v > maxFetchSize) maxFetchSize else v.toInt

                val messages =
                    if (fetchSize > 0) {
                        val buf = new ArrayBuffer[MessageAndOffset]()
                        var nextOffset = fromOffset
                        while (nextOffset < latest) {
                            val req = new FetchRequestBuilder()
                                .clientId(clientId)
                                .addFetch(topic, partition, nextOffset, fetchSize * sizeFactor)
                                .build();
                            val fetchResponse: kafka.javaapi.FetchResponse = simpleConsumer.fetch(req);
                            if (fetchResponse.hasError) {
                                throw new Exception(s"topic:${topic},partition:{${partition}},errorCode:${fetchResponse.errorCode(topic, partition)}")
                            }
                            val fetched = fetchResponse.messageSet(topic, partition)

                            //如果没有读到消息，则很可能是fetchSize设置过小，不足以消费一个消息，此时需要调整sizeFactor
                            val size = fetched.size
                            if (size <= 0) {
                                sizeFactor = Math.pow(2, sizeFactor).toInt
                            } else {
                                fetched.foreach(item => {
                                    nextOffset = item.nextOffset
                                    if (item.offset >= fromOffset && item.offset <= latest) {
                                        buf.append(item)
                                    }
                                })
                            }
                        }
                        buf.toArray
                    } else {
                        KafkaUtil.emptyMessageAndOffsets
                    }
                (partition, messages)
            } finally {
                if (simpleConsumer != null) {
                    simpleConsumer.close()
                }
            }
        })
    }

    /**
     * 获取特定topic的partition信息
     * @param topic
     * @return
     */
    def getPartitionMetadata(topic: String): List[PartitionMetadata] = {
        val infos = new util.HashMap[Int, PartitionMetadata]()
        for (broker <- brokerList) {
            var consumer: SimpleConsumer = null;
            try {
                consumer = new SimpleConsumer(broker._1, broker._2, 100000, 64 * 1024, "leaderLookup");
                val topics = Collections.singletonList(topic);
                val req = new TopicMetadataRequest(topics);
                val resp = consumer.send(req);
                val metaData = resp.topicsMetadata;
                for (item <- metaData) {
                    for (partitionMetadata <- item.partitionsMetadata)
                        infos.put(partitionMetadata.partitionId, partitionMetadata)
                }
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        infos.map(_._2).toList
    }


    /*
    def setFetchOffset(topic: String, groupId: String, offsets: Map[Int, Long], clientId: String = clientId): OffsetCommitResponse = {
        val requestInfo = new util.HashMap[TopicAndPartition, OffsetAndMetadata]()
        offsets.foreach(item => {
            requestInfo.put(new TopicAndPartition(topic, item._1), new OffsetAndMetadata(item._2))
        })
        val request = new kafka.javaapi.OffsetCommitRequest(groupId,
            requestInfo,
            0,
            clientId,
            OffsetCommitRequest.CurrentVersion)
        val res = simpleConsumer.commitOffsets(request)

        res
    }

    def getOffsetBefore(topic: String, clientId: String = clientId): Map[Int, Array[Long]] = {
        val version = OffsetCommitRequest.CurrentVersion
        val partitions = getPartitionMetadata(topic).map(item => item.partitionId)

        val requestInfo = new util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]()
        partitions.map(item => {
            requestInfo.put(new TopicAndPartition(topic, item), new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 10))
        }).toArray

        val request = new kafka.javaapi.OffsetRequest(requestInfo, version, clientId)
        val res = simpleConsumer.getOffsetsBefore(request)
        partitions.map(item => {
            (item, res.offsets(topic, item))
        }).toMap
    }

    */

}

object KafkaUtil {
    val emptyMessageAndOffsets = new ArrayBuffer[kafka.message.MessageAndOffset]().toArray

    //从zookeeper连接构建KafkaUtil
    def apply(zkServers: String, clientId: String = ConsumerMetadataRequest.DefaultClientId): KafkaUtil = {
        val zkClient = new ZkClient(zkServers, 30000, 30000, kafka.utils.ZKStringSerializer)
        val brokerList = getAllBrokerInfo(zkClient)
        new KafkaUtil(brokerList)
    }

    /**
     * 获取所有broker节点的主机信息
     * @param zkClient
     * @return
     */
    def getAllBrokerInfo(zkClient: ZkClient): Seq[(String, Int)] = {
        val ids = ZkUtils.getChildren(zkClient, ZkUtils.BrokerIdsPath)
        ids.map(id => {
            getBrokerInfo(zkClient, id.toInt)
        }).filter(_.isDefined).map(_.get)
    }


    /**
     * 获取特定broker节点的主机信息
     * @param zkClient
     * @param bid
     * @return
     */
    def getBrokerInfo(zkClient: ZkClient, bid: Int): Option[(String, Int)] = {

        ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + bid)._1 match {
            case Some(brokerInfoString) =>
                kafka.utils.Json.parseFull(brokerInfoString) match {
                    case Some(m) =>
                        val brokerInfo = m.asInstanceOf[Map[String, Any]]
                        val host = brokerInfo.get("host").get.asInstanceOf[String]
                        val port = brokerInfo.get("port").get.asInstanceOf[Int]
                        Some((host, port))
                    case None =>
                        throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
                }
            case None =>
                throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
        }

    }


    /**
     * 获取kafka集群topic列表
     * @return
     */
    def getTopics(zkClient: ZkClient): Seq[String] = {
        val opts = new TopicCommandOptions(Array("list"))
        val allTopics = ZkUtils.getAllTopics(zkClient).sorted
        if (opts.options.has(opts.topicOpt)) {
            val topicsSpec = opts.options.valueOf(opts.topicOpt)
            val topicsFilter = new Whitelist(topicsSpec)
            allTopics.filter(topicsFilter.isTopicAllowed(_, excludeInternalTopics = false))
        } else
            allTopics
    }

    def getTopics(zkServers: String): Seq[String] = {
        getTopics(new ZkClient(zkServers))
    }

}
