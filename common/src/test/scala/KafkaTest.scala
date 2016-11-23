import java.util
import java.util.concurrent.{LinkedBlockingQueue, Executors, Future, TimeUnit}

import cn.whaley.bi.logsys.common.{ConfManager, KafkaUtil}
import kafka.api.OffsetRequest
import kafka.consumer.KafkaStream
import kafka.message.MessageAndMetadata
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata, KafkaProducer}

import org.junit.Test

import scala.collection.mutable.ArrayBuffer


/**
 * Created by fj on 16/10/29.
 */
class KafkaTest extends LogTrait with TimeConsumeTrait {

    val kafkaBrokerHost = "localhost"
    val kafkaBrokerPort = 9092
    val clientId = "test"
    val topic = "test2"
    val groupId = "test"

    val confManager = new ConfManager("kafka-consumer.xml" :: "kafka-producer.xml" :: Nil)


    def getKafkaProducer[K, V]: KafkaProducer[K, V] = {
        val conf = confManager.getAllConf("kafka-producer", true)
        new KafkaProducer[K, V](conf)
    }


    def getKafkaConsumerConnector(grpId: String = groupId) = {
        val conf = confManager.getAllConf("kafka-consumer", true)
        conf.put("group.id", grpId)
        val consumerConf = new kafka.consumer.ConsumerConfig(conf)
        kafka.consumer.Consumer.createJavaConsumerConnector(consumerConf)
    }

    @Test
    def testCreateKafkaUtil: Unit = {
        val topic = "pre-boikgpokn78sb95kjhfrendo8dc5mlsr"
        val zkServers = "localhost:2181"

        println(KafkaUtil.getTopics(zkServers).mkString(","))

        val util = KafkaUtil(zkServers)
        var offset = util.getEarliestOffset("pre-boikgpokn78sb95kjhfrendo8dc5mlsr")
        println(offset)

        offset = util.getEarliestOffset("11")
        println(offset)
    }

    @Test
    def testProducer1: Unit = {
        val producer = getKafkaProducer[Array[Byte], Array[Byte]]
        val chars = "012345678ABCDEFabcdef"

        val threadCount = 4
        val msgCount = 100000
        val msgThreadCount = msgCount / threadCount
        LOG.info("msgThreadCount:{},threadCount:{}", msgThreadCount, threadCount)


        val offsetMap = new util.HashMap[Int, Long]

        val executor = Executors.newFixedThreadPool(threadCount)
        for (j <- 1 to threadCount) {
            val runnable = new Runnable {
                override def run(): Unit = {
                    val from = System.currentTimeMillis()
                    for (i <- 1 to msgThreadCount) {
                        val random = new scala.util.Random(System.currentTimeMillis())
                        val bytes = new ArrayBuffer[Byte]()
                        for (k <- 1 to 1000) {
                            val index = random.nextInt(chars.length - 1)
                            bytes.append(chars(index).toByte)
                        }

                        val key: Array[Byte] = i.toString.getBytes
                        val value: Array[Byte] = bytes.toArray
                        val record: ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
                        val future: Future[RecordMetadata] = producer.send(record)
                        //future.get()
                        /*
                        if (i >= msgThreadCount - 4 * 2) {
                            val metadata: RecordMetadata = future.get(10, TimeUnit.SECONDS)
                            offsetMap.put(metadata.partition(), metadata.offset())
                            LOG.info("thread {} last msg: [{},{},{}]", j.toString, metadata.topic, metadata.partition.toString, metadata.offset.toString)
                        }
                        */
                    }
                    val ts = System.currentTimeMillis() - from
                    LOG.info("thread:{}, ts:{}", j, ts.toString)
                }
            }
            executor.submit(runnable)
        }
        executor.shutdown()
        executor.awaitTermination(10, TimeUnit.SECONDS)
        LOG.info("completed")

        LOG.info("offsetMap:{}", offsetMap)
        Thread.sleep(1000 * 1000)

        /*  同一个字符串
        100000 4个分区，4线程 1k大小
        None: （同步）ts:10507 25M|(异步）ts:1802 25M
        gzip:（同步）ts:17977 1.7M|(异步）ts:4449 541K
        snappy:（同步）ts:11756 3.1M|(异步）ts:1501 2.2M

        100000 4个分区，1线程 1k大小
        None: （同步）ts:18632 25M |(异步） ts: 1670 25M
        gzip: （同步）ts:31514 1.9M  |(异步） ts: 5304 ~ 6367 730K
        snappy: ts:22291 3.1M
         */

        /* 随机字符串
        None: （同步）ts:10839 25M|(异步）ts:4736 25M
        gzip:（同步）ts:22364 14M|(异步）ts:9832 6.8M
        snappy:（同步）ts:13308 26M|(异步）ts:5266 25MM

        */

    }

    @Test
    def testProducer2: Unit = {
        val producer = getKafkaProducer[Array[Byte], Array[Byte]]

        val topic = "pre-boikgpokn78sb95kjhfrendo8dc5mlsr"
        val source = scala.io.Source.fromFile("/Users/fj/workspace/whaley/projects/WhaleyLogSys/forest/comm/src/test/resources/boikgpokn78sb95kjhfrendo8dc5mlsr.log")
        val filelines = source.getLines().toArray

        for (i <- 1 to filelines.length - 1) {
            val key: Array[Byte] = i.toString.getBytes
            val value: Array[Byte] = filelines(i).getBytes()
            val record: ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
            val future: Future[RecordMetadata] = producer.send(record)
            future.get()
        }
    }


    @Test
    def testConsumer1: Unit = {



        val connector = getKafkaConsumerConnector()
        val map = new java.util.HashMap[String, Integer]
        map.put(topic, 1)
        val streams = connector.createMessageStreams(map)
        val stream: KafkaStream[Array[Byte], Array[Byte]] = streams.get(topic).get(0)
        val queue = new LinkedBlockingQueue[MessageAndMetadata[Array[Byte], Array[Byte]]]()

        val thread = new Thread() {
            override def run: Unit = {
                val it = stream.iterator
                while (it.hasNext) {
                    queue.put(it.next())
                }
            }
        }
        thread.start()

        val from = System.currentTimeMillis()
        var c = 0
        var running = true
        val offsetMap = new util.HashMap[Int, Long]
        while (running) {
            val element = queue.poll(2, TimeUnit.SECONDS)

            if (element == null) {
                running = false
                LOG.info("ended : {},ts：{}", c, System.currentTimeMillis() - from)
            } else {
                offsetMap.put(element.partition, element.offset)
                c = c + 1
                if (c % 10000 == 0) {
                    LOG.info("{},ts：{}", c, System.currentTimeMillis() - from)
                }
            }
        }

        LOG.info("offsetMap:{}", offsetMap)
        connector.shutdown()

        //offsetMap:{0=136293, 1=135999, 2=139102, 3=138824}
    }


    @Test
    def testKafkaUtil_getOffset: Unit = {

        val util = new KafkaUtil(kafkaBrokerHost, kafkaBrokerPort, "test")
        val earliestOffset = util.getEarliestOffset(topic)
        val latestOffset1 = util.getLatestOffset(topic)
        val latestOffset2 = util.getOffset(topic, OffsetRequest.LatestTime, 2).map(item => s"${item._1}->${item._2.mkString(",")}").mkString(" , ")
        val latestOffset3 = util.getOffset(topic, OffsetRequest.LatestTime, 3).map(item => s"${item._1}->${item._2.mkString(",")}").mkString(" , ")
        LOG.info("earliestOffset: {} , {}", topic, earliestOffset, "")
        LOG.info("latestOffset1: {} , {}", topic, latestOffset1, "")
        LOG.info("latestOffset2: {} , {}", topic, latestOffset2, "")
        LOG.info("latestOffset3: {} , {}", topic, latestOffset3, "")

        val count = 10000
        val ts1 = timeConsumeTest(count, () => util.getLatestOffset(topic))
        LOG.info("latestOffset:count:{},ts:{}", count, ts1)

        val ts2 = timeConsumeTest(count, () => util.getEarliestOffset(topic))
        LOG.info("earliestOffset:count:{},ts:{}", count, ts2)
    }

    @Test
    def testKafkaUtil_setFetchOffset: Unit = {
        val util = new KafkaUtil(kafkaBrokerHost, kafkaBrokerPort, clientId)
        val offsets = Map(0 -> 100L, 1 -> 0L, 2 -> 0L, 3 -> 0L)
        val res = util.setFetchOffset(topic, groupId, offsets)
        if (!res._1) {
            LOG.error("{}", res._2)
        }

        val offsets2 = util.getFetchOffset(topic, groupId)
        LOG.info("{} \n {}", offsets, offsets2, "")

        require(offsets == offsets2)
    }

    @Test
    def testKafkaUtil_setFetchOffsetAndConsumer: Unit = {
        val util = new KafkaUtil(kafkaBrokerHost, kafkaBrokerPort, clientId)
        val latestOffset = util.getLatestOffset(topic)

        val size = 1000
        val targetOffsets = latestOffset.map(item => (item._1, item._2 - size))

        val beforeGroupOffset = util.getFetchOffset(topic, groupId)
        val ret = util.setFetchOffset(topic, groupId, targetOffsets)
        println("setFetchOffset:" + ret)
        if (!ret._1) {
            return
        }
        val afterGroupOffset = util.getFetchOffset(topic, groupId)

        println("latestOffset:" + latestOffset)
        println("beforeGroupOffset:" + beforeGroupOffset)
        println("targetOffsets:" + targetOffsets)
        println("afterGroupOffset:" + afterGroupOffset)

        val connector = getKafkaConsumerConnector()
        val map = new java.util.HashMap[String, Integer]
        map.put(topic, 1)
        val streams = connector.createMessageStreams(map)
        val stream: KafkaStream[Array[Byte], Array[Byte]] = streams.get(topic).get(0)
        val queue = new LinkedBlockingQueue[MessageAndMetadata[Array[Byte], Array[Byte]]]()
        val buf = new ArrayBuffer[MessageAndMetadata[Array[Byte], Array[Byte]]]()

        val it = stream.iterator
        var endCount = 0
        while (endCount != latestOffset.size && it.hasNext) {
            val curr = it.next()
            buf.append(curr)
            if (buf.size % 100 == 0) {
                println(s"${buf.size}:${curr.partition}:${curr.offset}:${latestOffset(curr.partition)}:${endCount}")
            }
            if (curr.offset + 1 >= latestOffset(curr.partition)) {
                endCount = endCount + 1
            }
        }
        connector.commitOffsets()
        println(s"${buf.size}:${buf.last.offset}")

    }

    @Test
    def TestKafkaUtil_getLatestMessage: Unit = {
        val util = new KafkaUtil(kafkaBrokerHost, kafkaBrokerPort)
        val latestOffset = util.getLatestOffset(topic)
        println(latestOffset)

        val datas = util.getLatestMessage(topic, 10)

        datas.foreach(item => {
            val partiton = item._1
            var count = 0
            item._2.foreach(msgAndOffset => {
                count = count + 1
                val message = msgAndOffset.message
                val keyBytes = new Array[Byte](message.keySize)
                val valueBytes = new Array[Byte](message.payloadSize)
                message.key.get(keyBytes)
                message.payload.get(valueBytes)
                val key = new String(keyBytes)
                val value = new String(valueBytes)
                println(s"[${partiton}:${count}:${msgAndOffset.offset}:${msgAndOffset.nextOffset}}]:${key}:${value}")
            })
        })
    }

}
