import java.util
import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, Executors, Future, TimeUnit}

import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import org.apache.kafka.clients.consumer.KafkaConsumer


import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata, KafkaProducer}
import org.apache.kafka.common.TopicPartition

import org.junit.Test

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._


/**
 * Created by fj on 16/10/29.
 */
class KafkaTest extends LogTrait with TimeConsumeTrait {

    val kafkaBrokerHost = "bigdata-appsvr-130-1"
    val kafkaBrokerPort = 9094
    val clientId = "test"
    val topic = "medusa-pre-log"
    val groupId = "test"
    val servers = "bigdata-appsvr-130-1:9094,bigdata-appsvr-130-2:9094,bigdata-appsvr-130-3:9094,bigdata-appsvr-130-4:9094,bigdata-appsvr-130-5:9094,bigdata-appsvr-130-6:9094"


    val confManager = new ConfManager("settings.properties" :: "kafka-consumer.xml" :: "kafka-producer.xml" :: Nil)

    def getKafkaUtil(): KafkaUtil = {
        /*
        val kafkaUtil = new KafkaUtil(
            ("bigdata-appsvr-130-1", 9094)
                ::("bigdata-appsvr-130-2", 9094)
                ::("bigdata-appsvr-130-3", 9094)
                ::("bigdata-appsvr-130-4", 9094)
                ::("bigdata-appsvr-130-5", 9094)
                ::("bigdata-appsvr-130-6", 9094)
                :: Nil)
        */
        val kafkaUtil = KafkaUtil(confManager.getConf("kafka-producer.bootstrap.servers"))
        kafkaUtil
    }


    def getKafkaProducer[K, V]: KafkaProducer[K, V] = {
        val conf = confManager.getAllConf("kafka-producer", true)
        new KafkaProducer[K, V](conf)
    }

    def getKafkaConsumer[K, V](): KafkaConsumer[K, V] = {
        val conf = confManager.getAllConf("kafka-consumer", true)
        new KafkaConsumer[K, V](conf)
    }

    @Test
    def testConsumer(): Unit = {
        val topic = "test"
        val consumer = getKafkaConsumer[Array[Byte], Array[Byte]]()
        consumer.subscribe(topic :: Nil)
        while (true) {

            val records = consumer.poll(1000)
            records.foreach(record => {
                val msg = new String(record.value())
                println(msg)
            })

            Thread.sleep(1000)
        }
    }


    @Test
    def testCreateKafkaUtil: Unit = {
        val topic = "pre-boikgpokn78sb95kjhfrendo8dc5mlsr"
        val kafkaUtil = new KafkaUtil((kafkaBrokerHost, kafkaBrokerPort) :: Nil)
        val topics = kafkaUtil.getTopics()
        println(topics.mkString(","))


        var offset = kafkaUtil.getEarliestOffset("pre-boikgpokn78sb95kjhfrendo8dc5mlsr")
        println(offset)

        offset = kafkaUtil.getEarliestOffset("11")
        println(offset)
    }

    @Test
    def testProducer1: Unit = {
        val producer = getKafkaProducer[Array[Byte], Array[Byte]]
        val chars = "012345678ABCDEFabcdef"
        val topic = "test"

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
    def testProducer3: Unit = {
        val producer = getKafkaProducer[Array[Byte], Array[Byte]]

        val topic = "test"
        val source = scala.io.Source.fromFile("/Users/fj/workspace/whaley/projects/WhaleyLogSys/forest/actionlog/src/test/resources/data/boikgpokn78sb95kjhfrendoj8ilnoi7.log-2017050718-bigdata-extsvr-log1")
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
    def testKafkaUtil_getOffset: Unit = {
        val topic = "test"
        val util = getKafkaUtil()
        val earliestOffset = util.getEarliestOffset(topic)
        val latestOffset = util.getLatestOffset(topic)
        LOG.info("earliestOffset: {} , {}", topic, earliestOffset, "")
        LOG.info("latestOffset1: {} , {}", topic, latestOffset, "")

        val count = 10000
        val ts1 = timeConsumeTest(count, () => util.getLatestOffset(topic))
        LOG.info("latestOffset:count:{},ts:{}", count, ts1)

        val ts2 = timeConsumeTest(count, () => util.getEarliestOffset(topic))
        LOG.info("earliestOffset:count:{},ts:{}", count, ts2)
    }

    @Test
    def testKafkaUtil_getOffset2: Unit = {
        val topic = "medusa-processed-log"
        val props = new Properties()
        props.put("bootstrap.servers", servers);
        props.put("group.id", clientId);
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
        val topicAndPartitions = consumer.partitionsFor(topic).map(item => new TopicPartition(topic, item.partition()))
        //.sortBy(item=>item.partition()).toList
        //val topicAndPartitions = new TopicPartition(topic, 1) :: Nil
        topicAndPartitions.foreach(item => {
            val partitionItem = new TopicPartition(topic, item.partition() + 1)
            print(s"getoffset:${item.partition()}\t")
            val offset = consumer.endOffsets(item :: Nil)
            offset.map(item => println(s"${item._1.partition()}\t${item._2.toLong}"))
        })
    }

    @Test
    def testKafkaUtil_setFetchOffset: Unit = {
        val topic = "test"
        val util = getKafkaUtil()
        val consumer = util.createConsumer(groupId)
        val partitions = util.getPartitionInfo(topic)
        val offsets = partitions.map(item => (item.partition(), 200000L)).toMap
        util.setFetchOffset(topic, groupId, offsets, consumer)

        val offsets2 = util.getFetchOffset(topic, groupId, consumer)
        LOG.info("{} \t {}", offsets, offsets2, "")

        require(offsets == offsets2)
    }


    @Test
    def TestKafkaUtil_getLatestMessage: Unit = {
        val topic = "test"
        val util = getKafkaUtil()
        val latestOffset = util.getLatestOffset(topic)
        println(latestOffset)

        val datas = util.getLatestMessage(topic, 100)

        datas.foreach(item => {
            val partiton = item._1
            var count = 0
            item._2.foreach(record => {
                count = count + 1
                val message = record.key()
                val keyBytes = record.key()
                val valueBytes = record.value()
                val key = new String(keyBytes)
                val value = new String(valueBytes)
                println(s"[${partiton}:${count}:${record.offset}]:${key}:${value}")
            })
        })

        System.in.read()
    }

}
