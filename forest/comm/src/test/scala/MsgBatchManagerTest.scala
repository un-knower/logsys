import java.nio.charset.Charset

import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import cn.whaley.bi.logsys.forest.MsgBatchManager
import cn.whaley.bi.logsys.forest.Traits.LogTrait
import org.junit.Test
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by fj on 16/11/18.
 */
class MsgBatchManagerTest extends LogTrait {
    //val topic = "pre-boikgpokn78sb95kjhfrendo8dc5mlsr"
    val topic = "pre-boikgpokn78sb95kjhfrendo"
    val groupId = "forest"

    @Test
    def testBatch: Unit = {

        resetOffset

        val confManager = new ConfManager(Array("MsgBatchManager.xml"))
        val batchManager = new MsgBatchManager()
        batchManager.init(confManager)
        batchManager.start()


        val topics = batchManager.consumingTopics
        if (topics.size > 0) {
            LOG.info(s"keep running,consumedTopic:${batchManager.consumingTopics}")
            Thread.sleep(1000 * 3)
        }


        //batchManager.shutdown(true)

        Thread.sleep(1000 * 3000)

        LOG.info("test completed.")

    }



    @Test
    def resetOffset: Unit = {
        val confManager = new ConfManager(Array("kafka-consumer.xml", "kafka.properties"))
        val zks = confManager.getConf("kafka-consumer", "zookeeper.connect")
        val util = KafkaUtil(zks)
        var offset = util.getEarliestOffset(topic).map(item => (item._1, 0L))
        util.setFetchOffset(topic, "forest", offset)

        offset = util.getFetchOffset(topic, "forest")
        LOG.info("curr:{}", offset)
    }

    @Test
    def getOffsetInfo: Unit = {
        // commitOffset:Map(pre-boikgpokn78sb95kjhfrendo8dc5mlsr -> Map(2 -> 653, 1 -> 654, 0 -> 691))
        val confManager = new ConfManager(Array("kafka-consumer.xml", "kafka.properties"))
        val zks = confManager.getConf("kafka-consumer", "zookeeper.connect")
        val util = KafkaUtil(zks)


        val latestOffset = util.getLatestOffset(topic)
        LOG.info("latestOffset:{}", latestOffset)

        val earliestOffset = util.getEarliestOffset(topic)
        LOG.info("earliestOffset:{}", earliestOffset)


    }

    @Test
    def getFetchOffsetInfo: Unit = {
        // commitOffset:Map(pre-boikgpokn78sb95kjhfrendo8dc5mlsr -> Map(2 -> 653, 1 -> 654, 0 -> 691))
        val confManager = new ConfManager(Array("kafka-consumer.xml", "kafka.properties"))
        val zks = confManager.getConf("kafka-consumer", "zookeeper.connect")
        val util = KafkaUtil(zks)


        val offset = util.getFetchOffset(topic, groupId)
        LOG.info("{}", offset)

    }

    @Test
    def testConsumerFromLatest: Unit = {
        val groupId = "test"
        val confManager = new ConfManager(Array("kafka-consumer.xml", "kafka.properties"))
        val zks = confManager.getConf("kafka-consumer", "zookeeper.connect")
        val util = KafkaUtil(zks)

        val offset = util.getLatestOffset(topic)
        LOG.info("offset:{}", offset)

        util.setFetchOffset(topic, groupId, offset.map(item => (item._1, item._2)))

        val consumerConf = confManager.getAllConf("kafka-consumer", true)
        consumerConf.setProperty("group.id", groupId)


        val consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(consumerConf))

        val map = new java.util.HashMap[String, Integer]()
        map.put(topic, 3)
        val streams = consumerConnector.createMessageStreams(map).get(topic)
        var count = 0
        streams.foreach(stream => {
            val it = stream.iterator()
            while (it.hasNext()) {
                val curr = it.next()
                val key = if (curr.key != null) new String(curr.key()) else ""
                val value = new String(curr.message())
                //println(s"(${curr.partition},${curr.offset}):${key}:${value}")
                count = count + 1
                println(s"(${curr.partition},${curr.offset}):${count}")
            }
        })
    }

    @Test
    def testGetLastOffset: Unit = {
        val confManager = new ConfManager(Array("kafka-consumer.xml", "kafka.properties"))
        val zks = confManager.getConf("kafka-consumer", "zookeeper.connect")
        val kafkaUtil = KafkaUtil(zks)

        val sourceTopic = "pre-boikgpokn78sb95kjhfrendo"
        val maxMsgCount = 3000
        val targetTopic = "post-boikgpokn78sb95kjhfrendo8dc5mlsr"

        val offsetRegex = "/(\\d+)/(\\d+)".r
        def getOffsetInfoFromKey(key: String, sourceTopic: String): Option[(Int, Long)] = {
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

        val latestOffset = kafkaUtil.getLatestOffset(sourceTopic)
        val offsetMap = new mutable.HashMap[Int, Long]
        val msgs = kafkaUtil.getLatestMessage(targetTopic, maxMsgCount)
        val charset = Charset.forName("UTF-8")
        val decoder = charset.newDecoder()
        msgs.foreach(msg => {
            msg._2.map(item => {
                val strKey = decoder.decode(item.message.key.asReadOnlyBuffer()).toString
                val offsetInfo = getOffsetInfoFromKey(strKey, sourceTopic)
                if (offsetInfo.isDefined) {
                    val partition = offsetInfo.get._1
                    val fromOffset = offsetInfo.get._2 + 1
                    if (fromOffset > offsetMap.getOrElse(partition, 0L)) {
                        val latestOffsetValue = latestOffset(partition)
                        if (fromOffset > latestOffsetValue) {
                            offsetMap.update(partition, latestOffsetValue)
                        } else {
                            offsetMap.update(partition, fromOffset)
                        }
                    }
                }
                println(strKey)
            })
        })


        val offset = kafkaUtil.getLatestOffset(topic)
        LOG.info("offset:{}", offset)
        println(s"offsetMap:${offsetMap}")
    }

}
