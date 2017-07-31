import cn.whaley.bi.logsys.common.{ConfManager, KafkaUtil}
import cn.whaley.bi.logsys.forest.Traits.LogTrait
import org.junit.Test

/**
 * Created by fj on 16/11/17.
 */
class KafkaMsgSourceTest extends LogTrait {

    @Test
    def test: Unit = {

    }

    @Test
    def resetOffset: Unit = {

        val confManager = new ConfManager(Array("kafkaMsgSource.xml", "kafka-consumer.xml"))
        val topic = "pre-boikgpokn78sb95kjhfrendo8dc5mlsr"
        val groupId = confManager.getConf("kafka-consumer", "group.id")
        val util = new KafkaUtil(Array(("localhost", 9092)))

        var offsets = util.getFetchOffset(topic, groupId)
        val newOffsets =
            offsets.map(item => {
                println(s"nowOffset:${item._1}->${item._2}")
                (item._1, 0L)
            })
        util.setFetchOffset("pre-boikgpokn78sb95kjhfrendo8dc5mlsr", groupId, newOffsets)
        Thread.sleep(1000)
        offsets = util.getFetchOffset(topic, groupId)
        offsets.map(item => {
            println(s"newOffset:${item._1}->${item._2}")
            (item._1, 0L)
        })
    }

    @Test
    def readKafkaMsg: Unit = {
        val confManager = new ConfManager(Array("settings.properties", "kafkaMsgSource.xml", "kafka-consumer.xml"))
        val servers = confManager.getConf("kafka-consumer.bootstrap.servers")
        LOG.info(s"servers:${servers}")
        val util = KafkaUtil(servers, "testx")

        val offset = 1145369352L
        val topic = "log-raw-boikgpokn78sb95ktmsc1bnk"
        val partition = 9

        /*
        val topics = util.getTopics()
        topics.foreach(topic => {
            val offsets = util.getLatestOffset(topic)
            LOG.info(s"${topic}: ${offsets}")
        })
        */

        val msg = util.getFirstMsg(topic, partition, offset)
        if (msg.isDefined) {
            val key = if (msg.get.key() != null) new String(msg.get.key()) else ""
            val value = new String(msg.get.value())
            LOG.info(s"key:${key},value:$value,offset:${msg.get.offset()}")
        } else {
            LOG.info("no message")
        }
    }


}
