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


}
