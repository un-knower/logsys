
import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import cn.whaley.bi.logsys.forest.MsgBatchManager
import cn.whaley.bi.logsys.forest.Traits.LogTrait
import org.junit.Test


/**
 * Created by fj on 16/11/18.
 */
class MsgBatchManagerTest extends LogTrait {
    //val topic = "pre-boikgpokn78sb95kjhfrendo8dc5mlsr"
    val topic = "pre-boikgpokn78sb95kjhfrendo"
    val groupId = "forest"

    def getUtil(): KafkaUtil = {
        val confManager = new ConfManager(Array("kafka-producer.xml", "kafka.properties"))
        val servers = confManager.getConf("kafka-producer", "bootstrap.servers")
        val util = KafkaUtil(servers)
        util
    }

    @Test
    def testBatch: Unit = {

        //resetOffset

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
        val util = getUtil()
        var offset = util.getEarliestOffset(topic).map(item => (item._1, 0L))
        util.setFetchOffset(topic, "forest", offset)

        offset = util.getFetchOffset(topic, "forest")
        LOG.info("curr:{}", offset)
    }

    @Test
    def getOffsetInfo: Unit = {
        // commitOffset:Map(pre-boikgpokn78sb95kjhfrendo8dc5mlsr -> Map(2 -> 653, 1 -> 654, 0 -> 691))
        val util = getUtil()

        val latestOffset = util.getLatestOffset(topic)
        LOG.info("latestOffset:{}", latestOffset)

        val earliestOffset = util.getEarliestOffset(topic)
        LOG.info("earliestOffset:{}", earliestOffset)


    }

    @Test
    def getFetchOffsetInfo: Unit = {
        // commitOffset:Map(pre-boikgpokn78sb95kjhfrendo8dc5mlsr -> Map(2 -> 653, 1 -> 654, 0 -> 691))
        val util = getUtil()

        val offset = util.getFetchOffset(topic, groupId)
        LOG.info("{}", offset)

    }

}
