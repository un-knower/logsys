import org.junit.Test

/**
 * Created by fj on 17/5/9.
 */
class MsgProcExecutorTest {

    @Test
    def test1(): Unit = {
        val args = Array("MsgProcExecutor"
            , "--f", "MsgBatchManager.xml,settings.properties"
            , "--c", "prop.MsgBatchManager.processorChain=GenericProcessorChain"
            , "--c", "prop.HdfsMsgSink.commitTimeMillSec=20000"
            , "--c", "prop.KafkaMsgSource.topics=^log-raw-boikgpokn78sb95kjhfrendoj8ilnoi7$"
        )
        cn.whaley.bi.logsys.forest.MainObj.main(args)
        System.in.read()
    }

}
