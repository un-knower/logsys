import cn.whaley.bi.logsys.forest.Traits.LogTrait
import cn.whaley.bi.logsys.forest.{MainObj, MsgProcExecutor}
import org.junit.Test

/**
 * Created by fj on 16/11/20.
 */
class MainObjTest extends LogTrait{
    @Test
    def testMsgProce: Unit = {
        val args = Array("MsgProcExecutor")
        MainObj.main(args)

        Thread.sleep(10000)
    }


    @Test
    def testMsgProcExecutor: Unit = {
        val args = Array("")
        val executor = new MsgProcExecutor()
        executor.execute(args)
        executor.shutdown(true)
        LOG.info("test completed.")
    }

}
