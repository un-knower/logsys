import cn.whaley.bi.logsys.common.AppIdUtil
import org.junit.Test

/**
 * Created by fj on 16/11/2.
 */
class AppIdUtilTest {

    @Test
    def testCreateAppId1: Unit = {
        val util = new AppIdUtil
        println("medusa:"+util.createAppId("whaley", "medusa", "main"))
        println("whaleytv:"+util.createAppId("whaley", "whaleytv", "main"))
        println("whaleyorca:"+util.createAppId("whaley", "whaleyorca", "main"))
        println("whaleyvr:"+util.createAppId("whaley", "whaleyvr", "main"))
    }

}
