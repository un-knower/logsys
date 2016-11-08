import cn.whaley.bi.logsys.common.AppIdUtil
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/11/2.
 */
class AppIdUtilTest {

    @Test
    def testCreateAppId1: Unit = {
        val util = new AppIdUtil
        println("medusa:" + util.createAppId("whaley", "medusa", "main"))
        println("whaleytv:" + util.createAppId("whaley", "whaleytv", "main"))
        println("whaleyorca:" + util.createAppId("whaley", "whaleyorca", "main"))
        println("whaleyvr:" + util.createAppId("whaley", "whaleyvr", "main"))
    }

    @Test
    def testCreateAppId2: Unit = {
        val util = new AppIdUtil

        val appInfo = new ArrayBuffer[(String, String, String)]()
        appInfo.append(("whaley", "medusa", "main3.0"))
        appInfo.append(("whaley", "medusa", "main2.0"))
        appInfo.append(("whaley", "medusa", "main1.0"))
        appInfo.append(("whaley", "medusa", "kids"))
        appInfo.append(("whaley", "mobilehelper", "main"))
        appInfo.append(("whaley", "whaleytv", "main"))
        appInfo.append(("whaley", "whaleyvr", "main"))
        appInfo.append(("whaley", "crawler", "price"))
        appInfo.append(("whaley", "crawler", "cis"))

        appInfo.foreach(item => {
            val id = util.createAppId(item._1, item._2, item._3)
            println(s"${id}\t${item._1}\t${item._2}\t${item._3}")
        })

    }

}
