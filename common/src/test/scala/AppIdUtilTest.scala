import cn.whaley.bi.logsys.common.{AppIdCreator, AppIdUtil}
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
        println("eagle:" + util.createAppId("whaley", "eagle", "main"))
        println("epop:" + util.createAppId("whaley", "whaleytv", "epop"))
    }

    @Test
    def testCreateAppId2: Unit = {
        val util = new AppIdUtil

        val appInfo = new ArrayBuffer[(String, String, String)]()
        appInfo.append(("whaley", "medusa", "main3.1"))
        appInfo.append(("whaley", "medusa", "main2.0"))
        appInfo.append(("whaley", "medusa", "main1.0"))
        appInfo.append(("whaley", "medusa", "kids"))
        appInfo.append(("whaley", "mobilehelper", "main"))
        appInfo.append(("whaley", "whaleytv", "main"))
        appInfo.append(("whaley", "whaleyvr", "main"))
        appInfo.append(("whaley", "crawler", "price"))
        appInfo.append(("whaley", "whaleytv", "epop"))
        appInfo.append(("whaley", "whaleytv", "global_menu_2.0"))
        appInfo.append(("whaley", "medusa", "main3.0")) //留给WUI2.0
        appInfo.append(("whaley", "whaleytv", "wui2.0"))
        appInfo.append(("whaley", "orca", "global_menu_2.0"))


        appInfo.foreach(item => {
            val id = util.createAppId(item._1, item._2, item._3)
            println(s"${id}\t${item._1}\t${item._2}\t${item._3}")
        })

    }

    @Test
    def testCreateAppId3: Unit = {
        //AppIdCreator.main("whaley,orca,global_menu_2.0,ds".split(","))
        AppIdCreator.main("whaley,orca,global_menu_2.0".split(","))
    }

}
