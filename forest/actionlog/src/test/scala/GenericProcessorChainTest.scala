import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.{ProcessResultCode, GenericProcessorChain}
import com.alibaba.fastjson.JSONObject
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/11/17.
 */
class GenericProcessorChainTest {

    @Test
    def testProcessor: Unit = {
        val chain = new GenericProcessorChain()
        val confManager = new ConfManager(Array("GenericProcessorChain.xml"))
        chain.init(confManager)


        val stream = this.getClass.getClassLoader.getResourceAsStream("boikgpokn78sb95kjhfrendoj8ilnoi7.log")
        val source = scala.io.Source.fromInputStream(stream)
        val filelines = source.getLines().toArray
        val lines = new ArrayBuffer[String]()
        for (i <- 0 to 0) {
            lines.append(filelines: _*)
        }

        val from = System.currentTimeMillis()

        for (i <- 0 to 0) {

            val from2 = System.currentTimeMillis()

            val result =
                lines.flatMap(item => {
                    val bytes = item.getBytes
                    val ret = chain.process(bytes)
                    if (ret.hasErr) {
                        if (ret.ex.isDefined) {
                            ret.ex.get.printStackTrace()
                        }
                    }
                    require(ret.hasErr == false)

                    //println(ret.result.get.length)

                    ret.result.get.foreach(item=>println(item.toJSONString))
                    println("")

                    ret.result.get

                })

            println(s"${i}:ts:${System.currentTimeMillis() - from2},${result.length}")

        }

        println(s"${lines.length}:ts:${System.currentTimeMillis() - from}")

    }

    @Test
    def testMsg3():Unit={
        val msgText="{\"@timestamp\":\"2017-05-21T11:56:27.602Z\",\"message\":\"{\\\"msgId\\\":\\\"AAABXCrdocQKE2uqUIwqAQAB\\\",\\\"msgVersion\\\":\\\"1.0\\\",\\\"msgSite\\\":\\\"10.19.107.170\\\",\\\"msgSource\\\":\\\"ngx_log\\\",\\\"msgFormat\\\":\\\"json\\\",\\\"msgSignFlag\\\":0,\\\"msgBody\\\":{\\\"svr_host\\\":\\\"wlslog.aginomoto.com\\\",\\\"svr_req_method\\\":\\\"POST\\\",\\\"svr_req_url\\\":\\\"/log/boikgpokn78sb95k7id7n8eb8dc5mlsr\\\",\\\"svr_content_type\\\":\\\"application/json\\\",\\\"svr_remote_addr\\\":\\\"10.10.251.81\\\",\\\"svr_forwarded_for\\\":\\\"223.73.52.211\\\",\\\"svr_receive_time\\\":1495367786948,\\\"appId\\\":\\\"boikgpokn78sb95k7id7n8eb8dc5mlsr\\\",\\\"body\\\":{\\\\x22md5\\\\x22:\\\\x22fccb0fd63c9251282fd843199bce0214\\\\x22,\\\\x22baseInfo\\\\x22:\\\\x22{\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x222.1.9\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22Channel_normal\\\\x5C\\\\x22,\\\\x5C\\\\x22areaCode\\\\x5C\\\\x22:\\\\x5C\\\\x22280102\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x22abf8bb72f89d840439a19e558c38da7e\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadtime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170521195626\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22BAOFENG_TVMST_6A338\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22eagle_live\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22220\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101280102\\\\x5C\\\\x22}\\\\x22,\\\\x22ts\\\\x22:1495367786909,\\\\x22logs\\\\x22:\\\\x22[{\\\\x5C\\\\x22currentPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22launcher\\\\x5C\\\\x22,\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22currentPageProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22tabInfo\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22央视卫视\\\\x5C\\\\x5C\\\\x5C\\\\x22}\\\\x5C\\\\x22,\\\\x5C\\\\x22eventId\\\\x5C\\\\x22:\\\\x5C\\\\x22launcher_click\\\\x5C\\\\x22,\\\\x5C\\\\x22logId\\\\x5C\\\\x22:11,\\\\x5C\\\\x22eventProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22contentName\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22东方卫视\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22status\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22ok\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22subTabInfo\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22contentId\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22dongfang\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22locationIndex\\\\x5C\\\\x5C\\\\x5C\\\\x22:12}\\\\x5C\\\\x22,\\\\x5C\\\\x22nextPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22live\\\\x5C\\\\x22,\\\\x5C\\\\x22happenTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170521195609\\\\x5C\\\\x22,\\\\x5C\\\\x22dynamicBasicData\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22com.moretv.android\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22sessionId\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x222c3d2bd4a66496c035e813823c4b7efd\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22isTest\\\\x5C\\\\x5C\\\\x5C\\\\x22:0}\\\\x5C\\\\x22},{\\\\x5C\\\\x22currentPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22live\\\\x5C\\\\x22,\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22currentPageProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22typeName\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22热播频道\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22liveChannelSid\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22cctv4\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22liveChanelName\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22CCTV-4 国际\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22contentType\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22直播\\\\x5C\\\\x5C\\\\x5C\\\\x22}\\\\x5C\\\\x22,\\\\x5C\\\\x22eventId\\\\x5C\\\\x22:\\\\x5C\\\\x22play\\\\x5C\\\\x22,\\\\x5C\\\\x22logId\\\\x5C\\\\x22:12,\\\\x5C\\\\x22eventProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22type\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22startplay\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22duration\\\\x5C\\\\x5C\\\\x5C\\\\x22:0}\\\\x5C\\\\x22,\\\\x5C\\\\x22nextPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22live\\\\x5C\\\\x22,\\\\x5C\\\\x22happenTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170521195611\\\\x5C\\\\x22,\\\\x5C\\\\x22dynamicBasicData\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22com.moretv.android\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22sessionId\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x222c3d2bd4a66496c035e813823c4b7efd\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22isTest\\\\x5C\\\\x5C\\\\x5C\\\\x22:0}\\\\x5C\\\\x22},{\\\\x5C\\\\x22currentPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22live\\\\x5C\\\\x22,\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22currentPageProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22typeName\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22热播频道\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22liveChannelSid\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22cctv4\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22liveChanelName\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22CCTV-4 国际\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22contentType\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22直播\\\\x5C\\\\x5C\\\\x5C\\\\x22}\\\\x5C\\\\x22,\\\\x5C\\\\x22eventId\\\\x5C\\\\x22:\\\\x5C\\\\x22play\\\\x5C\\\\x22,\\\\x5C\\\\x22logId\\\\x5C\\\\x22:13,\\\\x5C\\\\x22eventProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22type\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22endplay\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22duration\\\\x5C\\\\x5C\\\\x5C\\\\x22:2}\\\\x5C\\\\x22,\\\\x5C\\\\x22nextPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22live\\\\x5C\\\\x22,\\\\x5C\\\\x22happenTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170521195613\\\\x5C\\\\x22,\\\\x5C\\\\x22dynamicBasicData\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22com.moretv.android\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22sessionId\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x222c3d2bd4a66496c035e813823c4b7efd\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22isTest\\\\x5C\\\\x5C\\\\x5C\\\\x22:0}\\\\x5C\\\\x22},{\\\\x5C\\\\x22currentPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22launcher\\\\x5C\\\\x22,\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22currentPageProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22tabInfo\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22央视卫视\\\\x5C\\\\x5C\\\\x5C\\\\x22}\\\\x5C\\\\x22,\\\\x5C\\\\x22eventId\\\\x5C\\\\x22:\\\\x5C\\\\x22launcher_click\\\\x5C\\\\x22,\\\\x5C\\\\x22logId\\\\x5C\\\\x22:14,\\\\x5C\\\\x22eventProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22contentName\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22浙江卫视\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22status\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22ok\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22subTabInfo\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22contentId\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22zhejiang\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22locationIndex\\\\x5C\\\\x5C\\\\x5C\\\\x22:8}\\\\x5C\\\\x22,\\\\x5C\\\\x22nextPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22live\\\\x5C\\\\x22,\\\\x5C\\\\x22happenTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170521195617\\\\x5C\\\\x22,\\\\x5C\\\\x22dynamicBasicData\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22com.moretv.android\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22sessionId\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x222c3d2bd4a66496c035e813823c4b7efd\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22isTest\\\\x5C\\\\x5C\\\\x5C\\\\x22:0}\\\\x5C\\\\x22},{\\\\x5C\\\\x22currentPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22live\\\\x5C\\\\x22,\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22currentPageProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22typeName\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22省台卫视\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22liveChannelSid\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22zhejiang\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22liveChanelName\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22浙江卫视\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22contentType\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22直播\\\\x5C\\\\x5C\\\\x5C\\\\x22}\\\\x5C\\\\x22,\\\\x5C\\\\x22eventId\\\\x5C\\\\x22:\\\\x5C\\\\x22play\\\\x5C\\\\x22,\\\\x5C\\\\x22logId\\\\x5C\\\\x22:15,\\\\x5C\\\\x22eventProp\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22type\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22endplay\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22duration\\\\x5C\\\\x5C\\\\x5C\\\\x22:1495367784}\\\\x5C\\\\x22,\\\\x5C\\\\x22nextPageId\\\\x5C\\\\x22:\\\\x5C\\\\x22live\\\\x5C\\\\x22,\\\\x5C\\\\x22happenTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170521195623\\\\x5C\\\\x22,\\\\x5C\\\\x22dynamicBasicData\\\\x5C\\\\x22:\\\\x5C\\\\x22{\\\\x5C\\\\x5C\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x22com.moretv.android\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22sessionId\\\\x5C\\\\x5C\\\\x5C\\\\x22:\\\\x5C\\\\x5C\\\\x5C\\\\x222c3d2bd4a66496c035e813823c4b7efd\\\\x5C\\\\x5C\\\\x5C\\\\x22,\\\\x5C\\\\x5C\\\\x5C\\\\x22isTest\\\\x5C\\\\x5C\\\\x5C\\\\x22:0}\\\\x5C\\\\x22}]\\\\x22,\\\\x22version\\\\x22:\\\\x2202\\\\x22}}}\",\"type\":\"log\"}"
        val chain = new GenericProcessorChain()
        val confManager = new ConfManager(Array("GenericProcessorChain.xml"))
        chain.init(confManager)

        val bytes = msgText.getBytes
        val ret = chain.process(bytes)
        if (ret.hasErr) {
            if (ret.ex.isDefined) {
                ret.ex.get.printStackTrace()
            }
        }
        if(ret.hasErr){
           println(ret.ex.get.getMessage)
        }
        require(ret.hasErr == false)

        //println(ret.result.get.length)

        ret.result.get.foreach(item=>println(item.toJSONString))



    }

}
