import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import cn.whaley.bi.logsys.forest.Traits.LogTrait
import cn.whaley.bi.logsys.forest.{ProcessResultCode, GenericProcessorChain}
import com.alibaba.fastjson.JSONObject
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/11/17.
 */
class GenericProcessorChainTest extends LogTrait {

    @Test
    def testChainProcessor1: Unit = {
        val confManager = new ConfManager(Array("settings.properties", "GenericProcessorChain.xml"))

        //val dataPath="data/data1.txt"
        //confManager.putConf("GenericMsgDecoder.filebeatDecode","false");

        val dataPath = "data/data2.txt"
        confManager.putConf("GenericMsgDecoder.filebeatDecode", "true");

        val chain = new GenericProcessorChain()

        chain.init(confManager)



        val stream = this.getClass.getClassLoader.getResourceAsStream("data/data1.txt")
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

                    ret.result.get.foreach(item => println(item.toJSONString))
                    println("")

                    ret.result.get

                })

            println(s"${i}:ts:${System.currentTimeMillis() - from2},${result.length}")

        }

        println(s"${lines.length}:ts:${System.currentTimeMillis() - from}")

    }

    @Test
    def testChainProcessor2(): Unit = {
        val fileBeatMsg = "{\"@timestamp\":\"2017-07-11T22:20:12.803Z\",\"message\":\"{\\\"msgId\\\":\\\"AAABXTO9CGcKE1nEZStifAAB\\\",\\\"msgVersion\\\":\\\"1.0\\\",\\\"msgSite\\\":\\\"10.19.89.196\\\",\\\"msgSource\\\":\\\"ngx_log\\\",\\\"msgFormat\\\":\\\"json\\\",\\\"msgSignFlag\\\":0,\\\"msgBody\\\":{\\\"svr_host\\\":\\\"log.tvmore.com.cn\\\",\\\"svr_req_method\\\":\\\"POST\\\",\\\"svr_req_url\\\":\\\"/medusalog/\\\",\\\"svr_content_type\\\":\\\"application/json\\\",\\\"svr_remote_addr\\\":\\\"10.10.251.83\\\",\\\"svr_forwarded_for\\\":\\\"118.134.21.194, 117.144.227.50\\\",\\\"svr_receive_time\\\":1499811612775,\\\"appId\\\":\\\"boikgpokn78sb95ktmsc1bnkechpgj9l\\\",\\\"body\\\":{\\\\x22md5\\\\x22:\\\\x229e709f0132cf31656bdfc2a8942c2d7c\\\\x22,\\\\x22baseInfo\\\\x22:\\\\x22{\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712062012\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22}\\\\x22,\\\\x22ts\\\\x22:1499811612673,\\\\x22logs\\\\x22:\\\\x22[{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x22夏日狂欢\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x22tvn8b2oq3eu9\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712061815\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22},{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x22摇摆少女\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x229wqtp83f5iac\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712061827\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22},{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x226号别墅\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x22g6ab23bdqswy\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712061853\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22},{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x22饭桌女郎\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x22g6ab23237plm\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712061907\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22},{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x22恶女\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x22tvn8b2n8jkbd\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712061913\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22},{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x22活宝警探\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x22s9n8vwqsb2bc\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712061926\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22},{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x22贴身卧底\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x22g6abvxa23fqs\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712061944\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22},{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x22十八盗(云中鹤)\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x22g6x0gi9wvwsu\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712061953\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22},{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x22逆光之爱\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x22g6abvx1cjm3e\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712062000\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22},{\\\\x5C\\\\x22logVersion\\\\x5C\\\\x22:\\\\x5C\\\\x2201\\\\x5C\\\\x22,\\\\x5C\\\\x22accountId\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22appEnterWay\\\\x5C\\\\x22:\\\\x5C\\\\x22native\\\\x5C\\\\x22,\\\\x5C\\\\x22promotionChannel\\\\x5C\\\\x22:\\\\x5C\\\\x22guanwang\\\\x5C\\\\x22,\\\\x5C\\\\x22apkSeries\\\\x5C\\\\x22:\\\\x5C\\\\x22MoreTV_TVApp3.0_Medusa\\\\x5C\\\\x22,\\\\x5C\\\\x22productModel\\\\x5C\\\\x22:\\\\x5C\\\\x22MiBOX2\\\\x5C\\\\x22,\\\\x5C\\\\x22versionCode\\\\x5C\\\\x22:\\\\x5C\\\\x22313\\\\x5C\\\\x22,\\\\x5C\\\\x22retrieval\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22contentType\\\\x5C\\\\x22:\\\\x5C\\\\x22movie\\\\x5C\\\\x22,\\\\x5C\\\\x22videoName\\\\x5C\\\\x22:\\\\x5C\\\\x22山茶花\\\\x5C\\\\x22,\\\\x5C\\\\x22apkVersion\\\\x5C\\\\x22:\\\\x5C\\\\x223.1.3\\\\x5C\\\\x22,\\\\x5C\\\\x22pathMain\\\\x5C\\\\x22:\\\\x5C\\\\x22home*classification*movie-movie*七日更新\\\\x5C\\\\x22,\\\\x5C\\\\x22groupId\\\\x5C\\\\x22:\\\\x5C\\\\x22475\\\\x5C\\\\x22,\\\\x5C\\\\x22event\\\\x5C\\\\x22:\\\\x5C\\\\x22view\\\\x5C\\\\x22,\\\\x5C\\\\x22userId\\\\x5C\\\\x22:\\\\x5C\\\\x2201322f6c07c799fb5d1d44efc7cab794\\\\x5C\\\\x22,\\\\x5C\\\\x22videoSid\\\\x5C\\\\x22:\\\\x5C\\\\x22s9n8vw6kt99v\\\\x5C\\\\x22,\\\\x5C\\\\x22searchText\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22buildDate\\\\x5C\\\\x22:\\\\x5C\\\\x2220170118\\\\x5C\\\\x22,\\\\x5C\\\\x22uploadTime\\\\x5C\\\\x22:\\\\x5C\\\\x2220170712062011\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSub\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22pathSpecial\\\\x5C\\\\x22:\\\\x5C\\\\x22\\\\x5C\\\\x22,\\\\x5C\\\\x22weatherCode\\\\x5C\\\\x22:\\\\x5C\\\\x22101120501\\\\x5C\\\\x22,\\\\x5C\\\\x22logType\\\\x5C\\\\x22:\\\\x5C\\\\x22detail\\\\x5C\\\\x22}]\\\\x22,\\\\x22version\\\\x22:\\\\x2201\\\\x22}}}\",\"type\":\"log\"}"
        val chain = new GenericProcessorChain()
        val confManager = new ConfManager(Array("settings.properties", "GenericProcessorChain.xml"))
        confManager.putConf("GenericMsgDecoder.filebeatDecode", "true");
        chain.init(confManager)

        val bytes = fileBeatMsg.getBytes
        val ret = chain.process(bytes)
        if (ret.hasErr) {
            if (ret.ex.isDefined) {
                ret.ex.get.printStackTrace()
            }
        }
        if (ret.hasErr) {
            println(ret.ex.get.getMessage)
        }
        require(ret.hasErr == false)

        //println(ret.result.get.length)

        ret.result.get.foreach(item => println(item.toJSONString))


    }

    @Test
    def testChainProcessor3(): Unit = {

        val confManager = new ConfManager(Array("settings.properties", "kafkaMsgSource.xml", "kafka-consumer.xml", "GenericProcessorChain.xml"))
        confManager.putConf("GenericMsgDecoder.filebeatDecode", "true");

        val servers = confManager.getConf("kafka-consumer.bootstrap.servers")
        LOG.info(s"servers:${servers}")
        val util = KafkaUtil(servers, "testx")

        val offset = 1145369352L
        val topic = "log-raw-boikgpokn78sb95ktmsc1bnk"
        val partition = 9

        val chain = new GenericProcessorChain()
        chain.init(confManager)
        val msg = util.getFirstMsg(topic, partition, offset)
        if (!msg.isDefined) {
            LOG.info("no message")
            return
        }


        val bytes = msg.get.value()
        val ret = chain.process(bytes)
        if (ret.hasErr) {
            if (ret.ex.isDefined) {
                ret.ex.get.printStackTrace()
            }
        }
        if (ret.hasErr) {
            println(ret.ex.get.getMessage)
        }
        require(ret.hasErr == false)

        ret.result.get.foreach(item => println(item.toJSONString))


    }

}
