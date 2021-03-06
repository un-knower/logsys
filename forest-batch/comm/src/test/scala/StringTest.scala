import cn.whaley.bi.logsys.forest.StringUtil
import com.alibaba.fastjson.JSON
import org.junit.Test

/**
 * Created by fj on 17/3/28.
 */
class StringTest {



    @Test
    def test1(): Unit = {
        //cat /data/backups/nginx_log/20170327/*crashlog.*.log*|grep -i "Caused by: java.lang.NoSuchFieldError: PN"|head
        val str = "{\"msg_value\":\"(AAABXIYxc44KE7pKAVcOpwAB,{\\\"msgSignFlag\\\":0,\\\"msgId\\\":\\\"AAABXIYxc44KE7pKAVcOpwAB\\\",\\\"msgFormat\\\":\\\"json\\\",\\\"msgBody\\\":{\\\"svr_content_type\\\":\\\"application/json\\\",\\\"svr_forwarded_for\\\":\\\"106.92.116.97\\\",\\\"svr_host\\\":\\\"wlslog.aginomoto.com\\\",\\\"svr_remote_addr\\\":\\\"10.10.251.91\\\",\\\"svr_receive_time\\\":1496900006798,\\\"appId\\\":\\\"boikgpokn78sb95k7id7n8eb8dc5mlsr\\\",\\\"svr_fb_Time\\\":\\\"2017-06-08T05:33:27.459Z\\\",\\\"svr_req_url\\\":\\\"/log/boikgpokn78sb95k7id7n8eb8dc5mlsr\\\",\\\"body\\\":{\\\"baseInfo\\\":\\\"{\\\\\\\"apkVersion\\\\\\\":\\\\\\\"2.2.0\\\\\\\",\\\\\\\"accountId\\\\\\\":\\\\\\\"\\\\\\\",\\\\\\\"promotionChannel\\\\\\\":\\\\\\\"Channel_normal\\\\\\\",\\\\\\\"areaCode\\\\\\\":\\\\\\\"040108\\\\\\\",\\\\\\\"userId\\\\\\\":\\\\\\\"a1ab15380015c0a021d5947339ba6d6c\\\\\\\",\\\\\\\"uploadtime\\\\\\\":\\\\\\\"20170608133320\\\\\\\",\\\\\\\"productModel\\\\\\\":\\\\\\\"长虹智能电视\\\\\\\",\\\\\\\"apkSeries\\\\\\\":\\\\\\\"eagle_live\\\\\\\",\\\\\\\"versionCode\\\\\\\":\\\\\\\"221\\\\\\\",\\\\\\\"weatherCode\\\\\\\":\\\\\\\"101040800\\\\\\\"}\\\",\\\"logs\\\":\\\"[{\\\\\\\"currentPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"logVersion\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"currentPageProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"typeName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"快乐轮播\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChannelSid\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"lb_hkmovie\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChanelName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"香港电影\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"contentType\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"直播\\\\\\\\\\\\\\\"}\\\\\\\",\\\\\\\"eventId\\\\\\\":\\\\\\\"play\\\\\\\",\\\\\\\"logId\\\\\\\":21,\\\\\\\"eventProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"endplay\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"duration\\\\\\\\\\\\\\\":948}\\\\\\\",\\\\\\\"nextPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"happenTime\\\\\\\":\\\\\\\"20170608133007\\\\\\\",\\\\\\\"dynamicBasicData\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"appEnterWay\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"com.moretv.android\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"sessionId\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"4d5766ea9c6fb5d2e07554aee50a436d\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"isTest\\\\\\\\\\\\\\\":0}\\\\\\\"},{\\\\\\\"currentPageId\\\\\\\":\\\\\\\"play\\\\\\\",\\\\\\\"logVersion\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"currentPageProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"liveChannelSid\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"zxcdyhj\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChanelName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"周星驰电影合集\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"contentType\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"直播\\\\\\\\\\\\\\\"}\\\\\\\",\\\\\\\"eventId\\\\\\\":\\\\\\\"menu_click\\\\\\\",\\\\\\\"logId\\\\\\\":22,\\\\\\\"eventProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"status\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"THRID_PARTY_PLAYER\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"menuInfo\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"player\\\\\\\\\\\\\\\"}\\\\\\\",\\\\\\\"nextPageId\\\\\\\":\\\\\\\"play\\\\\\\",\\\\\\\"happenTime\\\\\\\":\\\\\\\"20170608133007\\\\\\\",\\\\\\\"dynamicBasicData\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"appEnterWay\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"com.moretv.android\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"sessionId\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"4d5766ea9c6fb5d2e07554aee50a436d\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"isTest\\\\\\\\\\\\\\\":0}\\\\\\\"},{\\\\\\\"currentPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"logVersion\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"currentPageProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"typeName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"我的频道\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChannelSid\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"zxcdyhj\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChanelName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"周星驰电影合集\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"contentType\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"直播\\\\\\\\\\\\\\\"}\\\\\\\",\\\\\\\"eventId\\\\\\\":\\\\\\\"play\\\\\\\",\\\\\\\"logId\\\\\\\":23,\\\\\\\"eventProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"startplay\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"duration\\\\\\\\\\\\\\\":0}\\\\\\\",\\\\\\\"nextPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"happenTime\\\\\\\":\\\\\\\"20170608133007\\\\\\\",\\\\\\\"dynamicBasicData\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"appEnterWay\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"com.moretv.android\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"sessionId\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"4d5766ea9c6fb5d2e07554aee50a436d\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"isTest\\\\\\\\\\\\\\\":0}\\\\\\\"},{\\\\\\\"currentPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"logVersion\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"currentPageProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"typeName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"我的频道\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChannelSid\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"zxcdyhj\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChanelName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"周星驰电影合集\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"contentType\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"直播\\\\\\\\\\\\\\\"}\\\\\\\",\\\\\\\"eventId\\\\\\\":\\\\\\\"play\\\\\\\",\\\\\\\"logId\\\\\\\":24,\\\\\\\"eventProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"startplay\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"duration\\\\\\\\\\\\\\\":0}\\\\\\\",\\\\\\\"nextPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"happenTime\\\\\\\":\\\\\\\"20170608133008\\\\\\\",\\\\\\\"dynamicBasicData\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"appEnterWay\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"com.moretv.android\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"sessionId\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"4d5766ea9c6fb5d2e07554aee50a436d\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"isTest\\\\\\\\\\\\\\\":0}\\\\\\\"},{\\\\\\\"currentPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"logVersion\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"currentPageProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"typeName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"网络大杂烩\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChannelSid\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"zxcdyhj\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChanelName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"周星驰电影合集\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"contentType\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"直播\\\\\\\\\\\\\\\"}\\\\\\\",\\\\\\\"eventId\\\\\\\":\\\\\\\"play\\\\\\\",\\\\\\\"logId\\\\\\\":25,\\\\\\\"eventProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"endplay\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"duration\\\\\\\\\\\\\\\":4}\\\\\\\",\\\\\\\"nextPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"happenTime\\\\\\\":\\\\\\\"20170608133013\\\\\\\",\\\\\\\"dynamicBasicData\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"appEnterWay\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"com.moretv.android\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"sessionId\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"4d5766ea9c6fb5d2e07554aee50a436d\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"isTest\\\\\\\\\\\\\\\":0}\\\\\\\"},{\\\\\\\"currentPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"logVersion\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"currentPageProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"typeName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"我的频道\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChannelSid\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"lzyjsp\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"liveChanelName\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"林正英僵尸鬼片\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"contentType\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"直播\\\\\\\\\\\\\\\"}\\\\\\\",\\\\\\\"eventId\\\\\\\":\\\\\\\"play\\\\\\\",\\\\\\\"logId\\\\\\\":26,\\\\\\\"eventProp\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"startplay\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"duration\\\\\\\\\\\\\\\":0}\\\\\\\",\\\\\\\"nextPageId\\\\\\\":\\\\\\\"live\\\\\\\",\\\\\\\"happenTime\\\\\\\":\\\\\\\"20170608133014\\\\\\\",\\\\\\\"dynamicBasicData\\\\\\\":\\\\\\\"{\\\\\\\\\\\\\\\"appEnterWay\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"com.moretv.android\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"sessionId\\\\\\\\\\\\\\\":\\\\\\\\\\\\\\\"4d5766ea9c6fb5d2e07554aee50a436d\\\\\\\\\\\\\\\",\\\\\\\\\\\\\\\"isTest\\\\\\\\\\\\\\\":0}\\\\\\\"}]\\\",\\\"version\\\":\\\"02\\\",\\\"md5\\\":\\\"fbcf1f57afd5a9ec36b682b5fe371ccd\\\",\\\"ts\\\":1496900000400},\\\"svr_req_method\\\":\\\"POST\\\"},\\\"msgSource\\\":\\\"ngx_log\\\",\\\"msgVersion\\\":\\\"1.0\\\",\\\"msgSite\\\":\\\"10.19.186.74\\\"})\",\"_sync\":{\"rawTopic\":\"log-raw-boikgpokn78sb95k7id7n8eb\",\"rawTs\":1496900007459,\"odsTs\":1496900013641,\"rawOffset\":7347200,\"rawParId\":0},\"msg_err\":\"{\\\"code\\\":\\\"exception\\\",\\\"ex\\\":\\\"[GenericActionLogGetProcessor->GenericActionLogPostProcessor;signFailure;]\\\",\\\"source\\\":\\\"GenericProcessorChain\\\",\\\"message\\\":\\\"应用层消息处理错误\\\"}\"}"
        val bytes = StringUtil.decodeNgxStrToBytes(str);
        println("-----")
        println(new String(bytes, "UTF-8"))
        println("-----")
        //println(new String(bytes, "UTF-16"))

        val jsonObj=JSON.parseObject(str)
        println(jsonObj.toJSONString);
    }

    @Test
    def test2: Unit = {
        val str = "\\x09\\x09\\x09"
        val decodedStr = StringUtil.decodeNgxStrToString(str);
        println(s"_${decodedStr}_")
    }

    @Test
    def test3: Unit = {
        val str = "\\x09\\x09\\x09"
        val bytes = StringUtil.decodeNgxStrToBytes(str);
        val value = StringTest.byteArrayToInt(bytes);
        println(value)

        println(Integer.parseInt("80", 16))
        println(Integer.toBinaryString(128))

    }

    @Test
    def test4: Unit = {
        val reg = ".*/([a-zA-Z0-9]{32})_(\\d{10})_(\\w+)_(\\d+)_(\\d+).json".r
        val path = "/data_warehouse/ods_origin.db/tmp_log_origin/boikgpokn78sb95kjhfrendoj8ilnoi7_2017050718_raw_0_536.json"
        val matched = reg.findFirstMatchIn(path).get
        println(matched.group(1))
        println(matched.group(2))
        println(matched.group(3))
        println(matched.group(4))
        println(matched.group(5))

    }

    @Test
    def test5: Unit = {
        val topicRegex = "(^log-raw-boikgpokn78sb95ktmsc1bnk.*$)|(^log-raw-boikgpokn78sb95kjtihcg26.*$)".r
        val topics = Array("log-raw-boikgpokn78sb95ktmsc1bnk", "log-raw-boikgpokn78sb95kjtihcg26")
        topics.filter(topic => (topic.startsWith("__") == false && topicRegex.findFirstMatchIn(topic).isDefined)).foreach(println)

    }

    @Test
    def test6: Unit = {
        val fileNameR = "([a-zA-Z0-9]{32})_(\\d{10})_raw_\\d+_\\d+.json$".r
        val fileName="boikgpokn78sb95ktmsc1bnken8tuboa_2017060915_raw_1_480977748.json"
        val m = fileNameR.findFirstMatchIn(fileName)
        println(m.isDefined)
        if (m.isDefined) {
            val appId = m.get.group(1)
            val time = m.get.group(2)
            val day = time.substring(0, 8)
            val hour = time.substring(8)
            val targetPath = s"..../key_appId=${appId}/key_day=${day}/key_hour=${hour}/${fileName}"

            System.out.println(s"process file:$fileName -> ${targetPath}")
        }
    }

    @Test
    def test7:Unit={
        val processFn = ()  => {
            println("exit");

        }
        processFn()
        println("end")
    }

}

object StringTest {
    def byteArrayToInt(bytes: Array[Byte]): Int = {
        var value = 0
        val c = bytes.length - 1
        for (i <- 0 to c) {
            val shift = (4 - 1 - i) * 8;
            value = value + ((bytes(i) & 0x000000FF) << shift);
        }
        value
    }
}
