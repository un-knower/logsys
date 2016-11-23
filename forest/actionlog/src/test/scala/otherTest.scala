import com.alibaba.fastjson.{JSONArray, JSON, JSONObject}
import org.junit.Test

/**
 * Created by fj on 16/11/16.
 */
class otherTest {
    @Test
    def testReg: Unit = {
        val quotJSONRegex = "\"(.*?)\":\\s?\"(\\{.*?\\})\"".r
        val source = "{\"baseInfo\":\"{\"userId\":\"2437afb48567015174f0c23f5ed96189\",\"accountId\":\"45172448\",\"groupId\":\"2961\",\"weatherCode\":\"101090801\",\"productSN\":\"KF1621C1000830F088\",\"productModel\":\"W50J\",\"romVersion\":\"01.20.05\",\"firmwareVersion\":\"HELIOSR-01.20.05.1641214\",\"productLine\":\"helios\",\"sysPlatform\":\"nonyunos\"}\",\"date\":\"2016-11-16\",\"remoteIp\":\"122.226.183.186\",\"version\":\"01\",\"urlPath\":\"romlog/\",\"tags\":{\"product\":\"helios\",\"method\":\"POST\"},\"logTimestamp\":1479264530734,\"datetime\":\"2016-11-16 10:48:50\",\"hour\":\"10\",\"host\":\"tvlog.aginomoto.com\",\"forwardedIp\":\"122.226.183.186\",\"logs\":\"[{\"logVersion\":\"02\",\"logType\":\"event\",\"happenTime\":1479264480655,\"eventId\":\"whaley-rom-multimedia-info\",\"params\":{\"mimeType\":\"audio\\/mp4a-latm\",\"audioCodec\":\"AAC\",\"videoCodec\":\"H264\"}}]\",\"day\":\"2016-11-16\",\"ts\":1479264530452,\"md5\":\"7d7db0fe76deceb710e79cb1aa28f8e7\"}"
        val seq =
            quotJSONRegex.findAllMatchIn(source).map(item => {
                (item.group(1), item.group(2))
            })
        seq.foreach(item => {
            println(s"1:${item._1}\n2:${item._2}")
        })
    }



}
