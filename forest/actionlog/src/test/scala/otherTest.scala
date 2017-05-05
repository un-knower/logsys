import java.util.Scanner
import java.util.concurrent.ArrayBlockingQueue

import com.alibaba.fastjson.{JSONArray, JSON, JSONObject}
import org.junit.Test

import scalaj.http.{Http, HttpRequest}

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


    @Test
    def testProduceNginxData: Unit = {
        val stream = this.getClass.getClassLoader.getResourceAsStream("logcenter-tv.log")
        val source = scala.io.Source.fromInputStream(stream)
        val lines = source.getLines().toArray
        var count = 0
        for (i <- 0 to 10) {
            lines.foreach(item => {
                val request: HttpRequest = Http("http://logupload.aginomoto.com:8180/")
                val res = request.postData(item.getBytes).header("content-type", "application/json").asString
                count = count + 1
                println(s"${count}:${res}")

                //if (count > 10) return
            })
        }

    }

    @Test
    def testBlockQueueStop: Unit = {
        val queue=new ArrayBlockingQueue[String](10)
        val t = new Thread {
            override def run(): Unit = {
                try{
                    queue.take()
                }catch {
                    case e:InterruptedException=>{
                        println("thread interrupted.")
                    }
                }
            }
        }
        t.start()

        Thread.sleep(1000)
        println("send interrupted")
        t.interrupt()

        Thread.sleep(3000)
    }

    @Test
    def testJsonObj():Unit={
        val msg=new JSONObject()
        msg.put("msg_key1","msg_val1")
        msg.put("msg_key2","msg_val2")
        val msg2=new JSONObject(msg)

        println(msg.toJSONString)
        println(msg2.toJSONString)

        msg.put("msg_key1","msg_val1_1")
        msg.put("msg_key3","msg_val3")

        println(msg.toJSONString)
        println(msg2.toJSONString)

    }

}
