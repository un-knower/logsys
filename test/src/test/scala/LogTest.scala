
import java.io.FileInputStream
import java.util.concurrent.Future

import cn.whaley.bi.logsys.common.{DigestUtil, ConfManager}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{RecordMetadata, ProducerRecord, KafkaProducer}
import org.junit.Test

import scalaj.http.{Http, HttpRequest}

class LogTest {


    @Test
    def test: Unit = {

        val dir = "/Users/fj/workspace/whaley/projects/WhaleyLogSys/test/src/test/datas/rawlogs"

        val aginomotoData = LogTest.parseJSONLines(s"${dir}/aginomoto.txt")
        val moretvData =  LogTest.parseJSONLines(s"${dir}/moretv.com.cn.post.txt")
        val moretvGetData =  LogTest.parseGetLogLines(s"${dir}/moretv.com.cn.get.txt")

        //aginomotoData.toArray.distinct.sortBy(_._1).foreach(println)
        //moretvData.toArray.distinct.sortBy(_._1).foreach(println)

        aginomotoData.foreach(data => {
            val host="test-wlslog.aginomoto.com"
            LogTest.sendHttp(host, data._2, data._3, data._4)
        })
        //moretvData.foreach(data=>{ sendHttp(data._1,data._2,data._3,data._4) })
        //moretvGetData.foreach(data => { sendHttp(data._1, data._2, data._3, data._4) })
    }


    @Test
    def test1(): Unit = {
        val stream = this.getClass.getClassLoader.getResourceAsStream("logcenter-tv.log")
        val source = scala.io.Source.fromInputStream(stream)
        val lines = source.getLines().toArray
        lines.foreach(item => {
            val json = JSON.parseObject(item)
            println(json.toJSONString)
        })
    }

    @Test
    def testProduceKafakData: Unit = {
        val confManager = new ConfManager(Array("kafka-consumer.xml", "kafka-producer.xml", "kafka.properties"))

        val conf = confManager.getAllConf("kafka-producer", true)
        val producer = new KafkaProducer[Array[Byte], Array[Byte]](conf)


        val topic = "pre-boikgpokn78sb95kjhfrendo8dc5mlsr"
        val stream = this.getClass.getClassLoader.getResourceAsStream("boikgpokn78sb95kjhfrendo8dc5mlsr.log")
        val source = scala.io.Source.fromInputStream(stream)
        val filelines = source.getLines().toArray

        for (i <- 1 to filelines.length - 1) {
            val key: Array[Byte] = i.toString.getBytes
            val value: Array[Byte] = filelines(i).getBytes()
            val record: ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
            val future: Future[RecordMetadata] = producer.send(record)
            future.get()
        }

        println(s"send ${filelines.length} message")

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
    def testHttpsNginxData: Unit = {
        val dir = "/Users/fj/workspace/whaley/projects/WhaleyLogSys/test/src/test/datas/rawlogs"
        val aginomotoData  = LogTest.parseJSONLines(s"${dir}/aginomoto.txt")
        val moretvData = LogTest.parseJSONLines(s"${dir}/moretv.com.cn.post.txt")
        val moretvGetData = LogTest.parseGetLogLines(s"${dir}/moretv.com.cn.get.txt")

        val testData=aginomotoData.union(moretvData).union(moretvGetData)

        while (true) {
            testData.foreach(data => {
                //val host=data._1
                val host = "wisp.aginomoto.com"
                val method = data._2
                val url = data._3
                val post = data._4
                LogTest.sendHttp(host, method, url, post)
                //LogTest.sendHttps(host, method, "/log/boikgpokn78sb95kjtihcg268dc5mlsr", post)
            })
        }


    }
}

object LogTest {
    val signKey = "92DOV+sOk160j=430+DM!ZzESf@XkEsn#cKanpB$KFB6%D8z4C^xg7cs6&7wn0A4A*iR9M3j)pLs]ll5E9aFlU(dE0[QKxHZzC.CaO/2Ym3|Tk<YyGZR>WuRUmI?x2s:Cg;YEA-hZubmGnWgXE"

    /**
     *
     * @param filepath
     * @return [host,method,url,data]
     */
    def parseJSONLines(filepath: String): Seq[(String, String, String, String)] = {
        val stream = new FileInputStream(filepath)
        val source = scala.io.Source.fromInputStream(stream)
        val info = source.getLines().flatMap(line => {
            try {
                val jsonObj = JSON.parseObject(line)
                if (jsonObj.containsKey("tags")) {
                    var urlPath = jsonObj.getString("urlPath")
                    if (!urlPath.startsWith("/")) urlPath = "/" + urlPath
                    if (urlPath.endsWith("/")) urlPath = urlPath.substring(0, urlPath.length - 1)
                    (jsonObj.getString("host"), jsonObj.getJSONObject("tags").getString("method").toUpperCase(), urlPath, line) :: Nil
                } else {
                    new Array[(String, String, String, String)](0)
                }
            } catch {
                case e: Throwable => {
                    new Array[(String, String, String, String)](0)
                }
            }
        }).toSeq
        info
    }

    /**
     *
     * @param filepath
     * @return [host,method,url,data]
     */
    def parseGetLogLines(filepath: String): Seq[(String, String, String, String)] = {
        val stream = new FileInputStream(filepath)
        val source = scala.io.Source.fromInputStream(stream)
        val info = source.getLines().map(_.trim).filter(line => {
            line.length > 0 && !line.startsWith("#")
        }).flatMap(line => {
            val fields = line.split(" ")
            val host = fields(1)
            val urlPath = fields(7)
            (host, "GET", urlPath, line) :: Nil
        }).toSeq
        info
    }

    //(ts,md5)
    def getSignMd5Info(logBody: String): (String, String) = {
        val ts = System.currentTimeMillis().toString
        val str = ts + logBody + signKey
        val md5 = DigestUtil.getMD5Str32(str)
        (ts, md5)
    }

    def sendHttp(host: String, method: String, urlPath: String, data: String): Unit = {
        val signInfo = getSignMd5Info(data)
        //val url = s"http://${host}:81${urlPath}"
        val url = s"http://${host}:80${urlPath}"
        val request: HttpRequest = Http(url).method(method).header("forwardhost", host)
        try {
            val res = method match {
                case "GET" => {
                    request.asString
                }
                case "POST" => {
                    request.header("log-sign-method", "md5")
                        .header("log-sign-version", "1.0")
                        .header("log-sign-ts", signInfo._1)
                        .header("log-sign-value", signInfo._2)
                        .header("content-type", "application/json")
                        .postData(data.getBytes).asString
                }
            }
            println(s"url:$url ; res:${res}")
        } catch {
            case e: Throwable => {
                println(s"url:$url, data:$data")
                e.printStackTrace()
            }
        }

    }

    def sendHttps(host: String, method: String, urlPath: String, data: String): Unit = {
        val signInfo = getSignMd5Info(data)
        //val url = s"http://${host}:81${urlPath}"
        val url = s"https://$host${urlPath}"
        val request: HttpRequest = Http(url).method(method).header("forwardhost", host)
        try {
            val res = method match {
                case "GET" => {
                    request.asString
                }
                case "POST" => {
                    request.header("log-sign-method", "md5")
                        .header("log-sign-version", "1.0")
                        .header("log-sign-ts", signInfo._1)
                        .header("log-sign-value", signInfo._2)
                        .header("content-type", "application/json")
                        .postData(data.getBytes).asString
                }
            }
            println(s"url:$url ; res:${res}")
        } catch {
            case e: Throwable => {
                println(s"url:$url, data:$data")
                e.printStackTrace()
            }
        }

    }


}