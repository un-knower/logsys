
import java.io.FileInputStream
import java.util.concurrent.Future

import cn.whaley.bi.logsys.common.ConfManager
import com.alibaba.fastjson.{JSONObject, JSON}
import org.apache.kafka.clients.producer.{KafkaProducer, RecordMetadata, ProducerRecord}
import org.junit.Test

import scalaj.http._

class LogTest {

    @Test
    def test: Unit = {
        import scalaj.http._
        import com.alibaba.fastjson.{JSONObject, JSON}
        val dir = "/Users/fj/workspace/whaley/projects/WhaleyLogSys/gathersys/actionlog/src/test/datas/rawlogs"

        val parseJSONLines = (filepath: String) => {
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
            })
            info
        }

        val parseGetLogLines = (filepath: String) => {
            val stream = new FileInputStream(filepath)
            val source = scala.io.Source.fromInputStream(stream)
            val info = source.getLines().map(_.trim).filter(line => {
                line.length > 0 && !line.startsWith("#")
            }).flatMap(line => {
                val fields = line.split(" ")
                val host = fields(1)
                val urlPath = fields(7)
                (host, "GET", urlPath, line) :: Nil
            })
            info
        }

        val aginomotoData = parseJSONLines(s"${dir}/aginomoto.txt")
        val moretvData = parseJSONLines(s"${dir}/moretv.com.cn.post.txt")
        val moretvGetData = parseGetLogLines(s"${dir}/moretv.com.cn.get.txt")

        //aginomotoData.toArray.distinct.sortBy(_._1).foreach(println)
        //moretvData.toArray.distinct.sortBy(_._1).foreach(println)

        val sendHttp = (host: String, method: String, urlPath: String, data: String) => {
            val url = s"http://${host}:81${urlPath}"
            val request: HttpRequest = Http(url).method(method)
            try {
                val res = method match {
                    case "GET" => {
                        request.asString
                    }
                    case "POST" => {
                        request.postData(data.getBytes).header("content-type", "application/json").asString
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

        aginomotoData.foreach(data=>{ sendHttp(data._1,data._2,data._3,data._4) })
        moretvData.foreach(data=>{ sendHttp(data._1,data._2,data._3,data._4) })
        moretvGetData.foreach(data => { sendHttp(data._1, data._2, data._3, data._4) })
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
}