
import java.util.concurrent.Future

import cn.whaley.bi.logsys.common.ConfManager
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, RecordMetadata, ProducerRecord}
import org.junit.Test

import scalaj.http._

class LogTest {


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
        for(i <- 0 to 10) {
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