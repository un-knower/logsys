import java.text.SimpleDateFormat

import cn.whaley.bi.logsys.common.{KafkaUtil, ConfManager}
import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.junit.Test


/**
 * Created by fj on 17/5/7.

sh ./bin/kafka-topics.sh --create --zookeeper bigdata-computing-01-010:2182 --replication-factor 2 --partitions 20  --topic log-raw-boikgpokn78sb95kjhfrendoj8ilnoi7
sh ./bin/kafka-topics.sh --list --zookeeper bigdata-computing-01-010:2182

 */
class TestDataProducerTest {

    val consumerConf = new ConfManager(Array("settings.properties", "kafka-consumer.xml"))
    val producerConf = new ConfManager(Array("settings.properties", "kafka-producer.xml"))

    def getKafkaProducer[K, V](): KafkaProducer[K, V] = {
        val conf = producerConf.getAllConf("kafka-producer", true)
        new KafkaProducer[K, V](conf)
    }

    def getKafkaUtil(): KafkaUtil = {
        val conf = consumerConf.getAllConf("kafka-consumer", true)
        val servers = conf.getProperty("bootstrap.servers")
        KafkaUtil(servers)
    }

    @Test
    def printOffset(): Unit = {
        val topic = "log-raw-boikgpokn78sb95kjhfrendoj8ilnoi7"
        val kafkaUtil = getKafkaUtil()
        val offsetInfo = kafkaUtil.getLatestOffset(topic).mkString(",")
        println("after:" + offsetInfo)
    }

    @Test
    def producerTest1(): Unit = {

        val topic = "log-raw-boikgpokn78sb95kjhfrendoj8ilnoi7"
        val resNames = Array("data/boikgpokn78sb95kjhfrendoj8ilnoi7.log-2017050718-bigdata-extsvr-log1", "data/boikgpokn78sb95kjhfrendoj8ilnoi7.log-2017050718-bigdata-extsvr-log3")

        val kafkaUtil = getKafkaUtil()
        val producer = getKafkaProducer[Array[Byte], Array[Byte]]()

        resNames.foreach(resName => {
            val offsetInfoBefore = kafkaUtil.getLatestOffset(topic).mkString(",")
            println("before:" + offsetInfoBefore)

            val resURI = this.getClass.getClassLoader.getResource(resName)
            val source = scala.io.Source.fromInputStream(resURI.openStream())
            source.getLines().foreach(line => {
                val fbJson = new JSONObject()
                fbJson.put("@timestamp", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()))
                fbJson.put("type", "log")
                fbJson.put("message", line)
                val valueBytes = fbJson.toJSONString.getBytes()
                val record: ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, valueBytes)
                producer.send(record)
            })
            producer.flush()
            val offsetInfoAfter = kafkaUtil.getLatestOffset(topic).mkString(",")
            println("after:" + offsetInfoAfter)
        })
    }

}
