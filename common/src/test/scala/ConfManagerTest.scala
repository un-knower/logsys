import java.io.FileOutputStream
import java.util.Properties

import cn.whaley.bi.logsys.common.ConfManager
import org.junit.Test

/**
 * Created by fj on 16/10/30.
 */
class ConfManagerTest extends LogTrait {
    @Test
    def testGetConf: Unit = {
        val res = "kafka-producer.xml" :: "log4j.properties" :: Nil
        val confManager = new ConfManager(res)

        LOG.info("ll:{},kk:{}",confManager.getConf("ll"),confManager.getConf("kk"),"")

        LOG.info("producerMap:{}", confManager.getAllConf("kafka-producer", true))

        LOG.info("producerMap:{}", confManager.getAllConf("kafka-producer", false))

        val props=new Properties()
        props.put("value.serializer2","org.apache.kafka.common.serialization.ByteArraySerializer2")
        val confManager2 = new ConfManager(props,res)
        LOG.info("producerMap:{}", confManager2.getAllConf("kafka-producer", true))

        confManager.writeXml(new FileOutputStream("/tmp/1.xml"))

    }

}
