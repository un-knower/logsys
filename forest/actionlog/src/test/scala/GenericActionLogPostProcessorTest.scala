import cn.whaley.bi.logsys.common.{ConfManager, StringDecoder}
import cn.whaley.bi.logsys.forest.StringUtil
import cn.whaley.bi.logsys.forest.actionlog.{GenericActionLogGetProcessor, GenericActionLogPostProcessor, NgxLogJSONMsgProcessor}
import cn.whaley.bi.logsys.forest.entity.{LogEntity, MsgEntity}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/11/16.
 */
class GenericActionLogPostProcessorTest {

    val confManager = new ConfManager("GenericActionLogPostProcessor.xml" :: Nil)

    val processor = new GenericActionLogPostProcessor

    val getProcessor = new GenericActionLogGetProcessor

    processor.init(confManager)

    getProcessor.init(confManager)

    val ngxLogProcessor = new NgxLogJSONMsgProcessor

    ngxLogProcessor.init(confManager)

    @Test
    def testPost: Unit = {
        val stream = this.getClass.getClassLoader.getResourceAsStream("data/medusamain3.x.2")
        val source = scala.io.Source.fromInputStream(stream)
        //val lines = source.getLines().toArray
        val decoder = new StringDecoder()
//        val fileLines = source.getLines().map(item => StringUtil.decodeNgxStrToString(item)).toArray
        val fileLines = source.getLines().map(item => {
            val bytes = decoder.decodeToBytes(item)
            new String(bytes, "UTF-8")
        }).toArray
        fileLines.foreach(println(_))
        println("_________________________________________________-----")
        val lines = new ArrayBuffer[String]()

        for (i <- 1 to 1) {
            lines.append(fileLines: _*)
        }

        println("length:" + lines.length)
        val from = System.currentTimeMillis()
        val logs =
            lines.flatMap(item => {
                var json: JSONObject = null
                try {
                    json = JSON.parseObject(item)
                }
                catch {
                    case e: Throwable => {
                        println(item)
                        e.printStackTrace()
                    }
                }
                val ret = ngxLogProcessor.process(new MsgEntity(json))
                if (ret.hasErr) {
                    println(item)
                    ret.ex.get.printStackTrace()
                }
                require(ret.hasErr == false)
                ret.result.get
            })

        println("ts:" + (System.currentTimeMillis() - from))

        logs.foreach(logEntity => {
            println("==============================")
            println(logEntity.toJSONString)
            val ret = processor.process(logEntity)
            if (!ret.hasErr) {
                ret.result.get.foreach(item => println(item.toJSONString))
            }
            println("==============================")
        })

        println("ts:" + (System.currentTimeMillis() - from))

    }

    @Test
    def msgProcTest2(): Unit = {
        val msg = "{\"msgSignFlag\":0,\"msgId\":\"AAABW/U8FyAKE7pKHpRAHgAB\",\"msgFormat\":\"json\",\"msgBody\":{\"svr_content_type\":\"-\",\"svr_forwarded_for\":\"106.75.91.27\",\"svr_host\":\"whaleyapplog.aginomoto.com\",\"svr_remote_addr\":\"10.10.251.81\",\"svr_receive_time\":1494468007712,\"appId\":\"boikgpokn78sb95kjhfrendo8dc5mlsr\",\"svr_fb_Time\":\"2017-05-11T06:18:14.797Z\",\"svr_req_url\":\"/moretv/userdurationlog?account_id=&account_logout_time=1494468007&account_type=&app_version=3.1.3&area=&area_id=&city=&city_id=&client_ip=192.168.0.100&client_online=&country=&country_id=&county=&county_id=&create_time=&device_id=&device_logout_time=1494468007&device_type=PULIER_RK28_XX&dns=&id=&isp=&isp_id=&last_account_login_time=&last_account_logout_time=&last_account_online_time=&last_device_login_time=1494468007&last_device_logout_time=1494468007&last_device_online_time=1140&last_time=1494468007&mac=D896E0AB77ED&net_ip=&nick_name=&os=android&os_version=3.1.3-R-20161009.1012&phone=&product=moretv&real_client_ip=123.180.245.133&region=&region_id=&sn=TZ187I3B3P&svr_key=106.75.14.253_9998_300000_100017&total_account_online_time=&total_device_online_time=4070903&uid=c34d98664e36bc628b3c06ed4b1b8ea1&userType=device&user_online=&wifi_mac=\",\"body\":{},\"svr_req_method\":\"GET\"},\"msgSource\":\"ngx_log\",\"msgVersion\":\"1.0\",\"msgSite\":\"10.19.186.74\"}"
        val json = JSON.parseObject(msg)
        val ret = ngxLogProcessor.process(new MsgEntity(json))
        println(ret)

        ret.result.get.foreach(logEntity => {
            val ret2 = getProcessor.process(logEntity)
            println(ret2)
        })
    }

}
