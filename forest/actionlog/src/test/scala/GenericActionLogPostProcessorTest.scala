import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.StringUtil
import cn.whaley.bi.logsys.forest.actionlog.{NgxLogJSONMsgProcessor, GenericActionLogPostProcessor, GenericActionLogGetProcessor}
import cn.whaley.bi.logsys.forest.entity.{MsgEntity, LogEntity}
import com.alibaba.fastjson.{JSONObject, JSON}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/11/16.
 */
class GenericActionLogPostProcessorTest {

    val confManager = new ConfManager("GenericActionLogPostProcessor.xml" :: Nil)

    val processor = new GenericActionLogPostProcessor

    processor.init(confManager)

    val ngxLogProcessor = new NgxLogJSONMsgProcessor

    ngxLogProcessor.init(confManager)

    @Test
    def testPost: Unit = {
        val stream = this.getClass.getClassLoader.getResourceAsStream("boikgpokn78sb95kjhfrendo8dc5mlsr.log")
        val source = scala.io.Source.fromInputStream(stream)
        //val lines = source.getLines().toArray
        val fileLines = source.getLines().map(item => StringUtil.decodeNgxStrToString(item)).toArray
        val lines = new ArrayBuffer[String]()
        for (i <- 1 to 100) {
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
            val ret = processor.process(logEntity)
            if (ret.hasErr) {
                ret.ex.get.printStackTrace()
            }
            require(ret.hasErr == false)
            //println(ret.code)
        })

        println("ts:" + (System.currentTimeMillis() - from))

    }

}
