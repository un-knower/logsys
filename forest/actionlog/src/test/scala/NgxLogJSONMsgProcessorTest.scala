import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.actionlog.{NgxLogJSONMsgProcessor, GenericActionLogPostProcessor}
import cn.whaley.bi.logsys.forest.entity.MsgEntity
import com.alibaba.fastjson.JSON
import org.junit.Test

/**
 * Created by fj on 16/11/16.
 */
class NgxLogJSONMsgProcessorTest {

    val confManager = new ConfManager("" :: Nil)

    val ngxLogProcessor = new NgxLogJSONMsgProcessor

    ngxLogProcessor.init(confManager)

    @Test
    def testProcess: Unit = {
        val stream = this.getClass.getClassLoader.getResourceAsStream("boikgpokn78sb95kjhfrendo8dc5mlsr.log")
        val source = scala.io.Source.fromInputStream(stream)
        val lines = source.getLines()
        lines.foreach(item => {
            val json = JSON.parseObject(item)
            val msgRet = ngxLogProcessor.process(new MsgEntity(json))
            require(msgRet.hasErr == false)
            msgRet.result.get.foreach(logEntity => {
                println(logEntity.toJSONString)
            })
        })
    }
}
