import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.{ProcessResultCode, Constants}
import cn.whaley.bi.logsys.forest.actionlog.GenericActionLogGetProcessor
import cn.whaley.bi.logsys.forest.entity.{MsgEntity, LogEntity}
import com.alibaba.fastjson.JSONObject
import org.junit.Test

/**
 * Created by fj on 16/11/15.
 */
class GenericActionLogGetProcessorTest {

    val confManager = new ConfManager("" :: Nil)

    val processor = new GenericActionLogGetProcessor

    processor.init(confManager)

    @Test
    def testProcess(): Unit = {
        val ts = System.currentTimeMillis()
        for (i <- 1 to 100000) {
            val bodyValue = new JSONObject()
            bodyValue.put("svr_req_method", "GET")
            bodyValue.put("svr_req_url", "/uploadlog?log=live-001-MoreTV_TVApp2.0_Android_2.5.9-f360a9dcc80919e308f8465760df9f37--760-home-TVlive-1-abx07nopuwnp-28-guanwang-101010300-MiBOX2-20161111192220")
            bodyValue.put("svr_receive_time", System.currentTimeMillis())
            bodyValue.put("appId", Constants.APPID_MEDUSA_2_0)

            val jsonValue = new JSONObject()
            jsonValue.put(MsgEntity.KEY_MSG_ID, "msg001")
            jsonValue.put(MsgEntity.KEY_MSG_FORMAT, "ngx_log")
            jsonValue.put(MsgEntity.KEY_MSG_SIGN_FLAG, 0)
            jsonValue.put(MsgEntity.KEY_MSG_SITE, "test")
            jsonValue.put(MsgEntity.KEY_MSG_SOURCE, "test")
            jsonValue.put(MsgEntity.KEY_MSG_VERSION, "1.0")
            jsonValue.put(MsgEntity.KEY_MSG_BODY,bodyValue)

            val logEntity = LogEntity.create(jsonValue)

            val ret = processor.process(logEntity)

            if (ret.code != ProcessResultCode.skipped) {
                if (i == 1) {
                    ret.result.get.foreach(item => {
                        println(item.toJSONString)
                    })
                }
            } else {
                println(ret)
            }
        }
        println(s"ts:${System.currentTimeMillis() - ts}")

    }

}
