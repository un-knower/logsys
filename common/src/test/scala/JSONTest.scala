import com.alibaba.fastjson.JSONObject
import org.junit.Test


/**
 * Created by fj on 16/10/30.
 */
class JSONTest {
    @Test
    def test1: Unit ={
        val jsonObj=new JSONObject()
        jsonObj.put("a","vvv")
        jsonObj.put("b","sdfsadf")
        println(jsonObj.toJSONString)
        jsonObj.remove("a")
        println(jsonObj.toJSONString)
    }

}
