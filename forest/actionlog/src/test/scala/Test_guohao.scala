import cn.whaley.bi.logsys.forest.Traits.LogTrait
import com.alibaba.fastjson.{JSON, JSONArray}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/11/20.
 */
class Test_guohao extends LogTrait{
    @Test
    def test1: Unit = {
        val str = new ArrayBuffer[String]()
        str += "aa"
        str += "bb"
        println(str.mkString("->"))
    }
    @Test
    def test2: Unit ={
        val str = "{\"a\":\"1\",\"bodyObj\":{\"baseInfo\":{\"baseInfo1\":\"baseInfo1\",\"baseInfo2\":\"baseInfo2\"},\"logs\":[{\"logs1\":\"logs1\"},{\"logs2\":\"logs2\"}]}}"
        val json = JSON.parseObject(str)
        val bodyObj = json.getJSONObject("bodyObj")
        val baseInfo = bodyObj.getJSONObject("baseInfo")
        bodyObj.remove("baseInfo")
        bodyObj.asInstanceOf[java.util.Map[String,Object]].putAll(baseInfo)

        val logs = bodyObj.getJSONArray("logs")
        bodyObj.remove("logs")
        val array = new JSONArray()
        val size = logs.size()
        for(i<-0 to size-1) yield {
            val log = logs.getJSONObject(i)
            log.asInstanceOf[java.util.Map[String,Object]].putAll(bodyObj)
//            bodyObj.asInstanceOf[java.util.Map[String,Object]].putAll(log)
            array.add(bodyObj)
        }


        array.toArray.map(f=>{
//            println(f)
            println("---------")
            println(JSON.parseObject(f.toString))
//
        })

    }


}
