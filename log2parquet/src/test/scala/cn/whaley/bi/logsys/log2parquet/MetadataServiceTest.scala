package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.log2parquet.utils.MetadataService
import cn.whaley.bi.logsys.metadata.entity.LogFileKeyFieldValueEntity
import com.alibaba.fastjson.{JSON, JSONObject, JSONArray}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 2017/7/4.
 */
class MetadataServiceTest {

    val server = "http://localhost:8084"

    def getService() = {
        new MetadataService(server)
    }

    @Test
    def testGetAllAppLogKeyFieldDesc(): Unit = {
        val data = getService.getAllAppLogKeyFieldDesc()
        data.foreach(item => println(JSON.toJSONString(item,true)))
    }

    @Test
    def testGetAllAppLogSpecialFieldDesc(): Unit = {
        val data = getService.getAllAppLogSpecialFieldDesc()
        data.foreach(item => println(JSON.toJSONString(item,true)))
    }

    @Test
    def testPutLogFileKeyFieldValue(): Unit = {
        val datas = new ArrayBuffer[LogFileKeyFieldValueEntity]()
        val item1 = new LogFileKeyFieldValueEntity()
        item1.setAppId("app1")
        item1.setLogPath("/test/log1.txt")
        item1.setFieldName("productCode")
        item1.setFieldValue("medusa")
        item1.setTaskId("task1")
        datas.append(item1)
        val data = getService.putLogFileKeyFieldValue("task1", datas)
        println(data.toJSONString)
    }


}
