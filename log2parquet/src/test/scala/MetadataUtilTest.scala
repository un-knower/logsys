import java.text.SimpleDateFormat
import java.util.Date

import cn.whaley.bi.logsys.log2parquet.utils.MetaDataUtils
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.junit.Test

/**
 * Created by fj on 2017/6/29.
 */
class MetadataUtilTest {

    val testPath = "/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95k7id7n8eb8dc5mlsr/key_day=20170629/key_hour=15/boikgpokn78sb95k7id7n8eb8dc5mlsr_2017062915_raw_5_15257483.json.gz"

    def getSparkContext() = {
        val conf = new SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName(this.getClass.getSimpleName)
        new org.apache.spark.SparkContext(conf)
    }

    @Test
    def testParseLogStrRddPath(): Unit = {
        val context = getSparkContext()
        val rdd = context.textFile(testPath)
        MetaDataUtils.parseLogStrRddPath(rdd).take(10).foreach(row => {
            println(row._1 + "\t" + row._2.toJSONString)
        })
    }

    @Test
    def testParseLogObjRddPath(): Unit = {
        val context = getSparkContext()
        val rdd = context.textFile(testPath).map(row =>JSON.parseObject(row))
        MetaDataUtils.parseLogObjRddPath(rdd).take(10).foreach(row => {
            println(row._1 + "\t" + row._2.toJSONString)
        })
    }

    @Test
    def testResolveAppLogKeyFieldDescConfig(): Unit = {
        val conf = MetaDataUtils.resolveAppLogKeyFieldDescConfig(1)
        conf.foreach(println)
    }

}
