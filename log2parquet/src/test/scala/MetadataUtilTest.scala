import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import cn.whaley.bi.logsys.log2parquet.utils.MetaDataUtils
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.junit.Test

/**
 * Created by fj on 2017/6/29.
 */
class MetadataUtilTest {

    val testPath = "/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendoj8ilnoi7/key_day=20170630/key_hour=04/boikgpokn78sb95kjhfrendoj8ilnoi7_2017063004_raw_7_337326252.json.gz"

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
        val rdd = context.textFile(testPath).map(row => JSON.parseObject(row))
        MetaDataUtils.parseLogObjRddPath(rdd).take(10).foreach(row => {
            println(row._1 + "\t" + row._2.toJSONString)
        })
    }

    @Test
    def testResolveAppLogKeyFieldDescConfig(): Unit = {
        val conf = MetaDataUtils.resolveAppLogKeyFieldDescConfig(1)
        conf.foreach(println)
    }

    @Test
    def testParseSpecialRules(): Unit = {
        val context = getSparkContext()
        val rdd = context.textFile(testPath).map(row => {
            val jsonObj= JSON.parseObject(row)
            jsonObj.getJSONObject("logBody").put("playStat5s","test")
            jsonObj.getJSONObject("logBody").put("pro","test")
            jsonObj
        })

        val pathRdd = MetaDataUtils.parseLogObjRddPath(rdd)
        pathRdd.take(10).foreach(println)
        MetaDataUtils.parseSpecialRules(pathRdd).take(10).foreach(row => {
            println(row)
        })

    }

    @Test
    def testReg() = {
        val pattern = Pattern.compile("whaleytv")
        println(pattern.matcher("/dw/whaleytv_dd/").find())

        println("whaleytv".r.findFirstMatchIn("ods_view.db/log_whaleytv_wui20/key_day=20170630/key_hour=04").isDefined)
    }


}
