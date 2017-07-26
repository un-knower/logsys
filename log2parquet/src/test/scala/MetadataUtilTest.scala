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

   // val testPath = "/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendoj8ilnoi7/key_day=20170630/key_hour=04/boikgpokn78sb95kjhfrendoj8ilnoi7_2017063004_raw_7_337326252.json.gz"
    //val testPath = "/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170707/key_hour=17/boikgpokn78sb95ktmsc1bnkechpgj9l_2017070717_raw_5_1046838202.json.gz"
    val testPath = "hdfs://hans/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170723/key_hour=01/boikgpokn78sb95ktmsc1bnkechpgj9l_2017072301_raw_0_1392502233.json.gz"

    //def getUtils()=new MetaDataUtils( "http://localhost:8084")
    def getUtils()=new MetaDataUtils( "http://bigdata-appsvr-130-5:8084")

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

        getUtils.parseLogStrRddPath(rdd).filter(rdd=>rdd._1.contains("035")).take(5).foreach(row => {
            println(row._1 + "\t" + row._2.toJSONString)
        })
    }

  @Test
  def testParseLogStrRddPath2(): Unit = {
    val context = getSparkContext()
    val rdd = context.textFile(testPath)
    getUtils.parseLogStrRddPath(rdd).map(e=>{
     e._1
    }).distinct().foreach(println)
  }

    @Test
    def testParseLogObjRddPath(): Unit = {
        val context = getSparkContext()
        val rdd = context.textFile(testPath).map(row => JSON.parseObject(row))
        getUtils.parseLogObjRddPath(rdd).take(10).foreach(row => {
            println(row._1 + "\t" + row._2.toJSONString)
        })
    }

    @Test
    def testResolveAppLogKeyFieldDescConfig(): Unit = {
        val conf = getUtils.resolveAppLogKeyFieldDescConfig(1)
        conf.foreach(println)
    }

    @Test
    def testParseSpecialRules(): Unit = {
        val utils=getUtils()
        val context = getSparkContext()
        val rdd = context.textFile(testPath).map(row => {
            val jsonObj= JSON.parseObject(row)
            jsonObj.getJSONObject("logBody").put("playStat5s","test")
            jsonObj.getJSONObject("logBody").put("pro","test")
            jsonObj.getJSONObject("logBody").put("accessLoc","test")
            jsonObj
        })

        val pathRdd = utils.parseLogObjRddPath(rdd)
        //pathRdd.take(10).foreach(println)
        utils.parseSpecialRules(pathRdd).take(10).foreach(row => {
            println(row)
        })

    }

    @Test
    def testReg() = {
        val pattern = Pattern.compile("whaleytv")
        println(pattern.matcher("/dw/whaleytv_dd/").find())

        println("whaleytv".r.findFirstMatchIn("ods_view.db/log_whaleytv_wui20/key_day=20170630/key_hour=04").isDefined)
    }


  @Test
  def testParseSpecialRules2(): Unit = {
    val utils=getUtils()
    val context = getSparkContext()
    val rdd = context.textFile(testPath).map(row => {
      val jsonObj= JSON.parseObject(row)
      jsonObj.getJSONObject("logBody").put(" I Just Wanna Dance - 柳熙烈的写生簿 现场版 16/06/25","")
    /*  jsonObj.getJSONObject("logBody").put("pro","test")
      jsonObj.getJSONObject("logBody").put("accessLoc","test")*/
      jsonObj
    })

    val pathRdd = utils.parseLogObjRddPath(rdd)
    //pathRdd.take(10).foreach(println)
    utils.parseSpecialRules(pathRdd).take(10).foreach(row => {
      println(row)
    })

  }

  @Test
  def testJson(): Unit = {
    val jsonObj= JSON.parseObject("{'a':'b'}")
    jsonObj.put(" I Just Wanna Dance - 柳熙烈的写生簿 现场版 16/06/25","d")
    jsonObj.remove(" I Just Wanna Dance - 柳熙烈的写生簿 现场版 16/06/25")
    println(jsonObj.getString(" I Just Wanna Dance - 柳熙烈的写生簿 现场版 16/06/25"))
  }

}
