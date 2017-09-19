package cn.whaley.bi.logsys.log2parquet

import java.util.regex.Pattern

import cn.whaley.bi.logsys.log2parquet.RecordCompare.getFiles
import cn.whaley.bi.logsys.log2parquet.utils.{MetaDataUtils, MyAccumulator}
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by fj on 2017/6/29.
 */
class MetadataUtilTest_guohao {

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

      val acc = rdd.sparkContext.longAccumulator

    }

  @Test
  def testParseLogStrRddPath2(): Unit = {
    val context = getSparkContext()
    val rdd = context.textFile(testPath)

  }

    @Test
    def testParseLogObjRddPath(): Unit = {
        val context = getSparkContext()
        val rdd = context.textFile(testPath).map(row => JSON.parseObject(row))

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


        //pathRdd.take(10).foreach(println)


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



  }

  @Test
  def testJson(): Unit = {
    val jsonObj= JSON.parseObject("{'a':'b'}")
    jsonObj.put(" I Just Wanna Dance - 柳熙烈的写生簿 现场版 16/06/25","d")
    jsonObj.remove(" I Just Wanna Dance - 柳熙烈的写生簿 现场版 16/06/25")
    println(jsonObj.getString(" I Just Wanna Dance - 柳熙烈的写生簿 现场版 16/06/25"))
  }

  @Test
  def testReadPath(): Unit = {
    val context = getSparkContext()
    val rdd = context.textFile("hdfs://hans/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170101/*/")
    println("----------"+rdd.count())
  }

  @Test
  def test(): Unit ={
   val map = new mutable.HashMap[String,Integer]()
    val value = map.getOrElseUpdate("qq",3)
    if(value ==None){
      println("none")
    }else{
      println(value+1)
    }

  }


  @Test
  def test2(): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val myAccumulator = new MyAccumulator
    val sc = sparkSession.sparkContext
    sc.register(myAccumulator,"myAccumulator")

    val data = sc.parallelize(Array("a", "a", "a", "b", "b", "b", "c", "d", "d","d","e","e","f"), 3).map(f=>{
      myAccumulator.add(f)
      f
    })
    println(data.count())
    myAccumulator.value.foreach(r=>{
      println("key:"+r._1+"-> value:"+r._2)
    })
  }

    @Test
   def Test3(): Unit ={
      val lines = Source.fromFile("C:\\Users\\hc\\Desktop\\HardcoreSid.txt")
      lines.getLines().foreach(println(_))
    }

  @Test
  def  Test4(): Unit ={

    val inputPath = "/log/default/parquet/ods_view/1502958051545_json"
//    val context = getSparkContext()
    val config = new Configuration()
    val fs = FileSystem.get(config)
    val a = ArrayBuffer[String]()

    getFiles(a,fs,new Path(inputPath))

    println(a.size)
  }



  def getFiles(b:ArrayBuffer[String],fs: FileSystem,path: Path):Unit={
    val fileStatus = fs.listStatus(path)
    fileStatus.foreach(f=>{
      if(f.isDirectory){
        val path = f.getPath
        getFiles(b,fs,path)
      }else{
        if( !b.contains(f.getPath.getParent.toString.trim()) ){
          b += f.getPath.getParent.toString.trim()
        }
      }
    })
  }

}
