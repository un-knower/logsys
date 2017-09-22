import cn.whaley.bi.logsys.batchforest.traits.LogTrait
import cn.whaley.bi.logsys.batchforest.util.MsgDecoder
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.junit.Test

/**
  * Created by guohao on 2017/8/28.
  */
class MainObjTest extends LogTrait{
  val inputPath = "/data_warehouse/ods_origin.db/log_raw/key_day=20170828/key_hour=00/boikgpokn78sb95ktmsc1bnkechpgj9l.log-2017082800-bigdata-extsvr-log1"
  def getSparkContext() = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName(this.getClass.getSimpleName)
    new org.apache.spark.SparkContext(conf)
  }
  @Test
  def test(): Unit ={
    val sparkContext = getSparkContext()
    val decoderRdd = sparkContext.textFile(inputPath).map(line=>{
        MsgDecoder.decode(line)
      })

    decoderRdd.filter(f=>(!f.get.isEmpty)).take(10).foreach(r=>{
      println(r.get)
    })
  }
    @Test
    def test2(): Unit ={
      val json = "{\"svr_content_type\":\"-\",\"svr_forwarded_for\":\"123.59.77.3\",\"svr_host\":\"vrlog.aginomoto.com\",\"svr_remote_addr\":\"10.19.251.56\",\"svr_receive_time\":1503851842892,\"appId\":\"boikgpokn78sb95kbqei6cc98dc5mlsr\",\"svr_req_url\":\"/uploadlog\",\"body\":[{\"a\":\"a\"},{\"b\":\"b\"}],\"svr_req_method\":\"GET\"}"
      val msgBody = JSON.parseObject(json)
      val array = msgBody.getJSONArray("body")
      msgBody.remove("body")
      for(i<- 0 to array.size() -1) yield {
        val item = array.get(i).asInstanceOf[JSONObject]
        item.asInstanceOf[java.util.Map[String,Object]].putAll(msgBody)
        println(item)
        item
      }


  }

  @Test
  def testGZ(): Unit ={
    val inputPath = "/data_warehouse/ods_origin.db/log_origin/key_appId2=boikgpokn78sb95ktmsc1bnkklf477ap/key_day=20170905/key_hour=19/boikgpokn78sb95ktmsc1bnkklf477ap_95065f50-41b3-496a-a53b-aab95399be50.json.gz"
    val sparkContext = getSparkContext()
    val rdd = sparkContext.textFile(inputPath).map(line=>{
      println(line)
    })

    println(s"count ${rdd.count()}")
  }

  @Test
  def testFilter(): Unit ={
    var array = Array("1","2","3","4")
    val array2 = Array("3","4")
    array = array.filter(f=>{
      if(!array2.contains(f)){
        true
      }else{
        false
      }

    })

    println(array.toList.toString())
  }


  @Test
  def test11(): Unit ={
    val inputPath1 = "/data_warehouse/ods_view.db/log_medusa_main3x_*/key_day=20170918/*"
    val inputPath2 = "/data_warehouse/ods_view.db/log_medusa_main3x_default/key_day=20170918/*"
    val inputPath3 = "/data_warehouse/ods_view.db/log_whaleytv_main_*/key_day=20170918/*"
    val inputPath4 = "/data_warehouse/ods_view.db/log_whaleytv_main_default/key_day=20170918/*"
    val sparkContext = getSparkContext()
    val cn1 = sparkContext.textFile(inputPath1).count()
    println(s"cn1 $cn1")
    val cn2 = sparkContext.textFile(inputPath2).count()
    println(s"cn2 $cn2")
    val cn3 = sparkContext.textFile(inputPath3).count()
    println(s"cn3 $cn3")
    val cn4 = sparkContext.textFile(inputPath4).count()
    println(s"cn4 $cn4")



  }
}
