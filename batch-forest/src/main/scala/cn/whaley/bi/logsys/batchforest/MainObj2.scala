package cn.whaley.bi.logsys.batchforest

import java.text.SimpleDateFormat
import java.util.Date

import cn.whaley.bi.logsys.batchforest.process._
import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}
import cn.whaley.bi.logsys.batchforest.util._
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession





/**
  * Created by guohao on 2017/8/28.
  * 程序入口
  */

object MainObj2 extends NameTrait with LogTrait{
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    try{
      val sparkConf = new SparkConf()
      sparkConf.setAppName(this.name)
      sparkConf.setMaster("local[2]")
      val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

      val sparkContext = sparkSession.sparkContext
      val myAccumulator =  new MyAccumulator
      sparkContext.register(myAccumulator,"myAccumulator")
      //whaley main
//      val inputPath = "/data_warehouse/ods_origin.db/log_raw/key_day=20171115/key_hour=13/boikgpokn78sb95kjhfrendo8dc5mlsr.log-2017111513-bigdata-extsvr-log1"
//      val inputPath = "/data_warehouse/ods_origin.db/log_origin/activity.json"
      //eagle
//      val inputPath = "/data_warehouse/ods_origin.db/log_raw/key_day=20171115/key_hour=15/boikgpokn78sb95k7id7n8eb8dc5mlsr.log-2017111515-bigdata-extsvr-log1"
//      val inputPath = "/data_warehouse/ods_origin.db/log_origin/eagle.json"
//      val inputPath = "/data_warehouse/ods_origin.db/log_origin/global_menu2.json"
//      val inputPath = "/data_warehouse/ods_origin.db/log_origin/wui20.json"
//      val inputPath = "/data_warehouse/ods_origin.db/log_origin/enter.json"
      val inputPath = "/data_warehouse/ods_origin.db/log_origin/eagle_crash"
      var inputRdd:RDD[String ] = sparkContext.textFile(inputPath)
      //1.日志解码
      val decodeRdd = inputRdd.map(line=>{
        myAccumulator.add("inputRecord")
//        LogFormat.decode(line)
        Some(line)
      })
      //2.验证日志格式
      val formatRdd = decodeRdd.filter(f=>{
        if(f.isEmpty){
          myAccumulator.add("decodeExcRecord")
          false
        }else{
          true
        }
      }).map(r=>{
        val line = r.get
        try{
          //非json格式数据丢弃
          println(s" line  ${line}")
          val message = JSON.parseObject(line)
          LogFormat.verificationFormat(message)
        }catch {
          case e:Exception=>{
            // LOG.error(s"日志格式异常log=>$line ,e => $e")
            None
          }
        }
      })
      //crash ,get,post 解析
      val logs = formatRdd.filter(f => {
        if(f.isEmpty){
          myAccumulator.add("logFormatExcRecord")
          false
        }else{
          true
        }
      }).map(r => {
        val message = r.get
        handleMessage(message)(myAccumulator)
      })
      //时间字段处理
      val resultRdd = logs.flatMap(x=>x).filter(f=>(!f.isEmpty)).map(r=>{
        myAccumulator.add("outputRecord")
        val log= r.get
        val logTime = log.getLong("logTime")
        val date = dateFormat.format(new Date(logTime))
        val datetime = datetimeFormat.format(new Date(logTime))
        log.put("date",date)
        log.put("day",date)
        log.put("logTimestamp",logTime)
        log.put("datetime",datetime)
        log
      })
//        .filter(f=>
//        (
//          f.getJSONObject("logBody").getString("apkVersion").equals("2.1.7")
//        && f.getJSONObject("logBody").getString("eventId").equalsIgnoreCase("letter_search_click")
//          )
//      )

      println("---------------------")
      resultRdd.foreach(f=>{
        println(f)
      })
      println("---------------------")
      myAccumulator.value.keys.foreach(key=>{
        val value = myAccumulator.value.getOrElseUpdate(key,0)
        println(s"$key -> $value")
      })

      println("##########myAccumulator start###############")
      println(s"inputRecord-> ${myAccumulator.value.getOrElseUpdate("inputRecord",0)}")
      println(s"decodeExcRecord-> ${myAccumulator.value.getOrElseUpdate("decodeExcRecord",0)}")
      println(s"logFormatExcRecord-> ${myAccumulator.value.getOrElseUpdate("logFormatExcRecord",0)}")
      println(s"handleRecord=handleCrashRecord+handleGetRecord+handlePostRecord")
      println(s"handleRecord-> ${myAccumulator.value.getOrElseUpdate("handleRecord",0)}")
      println(s"handleExcRecord-> ${myAccumulator.value.getOrElseUpdate("handleExcRecord",0)}")
      println(s"crash ... start ")
      println(s"chandleCrashRecord=handleCrashMd5ErrRecord+handleCrashOutRecord ")
      println(s"chandleCrashRecord=handleCrashNoMd5Record+handleCrashMd5Record ")
      println(s"handleCrashRecord-> ${myAccumulator.value.getOrElseUpdate("handleCrashRecord",0)}")
      println(s"handleCrashNoMd5Record-> ${myAccumulator.value.getOrElseUpdate("handleCrashNoMd5Record",0)}")
      println(s"handleCrashMd5Record-> ${myAccumulator.value.getOrElseUpdate("handleCrashMd5Record",0)}")
      println(s"handleCrashMd5ErrRecord-> ${myAccumulator.value.getOrElseUpdate("handleCrashMd5ErrRecord",0)}")
      println(s"handleCrashOutRecord-> ${myAccumulator.value.getOrElseUpdate("handleCrashOutRecord",0)}")
      println(s"crash ... end")

      println(s"get ... start ")
      println(s"handleGetRecord-> ${myAccumulator.value.getOrElseUpdate("handleGetRecord",0)}")
      println(s"handleGetExcRecord-> ${myAccumulator.value.getOrElseUpdate("handleGetExcRecord",0)}")
      println(s"handleGetOutRecord-> ${myAccumulator.value.getOrElseUpdate("handleGetOutRecord",0)}")
      println(s"get ... end ")


      println(s"post ... start ")
      println(s"handlePostRecord-> ${myAccumulator.value.getOrElseUpdate("handlePostRecord",0)}")
      println(s"post ... handlePostVerifyRecord=handlePostVerifyMd5SucessRecord+handlePostVerifyMd5ErrRecord+handlePostVerifyNoMd5Record ")
      println(s"handlePostVerifyRecord-> ${myAccumulator.value.getOrElseUpdate("handlePostVerifyRecord",0)}")
      println(s"handlePostVerifyMd5SucessRecord-> ${myAccumulator.value.getOrElseUpdate("handlePostVerifyMd5SucessRecord",0)}")
      println(s"handlePostVerifyMd5ErrRecord-> ${myAccumulator.value.getOrElseUpdate("handlePostVerifyMd5ErrRecord",0)}")
      println(s"handlePostVerifyNoMd5Record-> ${myAccumulator.value.getOrElseUpdate("handlePostVerifyNoMd5Record",0)}")
      println(s"handlePostExc-> ${myAccumulator.value.getOrElseUpdate("handlePostExc",0)}")
      println(s"handlePostOutRecord-> ${myAccumulator.value.getOrElseUpdate("handlePostOutRecord",0)}")
      println(s"post ... end ")


      println(s"outputRecord=handleCrashOutRecord+handleGetOutRecord+handlePostOutRecord")
      println(s"outputRecord-> ${myAccumulator.value.getOrElseUpdate("outputRecord",0)}")
      println("##########myAccumulator end###############")
    }catch {
      case e:Exception=>{
        println(e.getMessage)
      }
        throw e
    }
  }

  /**
    * crach 、get、post处理
    * @param message
    * @return
    */
  def handleMessage(message:JSONObject)(implicit myAccumulator:MyAccumulator=new MyAccumulator):Seq[Option[JSONObject]]={
    myAccumulator.add("handleRecord")
    message.put("logSignFlag",0)
    val msgBody = message.getJSONObject("msgBody")
    /*val body = msgBody.getJSONObject("body")

    //针对crash日志处理
    if(body.containsKey("STACK_TRACE")){
      myAccumulator.add("handleCrashRecord")
      val stackTraceStr = body.getString("STACK_TRACE")
      val stackTraceMd5 = DigestUtils.md5Hex(stackTraceStr)
      body.put("STACK_TRACE_MD5",stackTraceMd5)
      return CrashProcess.handleCrash(message)(myAccumulator)
    }*/
    println("11111111111111")
    val method = msgBody.getString("svr_req_method")
    method match {
      case "POST" => {
        myAccumulator.add("handlePostRecord")
        PostProcess.handlePost(message)(myAccumulator)
      }
      case "GET" => {
        myAccumulator.add("handleGetRecord")
        GetProcess.handleGet(message)(myAccumulator)
      }
      case _ => {
        myAccumulator.add("handleExcRecord")
        Array(None)
      }
    }

  }
}
