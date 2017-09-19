package cn.whaley.bi.logsys.batchforest

import java.text.SimpleDateFormat
import java.util.Date

import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}
import cn.whaley.bi.logsys.batchforest.util._
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


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
      val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

      val sparkContext = sparkSession.sparkContext
      val myAccumulator =  new MyAccumulator
      sparkContext.register(myAccumulator,"myAccumulator")
      //路径处理
      val inputPath = args(0)
      val key_day =args(1)
      val key_hour = args(2)
      val config = new Configuration()
      val fs = FileSystem.get(config)
      val fileStatus = fs.listStatus(new Path(s"$inputPath/key_day=${key_day}/key_hour=${key_hour}"))
      val paths = new ArrayBuffer[String]()
      fileStatus.foreach(f=>{
        val path = f.getPath.toString
        if(path.contains("boikgpokn78sb95k") && !path.contains("boikgpokn78sb95kbqei6cc98dc5mlsr")){
          paths.append(path)
        }
      })
      var inputRdd:RDD[String ] = sparkContext.textFile(paths(0))
      for(i<- 1 to paths.size -1){
        val rdd = sparkContext.textFile(paths(i))
        inputRdd = inputRdd.union(rdd)
      }
      //1.decode
      val decodeRdd = inputRdd.map(line=>{
        myAccumulator.add("inputRecord")
        MsgDecoder.decode(line)
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
          val message = JSON.parseObject(line)
          verificationFormat(message)
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
        log.put("datetime",datetime)
        log
      }).repartition(1000)

      val outputPath = s"/data_warehouse/ods_origin.db/log_origin/key_day=${key_day}/key_hour=${key_hour}"
      if(fs.exists(new Path(outputPath))){
        fs.delete(new Path(outputPath),true)
      }
      resultRdd.saveAsTextFile(outputPath)

//      Data2HdfsUtil.saveAsJson(resultRdd,key_day,key_hour)(myAccumulator)
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

  def handleTime(log:JSONObject): JSONObject ={
    val logTime = log.getLong("logTime")
    val time = if(log.getJSONObject("logBody").containsKey("happenTime")){
      val happenTime = try{
        log.getJSONObject("logBody").getLong("happenTime")
      }catch {
        case ex: Throwable => {
          null
        }
      }
      if(happenTime !=null && ((logTime - happenTime)<= 10 * 60 * 1000)){
        happenTime
      }else{
        logTime
      }
    }else{
      logTime
    }
    log.put("logTime",time)
    log
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
    val body = msgBody.getJSONObject("body")
    //针对crash日志处理
    if(body.containsKey("STACK_TRACE")){
      myAccumulator.add("handleCrashRecord")
      return handleCrash(message)(myAccumulator)
    }
    val method = msgBody.getString("svr_req_method")
      method match {
           case "POST" => {
             myAccumulator.add("handlePostRecord")
             handlePost(message)(myAccumulator)
           }
           case "GET" => {
             myAccumulator.add("handleGetRecord")
             handleGet(message)(myAccumulator)
           }
           case _ => {
             myAccumulator.add("handleExcRecord")
             Array(None)
           }
          }

  }

  /**
    * 解析归集层msgBody为日志消息体
    * 在原来的处理方式有logtime字段，
    * @param line
    * @return
    */
  def verificationFormat(line: JSONObject) ={
    if (line.getString("msgSource") == "ngx_log" && line.getString("msgFormat") == "json") {
       val msgBody = line.getJSONObject("msgBody")
       //msgBody为空返回None
       if(msgBody.keySet().size()==0 || !msgBody.isInstanceOf[JSONObject]){
          None
       }else{
         val body = msgBody.getJSONObject("body")
         if(body == null){
           None
         }else{
           Some(line)
         }
       }
     }else{
       None
     }
  }
  /**
    * crash日志处理
    * @param message
    * @return
    */
  def handleCrash(message:JSONObject)
                 (implicit myAccumulator:MyAccumulator=new MyAccumulator):Seq[Option[JSONObject]]={
    val msgBody = message.getJSONObject("msgBody")
    val appId = msgBody.getString("appId")
    val result = appId match {
        //whaley main crash 日志不做处理
      case "boikgpokn78sb95kjhfrendo8dc5mlsr" => {
        myAccumulator.add("handleCrashNoMd5Record")
        handleCrashFiled(message)(myAccumulator)
      }
      // medusa main3.x crash md5校验
      case "boikgpokn78sb95ktmsc1bnkechpgj9l" => {
        myAccumulator.add("handleCrashMd5Record")
        handleMedusaCrash(message)(myAccumulator)
      }
      case _ => {
        myAccumulator.add("handleCrashExcRecord")
        None
      }
    }
    Array(result)
  }
  def  handleCrashFiled(message:JSONObject)
                       (implicit myAccumulator:MyAccumulator=new MyAccumulator()):Option[JSONObject]={
    val msgBody = message.getJSONObject("msgBody")
      message.remove("msgBody")
    val body = msgBody.getJSONObject("body")
    val logTime = msgBody.getLong("svr_receive_time")
    msgBody.remove("body")
    val msgId = message.getString("msgId")
    val logId =msgId +StringUtil.fixLeftLen(Integer.toHexString(0),'0',4)
    msgBody.asInstanceOf[java.util.Map[String,Object]].putAll(body)
    message.put("logBody",msgBody)
    message.put("logId",logId)
    message.put("logTime",logTime)
    translateProp(message.getJSONObject("logBody"),"appId",message,"appId")
    translateProp(message.getJSONObject("logBody"),"logVersion",message,"logVersion")
    translateProp(message.getJSONObject("logBody"),"logSignFlag",message,"logSignFlag")
    myAccumulator.add("handleCrashOutRecord")
    return Some(message)
  }


  /**
    * 校验Medusa main3.x Crash md5
    * @param message
    * @return
    */
  def handleMedusaCrash(message:JSONObject)
                       (implicit myAccumulator:MyAccumulator=new MyAccumulator):Option[JSONObject]={
    val body = message.getJSONObject("msgBody").getJSONObject("body")
    val CRASH_KEY = "p&i#Hl5!gAo*#dwSa9sfluynvaoOKD3"
    val crashKey = body.getString("CRASH_KEY")
    val createDate = body.getString("USER_CRASH_DATE")
    val mac = body.getString("MAC")
    val productCode = body.getString("PRODUCT_CODE")
    val md5 = body.getString("MD5")
    val verificationStr = CRASH_KEY+crashKey+createDate+mac+productCode
    val flag = (md5 == DigestUtil.getMD5Str32(verificationStr))
    if(!flag){
      myAccumulator.add("handleCrashMd5ErrRecord")
      return None
    }
    //校验通过为状态为1
    message.put("logSignFlag",1)
    return handleCrashFiled(message)(myAccumulator)
  }

  /**
    * get 请求处理
    * get 日志无需平展化只有一条日志
    * 1.添加logId，msgSignFlag=0 ，
    * 2.需改msgBody中svr_req_url 值
    * 3.msgBody中的内容和queryObj 放入 logBody中
    * 4.svr_receive_time 转化为logTime
    * @param message
    */
  def handleGet(message:JSONObject)
               (implicit myAccumulator:MyAccumulator=new MyAccumulator):Seq[Option[JSONObject]]={
    val msgBody = message.getJSONObject("msgBody")
    msgBody.remove("body")
    message.remove("msgBody")
    val url = msgBody.getString("svr_req_url")
    //解析queryString
      val httpUrl = URLParser.parseHttpURL(url,true)
      val queryObj = URLParser.parseHttpQueryString(httpUrl.queryString)
      if(queryObj.isEmpty){
        myAccumulator.add("handleGetExcRecord")
        return Array(None)
      }
      //移除冗余的url参数
    msgBody.put("svr_req_url",httpUrl.location)
    message.put("logBody",queryObj)
    message.getJSONObject("logBody").asInstanceOf[java.util.Map[String, Object]].putAll(msgBody)
    //获取logId
    val msgId = message.getString("msgId")
    val logId =msgId +StringUtil.fixLeftLen(Integer.toHexString(0),'0',4)
    message.put("logId",logId)
    val logTime = msgBody.getLong("svr_receive_time")
    message.put("logTime",logTime)
    translateProp(message.getJSONObject("logBody"),"appId",message,"appId")
    translateProp(message.getJSONObject("logBody"),"logVersion",message,"logVersion")
    translateProp(message.getJSONObject("logBody"),"logSignFlag",message,"logSignFlag")
    myAccumulator.add("handleGetOutRecord")
   Array(Some(message))
  }


  /**
    * get 请求处理
    * @param message
    */
  def handleGet2(message:JSONObject):Seq[Option[JSONObject]]={
    val msgBody = message.getJSONObject("msgBody")
    val url = msgBody.getString("svr_req_url")
    //解析queryString
    val httpUrl = URLParser.parseHttpURL(url,true)
    val queryObj = URLParser.parseHttpQueryString(httpUrl.queryString)
    if(queryObj.isEmpty){
      return Array(None)
    }
    //移除冗余的url参数
    msgBody.put("svr_req_url",httpUrl.location)
    message.put("logSignFlag",0)
    if(message.containsKey("logBody")){
      message.getJSONObject("logBody").asInstanceOf[java.util.Map[String, Object]].putAll(queryObj)
    }else{
      message.put("logBody",queryObj)
    }
    //平展化
    //1.msgBody平展化
    val normalizeMsgBody = bodyNormalize(msgBody)
    message.remove("msgBody")
    val msgId = message.getString("msgId")
    val size = normalizeMsgBody.size
    //平展化msgBody内容到logBody中，同时生成logId
    for(i<-0 to size -1) yield {
      val entity = JSON.parseObject(message.toJSONString)
      val logId =msgId +StringUtil.fixLeftLen(Integer.toHexString(i),'0',4)
      entity.put("logId",logId)
      val logBody = normalizeMsgBody(i)
      if(entity.containsKey("logBody")){
        entity.getJSONObject("logBody").asInstanceOf[java.util.Map[String, Object]].putAll(logBody)
      }else{
        entity.put("logBody",logBody)
      }
      //提升公共字段
      translateProp(entity.getJSONObject("logBody"),"appId",entity,"appId")
      translateProp(entity.getJSONObject("logBody"),"logVersion",entity,"logVersion")
      translateProp(entity.getJSONObject("logBody"),"logSignFlag",entity,"logSignFlag")
      translateProp(entity.getJSONObject("logBody"),"logTime",entity,"logTime")
      Some(entity)
    }
  }

  /**
    * body 平展化,若body为数组展开为多条记录
    * @param msgBody
    * @return
    */
  def bodyNormalize(msgBody:JSONObject): Seq[JSONObject] ={
    val body = msgBody.get("body")
    if(body == null){
      return Array(msgBody)
    }
    if( body.asInstanceOf[JSONObject].isEmpty){
      msgBody.remove("body")
      return Array(msgBody)
    }
    println("msgBody : "+msgBody)
    println("body : "+body)
    if(body.isInstanceOf[JSONObject]){
      msgBody.asInstanceOf[java.util.Map[String,Object]].putAll(body.asInstanceOf[JSONObject])
      msgBody.remove("body")
      Array(msgBody)
    }else if(body.isInstanceOf[JSONArray]){
      val array = body.asInstanceOf[JSONArray]
      msgBody.remove("body")
      for(i<- 0 to array.size() -1) yield {
        val item = array.get(i).asInstanceOf[JSONObject]
        item.asInstanceOf[java.util.Map[String,Object]].putAll(msgBody)
        item
      }
    }else{
      Array(msgBody)
    }
  }


  /**
    * 转移并展开属性值
    * 如果fromProp为空,则不进任何处理,直接返回to
    * 如果fromProp为JSONObject,如果toKey为空,则展开fromProp到to,否则展开到toProp
    * 如果fromProp为非JSONObject,则要求toKey不能为空,且将toProp设置为fromProp
    * @param from
    * @param fromKey
    * @param to
    * @param toKey
    */
  def translateProp(from: JSONObject, fromKey: String, to: JSONObject, toKey: String): JSONObject ={
    val fromProp = from.get(fromKey)
//    from.remove(fromKey)

    //如果fromProp为空，则不做任何处理
    if(fromProp == null || (fromProp.isInstanceOf[String] && fromProp.asInstanceOf[String].isEmpty)){
      return to
    }
    if (fromProp.isInstanceOf[JSONObject]) {
      if (toKey != null && !toKey.isEmpty) {
        val toProp = if (to.get(toKey) != null && to.get(toKey).isInstanceOf[JSONObject]) {
          to.getJSONObject(toKey)
        } else {
          new JSONObject()
        }
        toProp.asInstanceOf[java.util.Map[String, Object]].putAll(fromProp.asInstanceOf[JSONObject])
        to.put(toKey, toProp)
      }else{
        to.asInstanceOf[java.util.Map[String, Object]].putAll(fromProp.asInstanceOf[JSONObject])
      }

    }else{
      assert(toKey != null && !toKey.isEmpty)
      to.put(toKey, fromProp)
    }
    to
  }

  /**
    * //post 解析
    * @param message
    * @return
    */
  def handlePost(message:JSONObject)
                (implicit myAccumulator:MyAccumulator=new MyAccumulator): Seq[Option[JSONObject]] ={
    val msgBody = message.getJSONObject("msgBody")
    val logTime = msgBody.getLong("svr_receive_time")
    message.put("logTime",logTime)
    val msgId = message.getString("msgId")
    if(!msgBody.getString("svr_req_method").equalsIgnoreCase("POST")){
      myAccumulator.add("handlePostExc")
      return Array(None)
    }
    //消息体验证,方法内部将更新相关签名字段的值
    val flag= verify(message)(myAccumulator)
    if(!flag){
      return Array(None)
    }
    message.remove("msgBody")
    val body = msgBody.getJSONObject("body")
    msgBody.remove("body")
    //baseInfo展开
    val baseInfo = if(body.get("baseInfo").isInstanceOf[String]){
          parseObject(body.get("baseInfo").asInstanceOf[String])
        }else{
          Some(body.get("baseInfo").asInstanceOf[JSONObject])
        }
      if(!baseInfo.isEmpty && baseInfo.get != null  ){
        //baseInfo 合并到body
        body.remove("baseInfo")
        body.asInstanceOf[java.util.Map[String,Object]].putAll(baseInfo.get)
      }
    //logs展开
    val logs =
      if (body.get("logs").isInstanceOf[String]) {
        parseArray(body.get("logs").asInstanceOf[String])
      } else {
          Some(body.get("logs").asInstanceOf[JSONArray])
      }
      body.remove("logs")
      if(!logs.isEmpty && logs.get != null){
        //logs 非空{}
        val size = logs.get.size()
        for(i<-0 to size -1) yield {
          //将body中的属性合并到log中
          val log = logs.get.getJSONObject(i)
          log.asInstanceOf[java.util.Map[String,Object]].putAll(body)
          //将log中的属性合并到msgBody中,一定要创建一个新的logBody而不能采用引用
          val logBody = JSON.parseObject(msgBody.toJSONString)
          logBody.asInstanceOf[java.util.Map[String,Object]].putAll(log)
          val entity = JSON.parseObject(message.toJSONString)
          val logId =msgId +StringUtil.fixLeftLen(Integer.toHexString(i),'0',4)
          entity.put("logId",logId)
          entity.put("logBody",logBody)
          //提升字段
          translateProp(entity.getJSONObject("logBody"),"appId",entity,"appId")
          translateProp(entity.getJSONObject("logBody"),"logVersion",entity,"logVersion")
          translateProp(entity.getJSONObject("logBody"),"logSignFlag",entity,"logSignFlag")
          translateProp(entity.getJSONObject("logBody"),"logTime",entity,"logTime")
          myAccumulator.add("handlePostOutRecord")
          Some(entity)
        }
        //处理展开后的日志
      }else{
        myAccumulator.add("handlePostExc")
        return Array(None)
      }
  }

  /**
    * 验证消息体格式
    * md5验证
    * 1.判断是否需要校验（version=="01" && msgSignFlag==0 需要校验）
    * 2.需要校验的时候设置logSignFlag=1 ，不需要校验设置为logSignFlag=0
    * 3.校验成功msgSignFlag=1 失败msgSignFlag=-1 一般可以不处理，丢弃
    * @param message
    */
  def verify(message:JSONObject)
            (implicit myAccumulator:MyAccumulator=new MyAccumulator): Boolean ={
    myAccumulator.add("handlePostVerifyRecord")
    val postObj = message.getJSONObject("msgBody")
    val bodyObj = postObj.getJSONObject("body")
    val version = bodyObj.getString("version")
    val msgSignFlag = message.getIntValue("msgSignFlag")
    //对于01版本的消息体，执行消息体签名验证;
    // msgBody中的msgSignFlag属性如果未0，则指明了签名验证未在接入层完成
    val key = version match {
      case "01" => "92DOV+sOk160j=430+DM!ZzESf@XkEsn#cKanpB$KFB6%D8z4C^xg7cs6&7wn0A4A*iR9M3j)pLs]ll5E9aFlU(dE0[QKxHZzC.CaO/2Ym3|Tk<YyGZR>WuRUmI?x2s:Cg;YEA-hZubmGnWgXE"
      case "02" => "92DOV+sOk160j=430+DM!ZzESf@XkEsn#cKanpB$KFB6%D8z4C^xg7cs6&7wn0A4A*iR9M3j)pLs]ll5E9aFlU(dE0[QKxHZzC.CaO/2Ym3|Tk<YyGZR>WuRUmI?x2s:Cg;YEA-hZubmGnWgXE"
      case _ => ""
    }
    //需要校验  msgSignFlag==0 未校验
    val needSign = (version=="01" && msgSignFlag==0)
    val signed = if(needSign){
      //校验
      message.put("logSignFlag",1)
      val baseInfo = bodyObj.get("baseInfo")
      val logs = bodyObj.get("logs")
      val ts = bodyObj.getString("ts")
      val md5 = bodyObj.getString("md5")
      val hasSignInfo = (baseInfo!=null && logs!=null && ts!=null && md5 != null)
      if(!hasSignInfo || key == ""){
        myAccumulator.add("handlePostVerifyMd5ErrRecord")
        false
      }else{
        val baseInfo = bodyObj.get("baseInfo").asInstanceOf[String]
        val logs = bodyObj.get("logs").asInstanceOf[String]
        val ts = bodyObj.getString("ts")
        val md5 = bodyObj.getString("md5")
        val str = ts + baseInfo + logs + key
        val signMD5 = DigestUtil.getMD5Str32(str)
        if(signMD5 == md5){
          myAccumulator.add("handlePostVerifyMd5SucessRecord")
          true
        }else{
          myAccumulator.add("handlePostVerifyMd5ErrRecord")
          false
        }
//        signMD5 == md5
      }
    }else{
      myAccumulator.add("handlePostVerifyNoMd5Record")
      //不需要校验
      message.put("logSignFlag",0)
      true
    }
    return signed
  }


  /**
    * 解析字符串为JSONObject对象
    * @param str
    * @return ，如果失败返回None
    */
  private def parseObject(str: String): Option[JSONObject] = {
    try {
      Some(JSON.parseObject(str))
    } catch {
      case e: Throwable => {
        LOG.error("", e)
        None
      }
    }
  }

  /**
    * 解析字符串为JSONArray对象
    * @param str
    * @return 如果失败返回None
    */
  private def parseArray(str: String): Option[JSONArray] = {
    try {
      Some(JSON.parseArray(str))
    } catch {
      case e: Throwable => {
        LOG.debug("invalid jsonarray str:" + str, e)
        None
      }
    }
  }


}
