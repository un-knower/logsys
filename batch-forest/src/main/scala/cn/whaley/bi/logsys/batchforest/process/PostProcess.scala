package cn.whaley.bi.logsys.batchforest.process

import cn.whaley.bi.logsys.batchforest.process.LogFormat.translateProp
import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}
import cn.whaley.bi.logsys.batchforest.util.{DigestUtil, JsonUtil, MyAccumulator, StringUtil}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  * Created by guohao on 2017/9/19.
  */
object PostProcess extends NameTrait with LogTrait{
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
    //处理eagle日志
    val appId = msgBody.getString("appId")
    if("boikgpokn78sb95k7id7n8eb8dc5mlsr".equalsIgnoreCase(appId)){
      myAccumulator.add("handleEagleRecord")
      return EagleProcess.handleMessage(message)(myAccumulator)
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
      JsonUtil.parseObject(body.get("baseInfo").asInstanceOf[String])
    }else{
      Some(body.get("baseInfo").asInstanceOf[JSONObject])
    }
    if(!baseInfo.isEmpty && baseInfo.get != null  ){
      //移除happenTime
      baseInfo.get.remove("happenTime")
      //baseInfo 合并到body
      body.remove("baseInfo")
      body.asInstanceOf[java.util.Map[String,Object]].putAll(baseInfo.get)
    }
    //logs展开
    val logs =
      if (body.get("logs").isInstanceOf[String]) {
        JsonUtil.parseArray(body.get("logs").asInstanceOf[String])
      } else {
        Some(body.get("logs").asInstanceOf[JSONArray])
      }

    body.remove("logs")
    if(!logs.isEmpty  && logs.get != null  && logs.get.size() !=0){
      //logs 非空{}
      val size = logs.get.size()
      for(i<-0 to size -1) yield {
        //将body中的属性合并到log中
        try{
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
        }catch {
          case e:Exception=>{
            LOG.info(s"logs ...... ${logs.get} ")
            LOG.error(e.getMessage)
            myAccumulator.add("handlePostExc")
            return Array(None)
          }
        }
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

}
