package cn.whaley.bi.logsys.batchforest.process

import cn.whaley.bi.logsys.batchforest.process.LogFormat.translateProp
import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}
import cn.whaley.bi.logsys.batchforest.util.{DigestUtil, MyAccumulator, StringUtil}
import com.alibaba.fastjson.JSONObject

/**
  * Created by guohao on 2017/9/19.
  */
object CrashProcess extends NameTrait with LogTrait{
  /**
    * crash日志处理
    * @param message
    * @return
    */
  def handleCrash(message:JSONObject)
                 (implicit myAccumulator:MyAccumulator=new MyAccumulator):Seq[Option[JSONObject]]={
    val msgBody = message.getJSONObject("msgBody")
    msgBody.put("logType","crashlog")
    val appId = msgBody.getString("appId")
    val result = appId match {
      //whaley main crash 日志不做处理
      case "boikgpokn78sb95kjhfrendo8dc5mlsr" => {
        myAccumulator.add("handleCrashNoMd5Record")
        handleCrashField(message)(myAccumulator)
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

  def  handleCrashField(message:JSONObject)
                       (implicit myAccumulator:MyAccumulator=new MyAccumulator()):Option[JSONObject]={
    val msgBody = message.getJSONObject("msgBody")
    message.remove("msgBody")
    val body = msgBody.getJSONObject("body")
    msgBody.remove("body")
    val msgId = message.getString("msgId")
    val logId =msgId +StringUtil.fixLeftLen(Integer.toHexString(0),'0',4)
    msgBody.asInstanceOf[java.util.Map[String,Object]].putAll(body)
    message.put("logBody",msgBody)
    message.put("logId",logId)
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
    return handleCrashField(message)(myAccumulator)
  }

}
