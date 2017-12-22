package cn.whaley.bi.logsys.batchforest.process

import cn.whaley.bi.logsys.batchforest.process.LogFormat.translateProp
import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}
import cn.whaley.bi.logsys.batchforest.util.{MyAccumulator, StringUtil, URLParser}
import com.alibaba.fastjson.JSONObject

/**
  * Created by guohao on 2017/9/19.
  */
object GetProcess extends NameTrait with LogTrait{
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
    val httpUrl = URLParser.parseHttpURL(url,false)
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

}
