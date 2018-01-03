package cn.whaley.bi.fresh.util

import com.alibaba.fastjson.{JSON, JSONObject}
import scalaj.http.{Http, HttpOptions}
/**
  * Created by guohao on 2017/11/7.
  */
class PhoenixUtil(metadataService:String="http://odsviewmd.whaleybigdata.com",readTimeOut:Int=900000) {

  def getJSONString[T](entities: Seq[T]): String = {
    "[" + entities.map(entity => JSON.toJSONString(entity, false)).mkString(",") + "]"
  }



  /**
    *
    * @param taskId
    * @param deleteOld drop partition 是否执行 true 执行
    * @param isDebug
    * @return
    */
  def postTaskId2MetaModel(taskId: String,deleteOld:String,isDebug:Boolean=false): JSONObject = {
//    assert(taskFlag!=null&&taskFlag.length==3)
//    var reallyTaskFlag=""
//    if(isDebug){
//      reallyTaskFlag=taskFlag.substring(0,2)+"0"
//    }else{
//      reallyTaskFlag=taskFlag
//    }
    println(metadataService + s"/metadata/processTask/${taskId}/${deleteOld}")
    val response = Http(metadataService + s"/metadata/processTask/${taskId}/${deleteOld}")
      .option(HttpOptions.readTimeout(readTimeOut))
      .method("POST")
      .header("Content-Type","text/plain")
      .postData("")
      .asString
    if (!response.isSuccess) {
      throw new RuntimeException(response.body)
    }
    JSON.parseObject(response.body)
  }
}

object PhoenixUtil{

}
