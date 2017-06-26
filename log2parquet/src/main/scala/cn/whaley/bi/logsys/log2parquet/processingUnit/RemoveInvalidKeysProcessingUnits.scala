package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTraitV2
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import cn.whaley.bi.logsys.log2parquet.{ProcessResult, ProcessResultCode}
import com.alibaba.fastjson.JSONObject


/**
  * Created by michael on 2017/6/26.
  *
  *去除不合法的字段或将不合法字段替换为合法字段
  */
class RemoveInvalidKeysProcessingUnits extends LogProcessorTraitV2 with LogTrait {
  private val longTypeKeyList = List(LogKeys.ACCOUNT_ID, LogKeys.DURATION, LogKeys.SPEED, LogKeys.SIZE, LogKeys.PRE_MEMORY, LogKeys.POST_MEMORY)

  /**
    * 初始化
    */
  def init(confManager: ConfManager): Unit = {

  }

  /**
    * 判断字段名(Key)是否合法，同时对中划线做处理
    * @param key
    * @return
    */
  def isValidKey(product:String,logType:String,key:String):Boolean = {
   /* if(key != null && key.nonEmpty) {
      if(!keyWhiteList.contains(key)){
        regexKey findFirstIn key match {
          case Some(_) => !FieldNameListUtil.isBlack(product,logType,key)
          case None => false
        }
      }else true
    }else false*/
    false
  }

  /**
    * 解析出realLogType，并校验realLogType是否有效
    *
    * @return
    */
  def process(jsonObject: JSONObject): ProcessResult[JSONObject] = {
    try {
      val realLogType=jsonObject.getString(LogKeys.LOG_BODY_REAL_LOG_TYPE)
      val it=jsonObject.keySet().iterator()
      while (it.hasNext){
        val key=it.next()
        //TODO
        if(!isValidKey("appId",realLogType,key)) {
          jsonObject.remove(key)
          false
        }else {
          if(key.contains('-')){
            val validKey = key.replace('-','_')
            val value = jsonObject.get(key)
            jsonObject.put(validKey,value)
            jsonObject.remove(key)
          }
          true
        }
      }
      new ProcessResult(this.name, ProcessResultCode.processed, "", Some(jsonObject))
    } catch {
      case e: Throwable => {
        ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
      }
    }
  }
}


