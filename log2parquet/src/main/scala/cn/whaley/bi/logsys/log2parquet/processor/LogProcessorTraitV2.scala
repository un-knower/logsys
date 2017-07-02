package cn.whaley.bi.logsys.log2parquet.processor

import cn.whaley.bi.logsys.log2parquet.ProcessResult
import cn.whaley.bi.logsys.log2parquet.traits.{NameTrait, InitialTrait}
import com.alibaba.fastjson.JSONObject

/**
  * Created by baozhiwang on 2017/6/23.
  */
trait LogProcessorTraitV2  extends InitialTrait with NameTrait with java.io.Serializable{

  /**
    * 解析日志消息体
    *
    * @return
    */
  def process(log: JSONObject): ProcessResult[JSONObject]
}