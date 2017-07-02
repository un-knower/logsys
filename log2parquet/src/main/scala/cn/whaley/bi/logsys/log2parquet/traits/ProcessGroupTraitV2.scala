package cn.whaley.bi.logsys.log2parquet.traits

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.ProcessResult
import com.alibaba.fastjson.JSONObject

/**
  * Created by michael on 2017/6/22.
  */
trait ProcessGroupTraitV2 extends InitialTrait with LogTrait with NameTrait with java.io.Serializable{
    def init(confManager: ConfManager): Unit
    def process(log:JSONObject): ProcessResult[JSONObject]
}
