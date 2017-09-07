package cn.whaley.bi.logsys.batchforest.traits

import org.slf4j.LoggerFactory

/**
  * Created by guohao on 2017/8/28.
  */
trait LogTrait {
  def LOG = LoggerFactory.getLogger(this.getClass)
}
