package cn.whaley.bi.logsys.batchforest.util

import cn.whaley.bi.logsys.batchforest.StringDecoder
import cn.whaley.bi.logsys.batchforest.traits.{LogTrait, NameTrait}

/**
  * Created by guohao on 2017/8/28.
  */
object MsgDecoder extends NameTrait with LogTrait{

  val decoder = new StringDecoder()
  /**
    * 消息解码，从字节数组解析为归集层消息体格式
    * @return
    */
  def decode(str: String) = {
    try {

      Some(decoder.decodeToString(str))
    } catch {
      case e: Throwable => {
        LOG.error(s"解码异常"+e.getMessage)
        None
      }
    }

  }

}
