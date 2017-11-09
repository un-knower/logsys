package cn.whaley.bi.logsys.metadataManage.util

import cn.whaley.bi.logsys.metadataManage.common.{ParamKey, Params}
import scopt.OptionParser

/**
  * Created by guohao on 2017/11/6.
  */
object ParamsParseUtil {
  private val default = Params()
  def parse(args:Seq[String],default:Params = default):Option[Params]={
    if (args.nonEmpty) {
      val parser = new OptionParser[Params]("ParamsParse"){
        head("ParamsParse", "1.x")
        opt[Map[String, String]]("paramMap").valueName("k1=v1,k2=v2...").action((x, c) => c.copy(paramMap = x)).
          text("param Map[String,String]")
        opt[String](ParamKey.PATH).action((x, c) => c.copy(path = x))
        opt[String](ParamKey.DB_NAME).action((x, c) => c.copy(db_name = x))
        opt[String](ParamKey.TAB_PREFIX).action((x, c) => c.copy(tab_prefix = x))
        opt[String](ParamKey.PRODUCT_CODE).action((x, c) => c.copy(product_code = x))
        opt[String](ParamKey.APP_CODE).action((x, c) => c.copy(app_code = x))
        opt[String](ParamKey.REALLOGTYPE).action((x, c) => c.copy(realLogType = x))
        opt[String](ParamKey.KEY_DAY).action((x, c) => c.copy(key_day = x))
        opt[String](ParamKey.KEY_HOUR).action((x, c) => c.copy(key_hour = x))
      }
      parser.parse(args,default) match {
        case Some(p) => Some(p)
        case None =>  throw new RuntimeException("parse error ")
      }
    }else{
      throw new RuntimeException("args is empty ,please input args ...")
    }
  }
}
