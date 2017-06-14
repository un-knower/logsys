package cn.whaley.bi.logsys.log2parquet.utils

import scopt.OptionParser
/**
  * Created by michael on 2017/6/14.
  */
object ParamsParseUtil {
  private val default = Params()

  private val readFormat = DateFormatUtils.readFormat
  private val readFormatHour = DateFormatUtils.readFormatHour

  def parse(args: Seq[String], default: Params = default): Option[Params] = {
    if (args.nonEmpty) {
      val parser = new OptionParser[Params]("ParamsParse") {
        head("ParamsParse", "1.2")
        opt[String]("appID").action((x, c) => c.copy(appID = x))
         opt[String]("startDate").action((x, c) => c.copy(startDate = x)).
          validate(e => try {
            readFormat.parse(e)
            success
          } catch {
            case e: Exception => failure("wrong date format, should be 'yyyyMMdd'")
          })
        opt[String]("endDate").action((x, c) => c.copy(endDate = x)).
          validate(e => try {
            readFormat.parse(e)
            success
          } catch {
            case e: Exception => failure("wrong date format, should be 'yyyyMMdd'")
          })
        opt[String]("startHour").action((x, c) => c.copy(startHour = x)).
          validate(e => try {
            readFormatHour.parse(e)
            success
          } catch {
            case e: Exception => failure("wrong date format, should be 'HH'")
          })
        opt[String]("endHour").action((x, c) => c.copy(endHour = x)).
          validate(e => try {
            readFormatHour.parse(e)
            success
          } catch {
            case e: Exception => failure("wrong date format, should be 'HH'")
          })
      }
      parser.parse(args, default) match {
        case Some(p) => Some(p)
        case None => throw new RuntimeException("parse error")
      }
    } else {
      throw new RuntimeException("args is empty,at least need --startDate --startHour")
    }
  }
}
