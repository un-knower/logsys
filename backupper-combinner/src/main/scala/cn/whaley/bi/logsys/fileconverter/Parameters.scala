package cn.whaley.bi.logsys.fileconverter

import java.util.{Properties, Date}

/**
 * Created by fj on 16/10/2.
 *
 * 统计相关参数
 * @param startDate 统计起始日期
 * @param endDate   统计结束日期
 * @param confs     自定义参数
 */
case class Parameters(  startDate: Date, endDate: Date, confs: Properties) {
    override def toString(): String = {
        s"""
           |startDate:${startDate}
              |endDate:${endDate}
              |confs:${confs}
             """.stripMargin
    }
}
