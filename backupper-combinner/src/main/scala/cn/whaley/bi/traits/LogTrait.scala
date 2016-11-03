package cn.whaley.bi.traits

import org.slf4j.LoggerFactory

/**
 * Created by fj on 16/9/29.
 *
 * 日志公共特质
 */
trait LogTrait {
    val LOG = LoggerFactory.getLogger(this.getClass)
}
