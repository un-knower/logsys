package cn.whaley.bi.logsys.forest.Traits

import org.slf4j.LoggerFactory

/**
 * Created by fj on 16/11/9.
 */
trait LogTrait {
    def LOG = LoggerFactory.getLogger(this.getClass)
}
