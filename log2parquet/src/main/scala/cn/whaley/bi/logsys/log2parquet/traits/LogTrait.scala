package cn.whaley.bi.logsys.log2parquet.traits

import org.slf4j.LoggerFactory

/**
 * Created by michael on 2017/6/22.
 */
trait LogTrait {
    def LOG = LoggerFactory.getLogger(this.getClass)
}
