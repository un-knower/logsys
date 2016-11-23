package cn.whaley.bi.logsys.forest

import cn.whaley.bi.logsys.forest.entity.LogEntity


/**
 * Created by fj on 16/11/19.
 *
 * 将日志消息topic名映射trait
 */
trait TopicMapperTrait {
    def getTopic(sourceTopic: String, logEntity: LogEntity): ProcessResult[Seq[String]]
}
