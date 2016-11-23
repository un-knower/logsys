package cn.whaley.bi.logsys.forest.TopicMapper

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.Traits.{NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest._
import cn.whaley.bi.logsys.forest.entity.LogEntity

/**
 * Created by fj on 16/11/19.
 *
 * 默认目标topic映射器
 */
class DefaultTargetTopicMapper extends TopicMapperTrait with InitialTrait with NameTrait {

    override def getTopic(sourceTopic: String, logEntity: LogEntity): ProcessResult[Seq[String]] = {
        val targetTopic =
            if (logEntity != null) {
                s"${prefix}-${logEntity.appId}"
            } else {
                if (sourceTopic.startsWith("pre-")) {
                    s"${prefix}-${sourceTopic.substring("pre-".length)}"
                } else {
                    s"${prefix}-${sourceTopic}"
                }
            }
        new ProcessResult(this.name, ProcessResultCode.processed, "", Some(Array(targetTopic)))
    }

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        prefix = confManager.getConfOrElseValue(this.name, "prefix", "post")
    }

    var prefix: String = null

}
