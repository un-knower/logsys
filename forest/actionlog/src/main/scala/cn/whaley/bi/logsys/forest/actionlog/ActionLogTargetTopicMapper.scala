package cn.whaley.bi.logsys.forest.actionlog

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.Traits.{NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest._
import cn.whaley.bi.logsys.forest.entity.LogEntity

/**
 * Created by fj on 16/11/19.
 *
 * 默认目标topic映射器
 */
class ActionLogTargetTopicMapper extends TopicMapperTrait with InitialTrait with NameTrait {

    override def getTopic(sourceTopic: String, logEntity: LogEntity): ProcessResult[Seq[String]] = {
        if (logEntity != null && logEntity.containsKey("contentType")) {
            val contentType = logEntity.getString("contentType")
            if (contentType == "mv") {
                val targetTopic = s"${prefix}-${logEntity.appId}-mv"
                new ProcessResult(this.name, ProcessResultCode.processed, "", Some(Array(targetTopic)))
            } else {
                new ProcessResult(this.name, ProcessResultCode.skipped)
            }
        } else {
            new ProcessResult(this.name, ProcessResultCode.skipped)
        }
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
