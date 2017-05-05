package cn.whaley.bi.logsys.forest.actionlog


import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.{ProcessResultCode, ProcessResult}
import cn.whaley.bi.logsys.forest.entity.{LogEntity, MsgEntity}
import cn.whaley.bi.logsys.forest.processor.MsgProcessorTrait
import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * Created by fj on 16/11/10.
 *
 * Nginx日志JSON格式处理器
 */
class NgxLogJSONMsgProcessor extends MsgProcessorTrait {
    /**
     * 解析归集层msgBody为日志消息体,对于不支持的格式体，Code应该返回skipped
     * @return
     */
    override def process(msgEntity: MsgEntity): ProcessResult[Seq[LogEntity]] = {

        if (msgEntity.msgSource == "ngx_log" && msgEntity.msgFormat == "json") {
            val msgBody = msgEntity.msgBody
            var msgBodyObj: Option[JSONObject] = None
            if (msgBody.isInstanceOf[String]) {
                msgBodyObj = Some(JSON.parseObject(msgBody.asInstanceOf[String]))
            } else if (msgBody.isInstanceOf[JSONObject]) {
                msgBodyObj = Some(msgBody.asInstanceOf[JSONObject])
            }
            if (msgBodyObj.isEmpty) {
                new ProcessResult[Seq[LogEntity]](this.name, ProcessResultCode.formatFailure, "invalid format:" + msgBody.getClass.getName, None)
            } else {
                val logEntity = new LogEntity(msgEntity)
                logEntity.updateLogId(msgEntity.msgId)
                new ProcessResult(this.name, ProcessResultCode.processed, "", Some(Array(logEntity)))
            }
        } else {
            new ProcessResult[Seq[LogEntity]](this.name, ProcessResultCode.skipped, "", None)
        }
    }

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {

    }

    //private def quotObjRegex = "\"(.*?)\":\\s?\"(\\{.*?\\})\"".r

    private def quotObjRegex = "\"(\\w+)\":\\s?\"(\\{.*?\\})\"".r

    private def quotArrayRegex = "\"(\\w+)\":\\s?\"(\\[?\\{.*?\\}\\]?)\"".r

}
