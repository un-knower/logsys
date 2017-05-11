package cn.whaley.bi.logsys.forest.actionlog


import cn.whaley.bi.logsys.common.{ConfManager}
import cn.whaley.bi.logsys.forest.Traits.LogTrait
import cn.whaley.bi.logsys.forest.actionlog.medusa20.{LogPreProcess}
import cn.whaley.bi.logsys.forest.entity.{MsgEntity, LogEntity}
import cn.whaley.bi.logsys.forest.processor.LogProcessorTrait
import cn.whaley.bi.logsys.forest._
import com.alibaba.fastjson.{JSONObject}


/**
 * Created by fj on 16/11/9.
 *
 * 通用的行为日志处理器
 * 1. 在归集层没有做签名验证的情况下，完成消息体签名验证
 * 2. 解析baseInfo属性为一个JSONObject，将其属性合并到父对象中
 * 3. 解析logs属性为一个JSON数组，其中每个元素合并父对象属性，并产生一个logId，形成一个ActionLogEntity对象
 *
 */
class GenericActionLogGetProcessor extends LogProcessorTrait with LogTrait {


    /**
     * 初始化
     */
    def init(confManager: ConfManager): Unit = {

    }


    /**
     * 解析日志消息体
     * @return
     */
    def process(log: LogEntity): ProcessResult[Seq[LogEntity]] = {
        val actionLogEntity = new ActionLogGetEntity(log)
        if (actionLogEntity.msgBodyObj == null) {
            return ProcessResult(this.name, ProcessResultCode.skipped, "msgBodyObj is null", None)
        }
        if (actionLogEntity.msgBodyObj.method == null) {
            return ProcessResult(this.name, ProcessResultCode.skipped, "msgBodyObj.method is null", None)
        }
        try {
            //跳过非GET方法提交的数据
            if (!actionLogEntity.msgBodyObj.method.equalsIgnoreCase("GET")) {
                return ProcessResult(this.name, ProcessResultCode.skipped, "", None)
            }

            //解析queryString
            val httpUrl = URLParser.parseHttpURL(actionLogEntity.msgBodyObj.url, true)
            val queryObj = URLParser.parseHttpQueryString(httpUrl.queryString)
            //移除冗余的url参数
            actionLogEntity.msgBodyObj.setUrl(httpUrl.location)
            actionLogEntity.updateLogSignFlag(LogEntity.VAL_SIGN_NO)
            actionLogEntity.updateLogBody(queryObj)
            /** ****** 不再处理medusa2.0的get日志, 处理放到数仓层完成 ****************************/
            /*
            val logBody =
                if (actionLogEntity.appId == Constants.APPID_MEDUSA_2_0) {
                    //medusa2.0需要解析log查询参数为日志消息体
                    //由于复用历史代码，需要附带上时间数据
                    val receiveTime = actionLogEntity.receiveTime
                    parseMedusa20Log(receiveTime + "-" + queryObj.getString("log"))
                } else {
                    queryObj
                }
                actionLogEntity.updateLogBody(logBody)
                */
            val ret = Some(actionLogEntity.normalize())
            ProcessResult(this.name, ProcessResultCode.processed, "", ret)
        } catch {
            case e: Throwable => {
                ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
            }
        }

    }

    /**
     * 解析medusa2.0日志
     * @param logStr
     * @return
     */
    private def parseMedusa20Log(logStr: String): JSONObject = {
        val logType = GenericActionLogGetProcessor.logTypeRegex.findFirstMatchIn(logStr).get.group(1)
        val logObj = LogPreProcess.matchLog(logType, logStr)
        logObj.toJSONObject()
    }
}

object GenericActionLogGetProcessor {
    val logTypeRegex = "\\d+-(\\w{1,30})-".r

}
