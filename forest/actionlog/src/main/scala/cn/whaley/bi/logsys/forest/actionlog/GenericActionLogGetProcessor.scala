package cn.whaley.bi.logsys.forest.actionlog


import cn.whaley.bi.logsys.common.{ConfManager}
import cn.whaley.bi.logsys.forest.Traits.LogTrait
import cn.whaley.bi.logsys.forest.actionlog.medusa20.{LogPreProcess}
import cn.whaley.bi.logsys.forest.entity.LogEntity
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
        try {
            //跳过非GET方法提交的数据
            if (actionLogEntity.method != "GET") {
                return ProcessResult(this.name, ProcessResultCode.skipped, "", None)
            }

            //解析queryString
            val url = actionLogEntity.url
            val httpUrl = URLParser.parseHttpURL(url, true)
            val queryObj = URLParser.parseHttpQueryString(httpUrl.queryString)
            val logBody =
                if (actionLogEntity.appId == Constants.APPID_MEDUSA_2_0) {
                    //medusa2.0需要解析log查询参数为日志消息体
                    //由于复用历史代码，需要附带上时间数据
                    val receiveTime = log.getLong("receiveTime")
                    parseMedusa20Log(receiveTime + "-" + queryObj.getString("log"))
                } else {
                    queryObj
                }

            //平展化日志消息体
            actionLogEntity.updateUrl(httpUrl.location)
            logBody.asInstanceOf[java.util.Map[String, Object]].putAll(actionLogEntity)

            ProcessResult(this.name, ProcessResultCode.processed, "", Some(new ActionLogGetEntity(logBody) :: Nil))

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
