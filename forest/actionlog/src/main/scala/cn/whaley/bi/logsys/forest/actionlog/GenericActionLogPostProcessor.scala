package cn.whaley.bi.logsys.forest.actionlog


import cn.whaley.bi.logsys.common.{ConfManager, DigestUtil}
import cn.whaley.bi.logsys.forest.Traits.LogTrait
import cn.whaley.bi.logsys.forest.entity.LogEntity
import cn.whaley.bi.logsys.forest.processor.LogProcessorTrait
import cn.whaley.bi.logsys.forest._
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.JavaConversions._

/**
 * Created by fj on 16/11/9.
 *
 * 通用的行为日志处理器
 * 1. 在归集层没有做签名验证的情况下，完成消息体签名验证
 * 2. 解析baseInfo属性为一个JSONObject，将其属性合并到父对象中
 * 3. 解析logs属性为一个JSON数组，其中每个元素合并父对象属性，并产生一个logId，形成一个ActionLogEntity对象
 *
 */
class GenericActionLogPostProcessor extends LogProcessorTrait with LogTrait {

    /**
     * POST日志消息体签名的Key值表，(版本号,key值)
     */
    private var signKeyMap: Map[String, String] = null

    /**
     * 解析字符串为JSONObject对象
     * @param str
     * @return ，如果失败返回None
     */
    private def parseObject(str: String): Option[JSONObject] = {
        try {
            Some(JSON.parseObject(str))
        } catch {
            case e: Throwable => {
                LOG.error("", e)
                None
            }
        }
    }

    /**
     * 解析字符串为JSONArray对象
     * @param str
     * @return 如果失败返回None
     */
    private def parseArray(str: String): Option[JSONArray] = {
        try {
            Some(JSON.parseArray(str))
        } catch {
            case e: Throwable => {
                LOG.error("", e)
                None
            }
        }
    }


    /**
     * 验证消息体格式
     *
     * @return processed:成功，formatFailure：格式解析失败，signFailure：签名校验失败
     *
     */
    private def verify(logEntity: ActionLogPostEntity): ProcessResultCode.ProcessCode = {

        //对于02版本的消息体，执行消息体签名验证;
        // msgBody中的msgSignFlag属性如果未0，则指明了签名验证未在接入层完成
        val needSign=logEntity.version == "02" && logEntity.method == "POST" && logEntity.msgSignFlag == 0
        //val needSign = (logEntity.version == "02" || logEntity.version == "01") && logEntity.method == "POST" && logEntity.msgSignFlag == 0
        val signed =
            if (needSign) {
                val hasSignInfo = (logEntity.baseInfo != null && logEntity.logs != null
                    && logEntity.ts != null && logEntity.md5 != null)
                val key = signKeyMap.get(logEntity.version)
                if (!hasSignInfo || !key.isDefined) {
                    false
                } else {
                    val baseInfo = logEntity.baseInfo.asInstanceOf[String]
                    val logs = logEntity.logs.asInstanceOf[String]
                    val ts = logEntity.ts
                    val md5 = logEntity.md5
                    val str = ts + baseInfo + logs + key.get
                    val signMD5 = DigestUtil.getMD5Str32(str)
                    signMD5 == md5
                }
            } else {
                true
            }

        if (!signed) {
            ProcessResultCode.signFailure
        } else {
            ProcessResultCode.processed
        }
    }


    /**
     * 初始化
     */
    def init(confManager: ConfManager): Unit = {
        val include = confManager.getConf(this.name + "." + "include")
        if (include != null) {
            val res = StringUtil.splitStr(include, ",")
            confManager.addConfResource(res: _*)
        }
        signKeyMap = confManager.getAllConf(GenericActionLogPostProcessor.CONFKEY_SIGN_KEY_PREFIX, true).toMap
    }


    /**
     * 解析日志消息体
     * @return
     */
    def process(log: LogEntity): ProcessResult[Seq[LogEntity]] = {

        //body展开后，构建ActionLogPostEntity对象
        val bodyValue = log.get("body")
        val bodyObj =
            if (bodyValue != null) {
                val obj =
                    if (bodyValue.isInstanceOf[String]) {
                        val str = (bodyValue.asInstanceOf[String])
                        JSON.parseObject(str)
                    } else {
                        bodyValue.asInstanceOf[JSONObject]
                    }
                log.remove("body")
                obj.asInstanceOf[java.util.Map[String, Object]].putAll(log)
                obj
            } else {
                log
            }

        val actionLogEntity = new ActionLogPostEntity(bodyObj)

        try {
            if (actionLogEntity.method != "POST") {
                return ProcessResult(this.name, ProcessResultCode.skipped, "", None)
            }


            //消息体验证

            val verifyCode = verify(actionLogEntity)
            if (verifyCode != ProcessResultCode.processed) {
                return ProcessResult(this.name, verifyCode, "", None)
            }
            //actionLogEntity.removeSignInfo()


            var errorResult: ProcessResult[Seq[LogEntity]] = null
            var logSeq: Seq[LogEntity] = null


            //---------------POST解析-----------------

            //baseInfo展开

            if (actionLogEntity.baseInfo != null) {
                val baseInfo =
                    if (actionLogEntity.baseInfo.isInstanceOf[String]) {
                        parseObject(actionLogEntity.baseInfo.asInstanceOf[String])
                    } else {
                        Some(actionLogEntity.baseInfo.asInstanceOf[JSONObject])
                    }
                if (baseInfo.isEmpty) {
                    errorResult = ProcessResult(this.name, ProcessResultCode.formatFailure, "baseInfo is not json", None)
                } else {
                    actionLogEntity.removeBaseInfo()
                    actionLogEntity.asInstanceOf[java.util.Map[String, Object]].putAll(baseInfo.get)
                }
            }

            //logs展开,其中每个元素派生出一个新的ActionLogEntity对象
            if (errorResult == null) {
                if (actionLogEntity.logs != null) {

                    val logs =
                        if (actionLogEntity.logs.isInstanceOf[String]) {
                            parseArray(actionLogEntity.logs.asInstanceOf[String])
                        } else {
                            Some(actionLogEntity.logs.asInstanceOf[JSONArray])
                        }
                    if (!logs.isDefined) {
                        errorResult = ProcessResult(this.name, ProcessResultCode.formatFailure, "", None)
                    } else {

                        actionLogEntity.removeLogs()
                        val size = logs.get.size()
                        logSeq =
                            for (i <- 0 to size - 1) yield {
                                //通常log中的属性会比较多，将actionLogEntity合并到log
                                val log = logs.get.getJSONObject(i)
                                log.asInstanceOf[java.util.Map[String, Object]].putAll(actionLogEntity.asInstanceOf[java.util.Map[String, Object]])
                                val actionLog = new ActionLogPostEntity(log)
                                val logId = actionLogEntity.logId + StringUtil.fixLeftLen(Integer.toHexString(i), '0', 4)
                                actionLog.updateLogId(logId)
                                actionLog
                            }
                    }

                } else {
                    logSeq = Array(actionLogEntity)
                }
            }

            if (errorResult != null) {
                errorResult
            } else {
                require(logSeq != null)
                ProcessResult(this.name, ProcessResultCode.processed, "", Some(logSeq))
            }

        } catch {
            case e: Throwable => {
                ProcessResult(this.name, ProcessResultCode.exception, e.getMessage, None, Some(e))
            }
        }

    }


}

object GenericActionLogPostProcessor {

    private val CONFKEY_SIGN_KEY_PREFIX = "GenericActionLogPostProcessor.sign.key"
}
