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
//                LOG.error("", e)
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
                LOG.debug("invalid jsonarray str:" + str, e)
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
        //crash日志校验

        val postBody = logEntity.postMsgBodyObj
        val bodyObj = postBody.bodyObj
        val signed = if(bodyObj.keys.contains("STACK_TRACE")){
            //crash日志校验
            verifyCrash(logEntity)
        }else{
            //post校验
            verifyPost(logEntity)
        }

        if (!signed) {
            logEntity.updateLogSignFlag(LogEntity.VAL_SIGN_ERR)
            ProcessResultCode.signFailure
        } else {
            logEntity.updateLogSignFlag(LogEntity.VAL_SIGN_PASS)
            ProcessResultCode.processed
        }

    }

    private def verifyCrash(logEntity: ActionLogPostEntity):Boolean = {
        val postBody = logEntity.postMsgBodyObj
        val bodyObj = postBody.bodyObj
        //添加logType
        bodyObj.put("logType","crashlog")
        //crash日志校验
        val appId = postBody.getString("appId")
        val flag:Boolean = appId match {
            //whaley main crash 日志不做处理
            case "boikgpokn78sb95kjhfrendo8dc5mlsr" => {
                true
            }
            // medusa main3.x crash md5校验
            case "boikgpokn78sb95ktmsc1bnkechpgj9l" => {
                // medusa main3.x crash md5校验
                val CRASH_KEY = "p&i#Hl5!gAo*#dwSa9sfluynvaoOKD3"
                val crashKey = bodyObj.getString("CRASH_KEY")
                val createDate = bodyObj.getString("USER_CRASH_DATE")
                val mac = bodyObj.getString("MAC")
                val productCode = bodyObj.getString("PRODUCT_CODE")
                val md5 = bodyObj.getString("MD5")
                val verificationStr = CRASH_KEY+crashKey+createDate+mac+productCode
                md5 == DigestUtil.getMD5Str32(verificationStr)
            }
            case _ => {
                false
            }
        }
        flag

    }

    private def verifyPost(logEntity: ActionLogPostEntity):Boolean = {
        val postBody = logEntity.postMsgBodyObj
        val bodyObj = postBody.bodyObj
        //对于01版本的消息体，执行消息体签名验证;
        // msgBody中的msgSignFlag属性如果未0，则指明了签名验证未在接入层完成
        val needSign = bodyObj.version == "01" && postBody.method.equalsIgnoreCase("POST") && logEntity.msgSignFlag == 0
        val signed =
            if (needSign) {
                val hasSignInfo = (bodyObj.baseInfo != null && bodyObj.logs != null
                  && bodyObj.ts != null && bodyObj.md5 != null)
                val key = signKeyMap.get(bodyObj.version)
                if (!hasSignInfo || !key.isDefined) {
                    false
                } else {
                    val baseInfo = bodyObj.baseInfo.asInstanceOf[String]
                    val logs = bodyObj.logs.asInstanceOf[String]
                    val ts = bodyObj.ts
                    val md5 = bodyObj.md5
                    val str = ts + baseInfo + logs + key.get
                    val signMD5 = DigestUtil.getMD5Str32(str)
                    signMD5 == md5
                }
            } else {
                logEntity.updateLogSignFlag(LogEntity.VAL_SIGN_NO)
                true
            }
        signed
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
        val actionLogEntity = new ActionLogPostEntity(log)
        if (actionLogEntity.msgBody == null) {
            return ProcessResult(this.name, ProcessResultCode.skipped, "msgBody is null", None)
        }
        val method = actionLogEntity.msgBodyObj.method
        if (method == null) {
            return ProcessResult(this.name, ProcessResultCode.skipped, "msgBodyObj.method is null", None)
        }
        if (method != "GET" && method != "POST") {
            return ProcessResult(this.name, ProcessResultCode.discard, s"invalid method ${method}", None)
        }
        val postObj = actionLogEntity.postMsgBodyObj;
        try {

            if (!postObj.method.equalsIgnoreCase("POST")) {
                return ProcessResult(this.name, ProcessResultCode.skipped, "", None)
            }

            //消息体验证,方法内部将更新相关签名字段的值
            val verifyCode = verify(actionLogEntity)
            if (verifyCode != ProcessResultCode.processed) {
                return ProcessResult(this.name, verifyCode, "", None)
            }

            var errorResult: ProcessResult[Seq[LogEntity]] = null

            //---------------POST解析-----------------
            val bodyObj = postObj.bodyObj;
            //baseInfo展开
            if (bodyObj.baseInfo != null) {
                val baseInfo =
                    if (bodyObj.baseInfo.isInstanceOf[String]) {
                        parseObject(bodyObj.baseInfo.asInstanceOf[String])
                    } else {
                        Some(bodyObj.baseInfo.asInstanceOf[JSONObject])
                    }
                if (baseInfo.isEmpty) {
                    errorResult = ProcessResult(this.name, ProcessResultCode.formatFailure, "baseInfo is not json", None)
                } else {
                    bodyObj.removeBaseInfo()
                    bodyObj.asInstanceOf[java.util.Map[String, Object]].putAll(baseInfo.get)
                }
            }

            //logs展开,其中每个元素派生出一个新的ActionLogEntity对象
            if (errorResult == null && bodyObj.logs != null) {
                val logs =
                    if (bodyObj.logs.isInstanceOf[String]) {
                        parseArray(bodyObj.logs.asInstanceOf[String])
                    } else {
                        Some(bodyObj.logs.asInstanceOf[JSONArray])
                    }
                if (!logs.isDefined) {
                    errorResult = ProcessResult(this.name, ProcessResultCode.formatFailure, "", None)
                } else {
                    bodyObj.removeLogs()
                    val size = logs.get.size()
                    val array = new JSONArray()
                    for (i <- 0 to size - 1) yield {
                        //通常log中的属性会比较多，将bodyObj合并到log
                        val log = logs.get.getJSONObject(i)
                        log.asInstanceOf[java.util.Map[String, Object]].putAll(bodyObj)
                        array.add(log)
                    }
                    if (array.size == 1) {
                        postObj.setBody(array.get(0))
                    } else {
                        postObj.setBody(array)
                    }
                }
            }

            if (errorResult != null) {
                errorResult
            } else {
                val ret = Some(actionLogEntity.normalize())
                ProcessResult(this.name, ProcessResultCode.processed, "", ret)
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
