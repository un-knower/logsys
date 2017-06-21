package cn.whaley.bi.logsys.log2parquet.processingUnit

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.log2parquet.utils.StringUtil
import cn.whaley.bi.logsys.log2parquet.{ProcessResultCode, ProcessResult}
import cn.whaley.bi.logsys.log2parquet.entity.{ActionLogPostEntity, MsgBodyEntity, LogEntity}
import cn.whaley.bi.logsys.log2parquet.processor.LogProcessorTrait
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait

/**
 * Created by fj on 16/11/14.
 */
class GenericActionLogCommProcessor extends LogProcessorTrait with LogTrait {
    /**
     * 解析日志消息体
      *
      * @return
     */
    override def process(log: LogEntity): ProcessResult[LogEntity] = {
        logTimeProc(log)
        new ProcessResult(this.name, ProcessResultCode.processed, "", Some(log))

    }

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        var conf = confManager.getConf("GenericActionLogCommProcessor.FieldRename")
        if (conf != null) {
            fieldRenameConf =
                StringUtil.splitStr(conf, ",").map(item => {
                    val vs = item.split(":")
                    val logType = vs(0)
                    val rename = vs(1).split("->")
                    (logType, rename(0), rename(1))
                })
        }
        conf = confManager.getConf("GenericActionLogCommProcessor.LongTypeKeys")
        if (conf != null) {
            longTypeKeys = conf.split(",")
        }

        happenTimeDeviationMillSec = confManager.getConfOrElseValue("GenericActionLogCommProcessor", "happenTime.deviation.sec", "3600").toLong * 1000
    }

    private def logTimeProc(log: LogEntity): LogEntity = {
        if (log.logBody == null) {
            return log
        }
        val receiveTime = log.logBody.getLong(MsgBodyEntity.KEY_SVR_RECEIVE_TIME)
        val logTime = if (log.logBody.containsKey(KEY_HAPPEN_TIME)) {
            val happenTime = try {
                log.logBody.getLong(KEY_HAPPEN_TIME)
            } catch {
                case ex: Throwable => {
                    null
                }
            }
            if (happenTime != null && Math.abs(receiveTime - happenTime) <= happenTimeDeviationMillSec) {
                happenTime
            } else {
                receiveTime
            }

        }

        else {
            receiveTime
        }
        log.updateLogTime(logTime)
        log
    }

    //特殊处理
    private def infoRevise(json: ActionLogPostEntity): Option[ActionLogPostEntity] = {
        val logType = json.getString(LogKeys.LOG_TYPE)
        val contentType = json.getString(LogKeys.CONTENT_TYPE)
        var info = Option(json)
        if (contentType != "") {
            regexContentType findFirstIn (contentType) match {
                case Some(_) =>
                case None => {
                    info = None
                }
            }

        }
        if (info.isDefined) {
            logType match {
                case "collect" => {
                    val eventType = json.getString(LogKeys.EVENT)
                    if (eventType == "live" || eventType == "past") {
                        // 将logType的内容更改为'live'
                        json.put(LogKeys.LOG_TYPE, "live")
                    }
                }
                case "launcher" => {
                    val accessArea = json.getString(LogKeys.ACCESS_AREA)
                    val accessLocation = json.getString(LogKeys.ACCESS_LOCATION)
                    if (accessArea != null && accessArea != "") {
                        regexWord findFirstIn accessArea match {
                            case Some(_) => if (accessLocation != "") {
                                regexWord findFirstIn accessLocation match {
                                    case Some(_) =>
                                    case None => {
                                        info = None
                                    }
                                }
                            }
                            // 将该log标记为null
                            case None => {
                                info = None
                            }
                        }
                    }
                }
                case _ =>
            }
        }
        if (info.isDefined) {
            if (info.get.containsKey("jsonlog") && info.get.getString("jsonlog").contains("playqos")) {
                info.get.put("logType", "playqos")
            }
        }
        info
    }


    private val KEY_HAPPEN_TIME: String = "happenTime"
    private var happenTimeDeviationMillSec: Long = 3600 * 1000;
    private val normal: Seq[String] = null
    private val regexWord = "^\\w+$".r
    private val regexContentType = "^[\\w\\-/]+$".r
    private var fieldRenameConf: Seq[(String, String, String)] = null
    private var longTypeKeys: Seq[String] = null
}
