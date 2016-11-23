package cn.whaley.bi.logsys.forest.actionlog

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.Traits.LogTrait
import cn.whaley.bi.logsys.forest.{ProcessResultCode, ProcessResult, StringUtil}
import cn.whaley.bi.logsys.forest.entity.LogEntity
import cn.whaley.bi.logsys.forest.processor.LogProcessorTrait
import com.alibaba.fastjson.JSONObject

/**
 * Created by fj on 16/11/14.
 */
class GenericActionLogCommProcessor extends LogProcessorTrait with LogTrait {
    /**
     * 解析日志消息体
     * @return
     */
    override def process(log: LogEntity): ProcessResult[Seq[LogEntity]] = {

        var actionLog = new ActionLogPostEntity(log)

        //平展化params
        floatParams(actionLog)

        //特殊处理
        val revise = infoRevise(actionLog)
        if (revise.isEmpty) {
            return new ProcessResult(this.name, ProcessResultCode.processed, "", Some(Nil))
        }

        actionLog = revise.get


        //字段重命名
        if (fieldRenameConf != null) {
            fieldRenameConf.filter(item => actionLog.getString(LogKeys.LOG_TYPE) == item._1)
                .foreach(item => {
                if (actionLog.containsKey(item._2)) {
                    actionLog.put(item._3, log.get(item._2))
                    actionLog.remove(item._2)
                }
            })
        }
        longTypeKeys.foreach(item => {
            val value = actionLog.get(item)
            if (value != null && !value.isInstanceOf[Long]) {
                if (value == "") {
                    actionLog.put(item, 0L)
                } else {
                    actionLog.put(item, value.toString.toLong)
                }
            }
        })

        new ProcessResult(this.name, ProcessResultCode.processed, "", Some(log :: Nil))

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
    }

    //平展化params
    private def floatParams(log: ActionLogPostEntity): ActionLogPostEntity = {
        // 平展化params信息
        if (log.containsKey(LogKeys.LOG_PARAMS)) {
            // 处理params非json格式的情形
            val paramsV = log.get(LogKeys.LOG_PARAMS)
            if (paramsV.isInstanceOf[String]) {
                val paramsInfo = paramsV.asInstanceOf[String]
                if (paramsInfo.contains(",")) {
                    val parmasSplit = paramsInfo.split(",")
                    if (parmasSplit.length >= 1) {
                        (0 until parmasSplit.length).foreach(i => {
                            val parameterInfo = parmasSplit(i).trim
                            if (parameterInfo.contains("=")) {
                                val parameterSplit = parameterInfo.split("=")
                                if (parameterSplit.length == 2) {
                                    log.put(parameterSplit(0), parameterSplit(1))
                                }
                            }
                        })
                    }
                }
            } else if (paramsV.isInstanceOf[JSONObject]) {
                val paramsInfo = paramsV.asInstanceOf[JSONObject]
                log.asInstanceOf[java.util.Map[String, Object]].putAll(paramsInfo)
            }
            log.remove(LogKeys.LOG_PARAMS)
        }
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


    private val regexWord = "^\\w+$".r
    private val regexContentType = "^[\\w\\-/]+$".r
    private var fieldRenameConf: Seq[(String, String, String)] = null
    private var longTypeKeys: Seq[String] = null
}
