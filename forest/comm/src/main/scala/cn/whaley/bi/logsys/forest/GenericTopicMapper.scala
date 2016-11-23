package cn.whaley.bi.logsys.forest

import java.io.{File, FileOutputStream}
import java.util

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.Traits.{LogTrait, NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest.entity.LogEntity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/11/17.
 */
class GenericTopicMapper extends InitialTrait with LogTrait with NameTrait {

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        val errTopicMapperNames = confManager.getConf(this.name, "errTopicMapper")
        errTopicMappers =
            StringUtil.splitStr(errTopicMapperNames, ",").map(errTopicMapperName => {
                instanceFrom(confManager, errTopicMapperName).asInstanceOf[TopicMapperTrait]
            })

        val targetTopicMapperNames = confManager.getConf(this.name, "targetTopicMapper")
        targetTopicMappers =
            StringUtil.splitStr(targetTopicMapperNames, ",").map(targetTopicMapperName => {
                instanceFrom(confManager, targetTopicMapperName).asInstanceOf[TopicMapperTrait]
            })

        val defaultTopicMapFile = this.getClass.getClassLoader().getResource("").getPath() + "targetTopicMapFile.xml";
        targetTopicMapFile = confManager.getConfOrElseValue(this.name, "targetTopicMapFile", defaultTopicMapFile)

        //从文件中恢复目标topic映射
        targetTopicMap = new java.util.concurrent.ConcurrentHashMap[String, Seq[String]]()
        val filePath = getTargetTopicMapSavePath()
        val file = new File(filePath)
        if (file.exists() && file.isFile && file.length() > 0) {
            val confManager = new ConfManager(Array(filePath))
            val confs = confManager.getAllConf()
            confs.stringPropertyNames().toArray().foreach(item => {
                val key = item.asInstanceOf[String]
                val value = confs.getProperty(key).split(",").toSeq
                targetTopicMap.put(key, value)
            })
            this.hasChange = false
        }

    }


    /**
     * 错误结果写入Topic
     * @param sourceTopic
     * @return
     */
    def getErrTopic(sourceTopic: String, logEntity: LogEntity): Seq[String] = {
        val topics = new ArrayBuffer[String]()
        var break = false
        for (i <- 0 to errTopicMappers.length - 1 if break == false) {
            val ret = errTopicMappers(i).getTopic(sourceTopic, logEntity)
            break = ret.code == ProcessResultCode.break
            if (ret.code == ProcessResultCode.processed) {
                topics.append(ret.result.get: _*)
            }
        }
        topics
    }

    /**
     * 目标结果写入Topic
     * @param sourceTopic
     * @param logEntity
     * @return
     */
    def getTargetTopic(sourceTopic: String, logEntity: LogEntity): Seq[String] = {
        val topics = new ArrayBuffer[String]()
        var break = false
        for (i <- 0 to targetTopicMappers.length - 1 if break == false) {
            val ret = targetTopicMappers(i).getTopic(sourceTopic, logEntity)
            break = ret.code == ProcessResultCode.break
            if (ret.code == ProcessResultCode.processed) {
                topics.append(ret.result.get: _*)
            }
        }
        topics.foreach(topic => addTargetTopicMap(sourceTopic, topic))
        topics
    }

    /**
     * 目标topic映射信息保存路径
     * @return
     */
    def getTargetTopicMapSavePath(): String = {
        targetTopicMapFile
    }


    /**
     * 保存一条目标topic映射
     */
    def saveTargetTopicMap(): Unit = {
        this.synchronized {
            if (hasChange) {
                val confManager = new ConfManager(Array(""))
                val it = targetTopicMap.entrySet().iterator()
                while (it.hasNext) {
                    val curr = it.next()
                    confManager.putConf(curr.getKey, curr.getValue.mkString(","))
                }
                val fileOutputStream = new FileOutputStream(getTargetTopicMapSavePath())
                confManager.writeXml(fileOutputStream)
                fileOutputStream.close()
                hasChange = false
            }
        }
    }

    /**
     * 获取所有目标topic映射
     * @param sourceTopic
     * @return
     */
    def getTargetTopicMap(sourceTopic: String): Option[Seq[String]] = {
        val v = targetTopicMap.get(sourceTopic)
        if (v == null) None else Some(v)
    }

    /**
     * 增加一条目标topic映射
     * @param sourceTopic
     * @param targetTopic
     */
    def addTargetTopicMap(sourceTopic: String, targetTopic: String): Unit = {
        var contains = false
        if (targetTopicMap.contains(sourceTopic)) {
            val targets = targetTopicMap.get(sourceTopic)
            contains = targets.contains(targetTopic)
        }
        if (!contains) {
            val newValue: ArrayBuffer[String] = new ArrayBuffer[String]()
            if (targetTopicMap.contains(sourceTopic)) {
                newValue.append(targetTopicMap.get(sourceTopic): _*)
            }
            newValue.append(targetTopic)
            targetTopicMap.put(sourceTopic, newValue)
            hasChange = true
        }
    }

    private var errTopicMappers: Seq[TopicMapperTrait] = null
    private var targetTopicMappers: Seq[TopicMapperTrait] = null
    private var targetTopicMapFile: String = null
    private var hasChange = true
    private var targetTopicMap: java.util.concurrent.ConcurrentHashMap[String, Seq[String]] = null
}
