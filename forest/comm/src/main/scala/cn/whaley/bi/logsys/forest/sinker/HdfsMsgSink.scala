package cn.whaley.bi.logsys.forest.sinker


import java.text.SimpleDateFormat
import java.util.{TimerTask, Date}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{ReentrantReadWriteLock}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.{ProcessResultCode, ProcessResult}
import cn.whaley.bi.logsys.forest.Traits.{LogTrait, NameTrait, InitialTrait}
import cn.whaley.bi.logsys.forest.entity.LogEntity
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{SequenceFile, Text}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 17/5/3.
 */
class HdfsMsgSink extends MsgSinkTrait with InitialTrait with NameTrait with LogTrait {

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {
        val conf = confManager.getAllConf("HdfsMsgSink.hdfs", true)
        conf.keySet().foreach(key => {
            val value = conf.getProperty(key.toString)
            hdfsConf.set(key.toString, value)
        })

        commitOpCount = confManager.getConfOrElseValue("HdfsMsgSink", "commitOpCount", commitOpCount.toString).toInt
        commitSizeByte = confManager.getConfOrElseValue("HdfsMsgSink", "commitSizeByte", commitSizeByte.toString).toLong
        commitTimeMillSec = confManager.getConfOrElseValue("HdfsMsgSink", "commitTimeMillSec", commitTimeMillSec.toString).toLong
        commitRootDir = confManager.getConfOrElseValue("HdfsMsgSink", "commitRootDir", commitRootDir)
        errRootDir = confManager.getConfOrElseValue("HdfsMsgSink", "errRootDir", errRootDir)

        launchTimeCommitter()

    }

    /**
     * 保存处理后的数据
     * @param procResults
     */
    override def saveProcMsg(procResults: Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])]): (Int, Int) = {
        val success = procResults.filter(result => result._2.hasErr == false)
        val error = procResults.filter(result => result._2.hasErr == true)
        (saveSuccess(success), saveError(error))
    }


    /**
     * 从目标中获取某个源topic最后写入的offset信息
     * @param sourceTopic 源topic
     * @param sourceLatestOffset 源topic的最后偏移信息
     * @param maxMsgCount 检索目标时,每个partition最多读取的消息数量
     * @return 源topic相关的offse信（partition，offset），如果没有则返回空Map对象
     */
    override def getTopicLastOffset(sourceTopic: String, sourceLatestOffset: Map[Int, Long], maxMsgCount: Int): Map[Int, Long] = ???

    /**
     * 保存正常数据
     * @param datas
     */
    private def saveSuccess(datas: Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])]): Int = {
        //相同topic的相同分区写入到相同文件
        var count = 0
        val items = datas.flatMap(data => {
            data._2.result.get.map(log => (data._1, log))
        })
        val groups = items.groupBy(item => {
            val message: KafkaMessage = item._1
            val log: LogEntity = item._2

            val isFromOri = messageFromOri(message)
            val fileFlag = if (isFromOri) "ori" else "raw"
            val parId = message.partition()
            val appId = log.appId
            val timeKey = new SimpleDateFormat("yyyyMMddHH").format(new Date(log.logTime))
            LogWriteCacheKey(appId, timeKey, fileFlag, parId)
        })
        groups.foreach(group => {
            val fileKey = group._1
            val items = group._2
            val firstItem = items.head
            val offset = firstItem._1.offset()
            val fileNamePrefix = fileKey.fileNamePrefix
            val filePath = s"${commitRootDir}/${fileNamePrefix}_${offset}.seq"

            val cacheItem = getOrCreateLogWriter(fileKey, filePath)
            try {
                cacheItem.writeLock.lock()
                items.foreach(item => {
                    val message = item._1
                    val syncInfo = buildSyncInfo(message)
                    val syncInfoJsonStr = syncInfo.toJSONString
                    val rowBytes = combineSyncInfo(syncInfoJsonStr, message.value())
                    cacheItem.writer.append(new Text(syncInfoJsonStr.getBytes), new Text(rowBytes))
                    cacheItem.changeOpBytes(rowBytes.length)
                    count = count + 1
                })
                commitLogFileIf(fileKey)
            }
            finally {
                cacheItem.writeLock.unlock()
            }

        })

        count
    }


    /**
     * 保存错误数据
     * @param datas
     */
    private def saveError(datas: Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])]): Int = {
        //相同topic的相同分区写入到相同文件
        var count = 0
        val groups = datas.groupBy(item => {
            val message = item._1
            (message.topic(), message.partition())
        })
        val fs = FileSystem.get(hdfsConf)
        groups.foreach(group => {
            val topic = group._1._1
            val parId = group._1._2
            val filePath = new Path(s"${errRootDir}/${topic}_${parId}.txt")
            val items = group._2
            fs.createNewFile(filePath)
            val writer = fs.append(filePath)
            items.foreach(item => {
                val message: KafkaMessage = item._1
                val errResult = item._2

                val errData =buildErrorData(message,errResult)
                writer.write(errData.toJSONString.getBytes)
                writer.write('\n')
                count = count + 1
            })
            writer.close()
        })
        count
    }


    //启动定期提交扫描线程
    private def launchTimeCommitter(): Unit = {
        val timer = new java.util.Timer()
        timer.scheduleAtFixedRate(new TimerTask {
            override def run(): Unit = {
                LOG.info("launch committer.")
                val entries = logWriterCache.entrySet().toList
                entries.foreach(entry => {
                    val value = entry.getValue
                    if (System.currentTimeMillis() - value.lastOpTs > commitTimeMillSec) {
                        value.readyForCommit.set(true)
                    }
                    if (value.readyForCommit.get()) {
                        commitLogFileIf(entry.getKey)
                    }
                })
            }
        }, new Date(System.currentTimeMillis() + commitTimeMillSec), commitTimeMillSec)
    }

    //消息是否来源于应用层
    private def messageFromOri(source: KafkaMessage): Boolean = {
        val keyBytes = source.key()
        if (keyBytes == null || keyBytes.length == 0) {
            false
        } else {
            try {
                val keyStr = new String(keyBytes)
                val obj = JSON.parseObject(keyStr)
                if (obj.containsKey("rawTopic")) {
                    true
                } else {
                    false
                }
            } catch {
                case ex: Throwable => {
                    //LOG.warn("parse keyObj error:" + keyStr)
                    false
                }
            }
        }

    }

    //如有必要则提交文件,并清理相关缓存项
    private def commitLogFileIf(fileKey: LogWriteCacheKey): Boolean = {
        val cacheItem = logWriterCache.get(fileKey)
        if (cacheItem == null || cacheItem.readyForCommit.get() == false) {
            return false
        }
        try {
            cacheItem.writeLock.lock()
            if (cacheItem.isCommitted.get()) {
                return false
            }

            val fs = FileSystem.get(hdfsConf)
            cacheItem.writer.close()
            val source = new Path(cacheItem.filePath)
            val keyAppIdPar = s"key_appId=${fileKey.appId}"
            val keyDayPar = s"key_day=${fileKey.timeKey.substring(0, 8)}"
            val keyHourPar = s"key_hour=${fileKey.timeKey.substring(8, 10)}"
            val target = new Path(s"${commitRootDir}/${keyAppIdPar}/${keyDayPar}/${keyHourPar}/${cacheItem.fileName}")
            fs.rename(source, target)
            LOG.info("commit file [ OpCount={}, OpBytes={},lastOpTime:{} ]: {} -> {}",
                Array(cacheItem.OpCount, cacheItem.OpBytes, new Date(cacheItem.lastOpTs), source, target))
            logWriterCache.remove(fileKey)
            cacheItem.isCommitted.set(true)
            true
        } finally {
            cacheItem.writeLock.unlock()
        }

    }

    //获取日志数据文件writer对象
    private def getOrCreateLogWriter(fileKey: LogWriteCacheKey, filePath: String): LogWriteCacheItem = {
        val item = logWriterCache.get(fileKey)
        if (item != null && item.readyForCommit == false) {
            return item
        }

        //提交
        if (item != null) {
            commitLogFileIf(fileKey)
        }

        //新建缓存项
        val path = new Path(filePath)
        var writer: SequenceFile.Writer = null
        writer = SequenceFile.createWriter(hdfsConf, SequenceFile.Writer.file(path)
            , SequenceFile.Writer.keyClass(classOf[Text]),
            SequenceFile.Writer.valueClass(classOf[Text]),
            SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
        );
        val newItem = new LogWriteCacheItem(filePath, writer, System.currentTimeMillis())
        logWriterCache.put(fileKey, newItem)
        newItem
    }


    //将同步信息合并到消息体中
    private def combineSyncInfo(syncInfoJsonStr: String, messageBody: Array[Byte]): Array[Byte] = {
        val buf = new ArrayBuffer[Byte]()
        var finded = false
        messageBody.foreach(b => {
            if (finded == false && b == '{') {
                finded = true
                buf.append('{')
                val str = "\"_sync\":{" + syncInfoJsonStr + "},"
                buf.appendAll(str.getBytes)
            } else {
                buf.append(b)
            }
        })
        buf.toArray
    }

    private var errRootDir = "/data_warehouse/ods_origin.db/err_log_origin"
    private var commitRootDir = "/data_warehouse/ods_origin.db/log_origin"
    private var commitOpCount: Int = 100000
    private var commitSizeByte: Long = 1024 * 1024 * 10
    private var commitTimeMillSec: Long = 300 * 1000
    private val hdfsConf: Configuration = new Configuration()
    private val logWriterCache: ConcurrentMap[LogWriteCacheKey, LogWriteCacheItem] = new ConcurrentHashMap[LogWriteCacheKey, LogWriteCacheItem]()

    case class LogWriteCacheKey(appId: String, timeKey: String, fileFlag: String, parId: Int) {
        val fileNamePrefix = s"${appId}_${timeKey}_${fileFlag}_${parId}"
    }

    class LogWriteCacheItem(pFilePath: String, pWriter: SequenceFile.Writer, pLastOpTs: Long) {
        var filePath: String = pFilePath
        var writer: SequenceFile.Writer = pWriter
        var lastOpTs: Long = pLastOpTs
        var OpBytes: Long = 0
        var OpCount: Long = 0
        val fileName: String = {
            val idx = pFilePath.lastIndexOf('/')
            pFilePath.substring(idx + 1)
        }
        val readyForCommit = new AtomicBoolean(false)
        var isCommitted = new AtomicBoolean(false)

        val writeLock = new ReentrantReadWriteLock().writeLock()

        def changeOpBytes(byteCount: Int): Unit = {
            OpBytes = OpBytes + byteCount
            OpCount = OpCount + 1

            if (OpBytes >= commitSizeByte) {
                readyForCommit.set(true)
            } else if (OpCount >= commitOpCount) {
                readyForCommit.set(true)
            } else if (System.currentTimeMillis() - lastOpTs >= commitTimeMillSec) {
                readyForCommit.set(true)
            }

            lastOpTs = System.currentTimeMillis()
        }


    }


}
