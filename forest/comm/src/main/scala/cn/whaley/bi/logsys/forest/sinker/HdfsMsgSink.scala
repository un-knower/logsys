package cn.whaley.bi.logsys.forest.sinker


import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Timer, TimerTask}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{CountDownLatch, Executors}

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.ProcessResult
import cn.whaley.bi.logsys.forest.Traits.{InitialTrait, LogTrait, NameTrait}
import cn.whaley.bi.logsys.forest.entity.LogEntity
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{IOUtils, SequenceFile, Text}
import org.apache.hadoop.util.ReflectionUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

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

        codec = confManager.getConfOrElseValue("HdfsMsgSink", "codec", codec);
        commitOpCount = confManager.getConfOrElseValue("HdfsMsgSink", "commitOpCount", commitOpCount.toString).toInt
        commitSizeByte = confManager.getConfOrElseValue("HdfsMsgSink", "commitSizeByte", commitSizeByte.toString).toLong
        commitTimeMillSec = confManager.getConfOrElseValue("HdfsMsgSink", "commitTimeMillSec", commitTimeMillSec.toString).toLong
        commitRootDir = confManager.getConfOrElseValue("HdfsMsgSink", "commitRootDir", commitRootDir)
        tmpRootDir = confManager.getConfOrElseValue("HdfsMsgSink", "tmpRootDir", tmpRootDir)
        errRootDir = confManager.getConfOrElseValue("HdfsMsgSink", "errRootDir", errRootDir)

        //至少需要有一个选项被设置
        require(!(commitTimeMillSec <= 0 && commitOpCount <= 0 && commitSizeByte <= 0))

        launchTimeCommitter()

    }

    /**
     * 停止服务
     */
    override def stop(): Unit = {
        if (commitTimer != null) {
            commitTimer.cancel()
        }
        while (saveOptCount.get() > 0) {
            LOG.info(s"saveOptCount=${saveOptCount.get()},sleep 1s")
            Thread.sleep(1000)
        }
        //关闭线程池
        procThreadPool.shutdown();

        //关闭所有文件流
        logWriterCache.keySet.foreach(key => {
            val item = logWriterCache.get(key)
            item.writeLock.lock()
            IOUtils.closeStream(item.writer)
            commitFile(key)
            LOG.info(s"clean file: ${item.fileName}")
            item.writeLock.unlock()
        })
    }

    /**
     * 保存处理后的数据
     * @param procResults
     */
    override def saveProcMsg(procResults: Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])]): (Int, Int) = {
        try {
            saveOptCount.incrementAndGet()
            val success = procResults.filter(result => result._2.hasErr == false)
            val error = procResults.filter(result => result._2.hasErr == true)
            (saveSuccess(success), saveError(error))
        }
        finally {
            saveOptCount.decrementAndGet();
        }
    }


    /**
     * 从目标中获取某个源topic最后写入的offset信息
     * @param sourceTopic 源topic
     * @param sourceLatestOffset 源topic的最后偏移信息
     * @param maxMsgCount 检索目标时,每个partition最多读取的消息数量
     * @return 源topic相关的offse信（partition，offset），如果没有则返回空Map对象
     */
    override def getTopicLastOffset(sourceTopic: String, sourceLatestOffset: Map[Int, Long], maxMsgCount: Int): Map[Int, Long] = {
        val offsetMap = new mutable.HashMap[Int, Long]
        val isFromOri = sourceTopic.startsWith("log-origin-")
        val isFromRaw = sourceTopic.startsWith("log-raw-")
        val appIdOrProduct = if (isFromRaw) {
            sourceTopic.substring("log-raw-".length)
        } else if (isFromOri) {
            sourceTopic.substring("log-origin-".length)
        } else {
            ""
        }
        if (appIdOrProduct == "") {
            return offsetMap.toMap
        }

        val fs = FileSystem.get(hdfsConf)
        val pathPattern = new Path(s"${tmpRootDir}/${appIdOrProduct}*.txt")
        val fileStatuses = fs.globStatus(pathPattern)
        if (fileStatuses.size == 0) {
            return offsetMap.toMap
        }
        fileStatuses.foreach(status => {
            try {
                val reader = new SequenceFile.Reader(hdfsConf, SequenceFile.Reader.file(status.getPath));
                var lastKey: Text = null
                val key = ReflectionUtils.newInstance(reader.getKeyClass, hdfsConf).asInstanceOf[Text];
                val value = ReflectionUtils.newInstance(reader.getValueClass, hdfsConf).asInstanceOf[Text]
                //取最后一行数据
                while (reader.next(key, value)) {
                    lastKey = key
                }
                if (lastKey != null && lastKey.getLength > 0) {
                    val syncInfo = JSON.parseObject(new String(key.getBytes))
                    val parId = if (isFromOri) syncInfo.getIntValue("oriParId") else syncInfo.getIntValue("rawParId")
                    val offset = if (isFromOri) syncInfo.getLongValue("oriOffset") else syncInfo.getLongValue("rawOffset")
                    //最后偏移不能大于源topic的最后偏移值
                    val latestOffsetValue = sourceLatestOffset.getOrElse(parId, 0L)
                    val currValue = Math.max(offset, offsetMap.getOrElse(parId, offset))
                    val value = Math.min(currValue, latestOffsetValue)
                    offsetMap.put(parId, value)
                }
                IOUtils.closeStream(reader)

            }
            catch {
                case ex: Throwable => {
                    ex.printStackTrace()
                }
            }
        })
        offsetMap.toMap
    }


    /**
      * 保存监控数据
      *
      * @param monitorInfo 监控数据
      */
    override def saveMonitorInfo(monitorInfo: JSONObject): Unit = {}

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

            val isFromOri = message.topic().startsWith("log-origin")
            val fileFlag = if (isFromOri) "ori" else "raw"
            val parId = message.partition()
            val appId = log.appId
            val timeKey = new SimpleDateFormat("yyyyMMddHH").format(new Date(log.logTime))
            LogWriteCacheKey(appId, timeKey, fileFlag, parId)
        })
        groups.foreach(group => {
            val fileKey = group._1
            val items = group._2

            //以第一条记录的offset作为文件名的一部分,同一时间段内可以存在多个文件
            val firstItem = items.head
            val offset = firstItem._1.offset()
            val fileNamePrefix = fileKey.fileNamePrefix
            val filePath = s"${tmpRootDir}/${fileNamePrefix}_${offset}.json"

            val cacheItem = getOrCreateLogWriter(fileKey, filePath)
            try {
                cacheItem.writeLock.lock()
                items.foreach(item => {
                    writeEntity(cacheItem, filePath, item)
                    count = count + 1
                })
                if (cacheItem.readyForCommit.get() == true) {
                    IOUtils.closeStream(cacheItem.writer);
                }
                commitFileIf(fileKey)
            }
            finally {
                cacheItem.writeLock.unlock()
            }
        })
        count
    }

    def writeEntity(cacheItem: LogWriteCacheItem, filePath: String, item: (KafkaMessage, LogEntity)) = {
        val message = item._1
        val msgBody = item._2
        val syncInfo = buildSyncInfo(message)
        msgBody.put("_sync", syncInfo)
        val bytes = msgBody.toJSONString.getBytes
        try {
            cacheItem.writer.write(bytes)
            cacheItem.writer.write('\n')
        }
        catch {
            case ex: java.nio.channels.ClosedChannelException => {
                //文件系统关闭的情况下重新打开文件
                LOG.warn("FileChannel closed.open file again. " + filePath)
                val ret = createFileWriter(filePath)
                cacheItem.filePath = ret._1
                cacheItem.writer = ret._2
                cacheItem.writer.write(bytes)
            }
        }
        cacheItem.changeOpBytes(bytes.length)
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
        val currDate=new SimpleDateFormat("yyyyMMdd").format(new Date())
        val currHour=new SimpleDateFormat("HH").format(new Date())
        val fs = FileSystem.get(hdfsConf)
        groups.foreach(group => {
            val topic = group._1._1
            val parId = group._1._2
            val items = group._2
            val writer = {
                try {
                    val filePath = s"${errRootDir}/key_day=${currDate}/key_hour=${currHour}/${topic}_${parId}.json"
                    val path = new Path(filePath)
                    if (fs.createNewFile(path)) {
                        LOG.info(s"append file:${filePath}")
                    }
                    fs.append(path)
                } catch {
                    case ex: Throwable => {
                        val filePath = s"${errRootDir}/key_day=${currDate}/key_hour=${currHour}/${topic}_${parId}_${System.currentTimeMillis()}.json"
                        val path = new Path(filePath)
                        LOG.warn(s"append not supported. create file: ${filePath}")
                        LOG.error("append file failure:", ex);
                        fs.create(path, true)
                    }
                }
            }
            items.foreach(item => {
                val message: KafkaMessage = item._1
                val errResult = item._2

                val errData = buildErrorData(message, errResult)
                writer.write(errData.toJSONString.getBytes)
                writer.write('\n')
                count = count + 1
            })
            writer.close()
        })
        count
    }


    //启动定期提交扫描线程,提交已经提交就绪的文件
    private def launchTimeCommitter(): Unit = {
        commitTimer = new java.util.Timer()
        val interval = if (commitTimeMillSec > 0) commitTimeMillSec else 300 * 1000
        commitTimer.scheduleAtFixedRate(new TimerTask {
            override def run(): Unit = {
                //缓存中的满足提交条件的文件
                val entries = logWriterCache.entrySet().toList
                var count = 0
                entries.foreach(entry => {
                    val value = entry.getValue
                    if (commitTimeMillSec > 0 && System.currentTimeMillis() - value.lastOpTs > commitTimeMillSec) {
                        value.readyForCommit.set(true)
                    }
                    if (value.readyForCommit.get()) {
                        val committed = commitFileIf(entry.getKey)
                        count = count + (if (committed) 1 else 0)
                    }
                })
                if (count > 0) {
                    LOG.info(s"launch committer.${count} files committed.")
                }
            }
        }, new Date(System.currentTimeMillis() + interval), interval / 2)
    }

    //如有必要则提交文件,并清理相关缓存项
    private def commitFileIf(fileKey: LogWriteCacheKey): Boolean = {
        val cacheItem = logWriterCache.get(fileKey)
        if (cacheItem == null || cacheItem.readyForCommit.get() == false || cacheItem.isCommitted.get() == true) {
            return false
        }
        procThreadPool.submit(new Runnable {
            override def run(): Unit = {
                try {
                    cacheItem.writeLock.lock()
                    if (cacheItem.isCommitted.get() == true) {
                        return
                    }
                    IOUtils.closeStream(cacheItem.writer)
                    commitFile(fileKey)
                    logWriterCache.remove(fileKey)
                    cacheItem.isCommitted.set(true)
                } finally {
                    cacheItem.writeLock.unlock()
                }
            }
        })

        true
    }

    private def commitFile(fileKey: LogWriteCacheKey): Unit = {
        val cacheItem = logWriterCache.get(fileKey)
        val source = cacheItem.filePath
        val target = fileKey.getTargetFilePath(cacheItem.fileName)
        val fs = FileSystem.get(hdfsConf)
        val sourceLen = fs.getContentSummary(new Path(source)).getLength
        if (sourceLen > 0) {
            val finalPath = HdfsMsgSink.compressFileWithTry(source, target, codec, hdfsConf, 1)
            if (!finalPath.isDefined || !fs.exists(new Path(finalPath.get))) {
                LOG.warn(s"commit file failure, target file not exist.${source}[${sourceLen}] -> ${finalPath.get}")
                return;
            }
            fs.delete(new Path(source), false);
            val finalLen = fs.getContentSummary(new Path(finalPath.get)).getLength
            LOG.info(s"commit file ${source}[${sourceLen}] -> ${finalPath.get}[${finalLen}] [ OpCount=${cacheItem.OpCount}, OpBytes=${cacheItem.OpBytes},lastOpTime:${new Date(cacheItem.lastOpTs)} ]")
        } else {
            fs.delete(new Path(source), false);
            LOG.warn(s"source file is empty.[${source}}]")
        }

    }

    //获取日志数据文件writer对象
    private def getOrCreateLogWriter(fileKey: LogWriteCacheKey, filePath: String): LogWriteCacheItem = {
        val item = logWriterCache.get(fileKey)
        if (item != null) {
            //获取写锁,确保状态判断的正确性
            try {
                item.writeLock.lock()
                if (item.readyForCommit.get() == false) {
                    return item
                } else {
                    LOG.info(s"item[${item.filePath},OpBytes=${item.OpBytes},OpCount=${item.OpCount},lastOpTs=${item.lastOpTs},ts=${System.currentTimeMillis() - item.lastOpTs}] readyForCommit=true ,new item will be created")
                }
            } finally {
                item.writeLock.unlock()
            }
        } else {
            LOG.info(s"item[${fileKey}] not found ,new item will be created")
        }
        val ret = createFileWriter(filePath)
        val newItem = new LogWriteCacheItem(ret._1, ret._2, System.currentTimeMillis())
        logWriterCache.put(fileKey, newItem)
        LOG.info(s"create item ${filePath}")
        newItem
    }

    private def createFileWriter(filePath: String): (String, FSDataOutputStream) = {
        //如果不存在,则新建缓存项
        var targetFilePath = filePath
        var path = new Path(targetFilePath)
        val fs = FileSystem.get(hdfsConf)
        val ts = System.currentTimeMillis()
        val writer = try {
            fs.create(path, false)
        } catch {
            case ex: IOException => {
                val idx = filePath.lastIndexOf('.')
                val pref = filePath.substring(0, idx)
                val ext = filePath.substring(idx + 1)
                targetFilePath = s"${pref}_${ts}.${ext}"
                path = new Path(targetFilePath)
                fs.create(path, false)
            }
        }
        (targetFilePath, writer)
    }

    private var codec: String = HdfsMsgSink.DEFAULT_CODEC;

    //文件提交定时器
    private var commitTimer: Timer = null;

    //文件提交线程池
    private val procThreadPool = Executors.newCachedThreadPool()

    private val hdfsConf: Configuration = new Configuration()
    //错误数据根目录
    private var errRootDir = "/data_warehouse/ods_origin.db/err_log_origin2"
    //提交数据根目录
    private var commitRootDir = "/data_warehouse/ods_origin.db/log_origin2"
    //临时数据根目录
    private var tmpRootDir = "/data_warehouse/ods_origin.db/tmp_log_origin2"
    //操作次数阈值
    private var commitOpCount: Int = 100000
    //操作字节数阈值
    private var commitSizeByte: Long = 1024 * 1024 * 10
    //操作时间间隔阈值
    private var commitTimeMillSec: Long = 300 * 1000
    //缓存
    private val logWriterCache: java.util.concurrent.ConcurrentHashMap[LogWriteCacheKey, LogWriteCacheItem] = new java.util.concurrent.ConcurrentHashMap[LogWriteCacheKey, LogWriteCacheItem]()

    //保存操作计数器
    private val saveOptCount: AtomicInteger = new AtomicInteger(0)


    case class LogWriteCacheKey(appId: String, timeKey: String, fileFlag: String, parId: Int) {
        val fileNamePrefix = s"${appId}_${timeKey}_${fileFlag}_${parId}"

        def getTargetFilePath(srcfileName: String): String = {
            val keyAppIdPar = s"key_appId=${appId}"
            val keyDayPar = s"key_day=${timeKey.substring(0, 8)}"
            val keyHourPar = s"key_hour=${timeKey.substring(8, 10)}"
            s"${commitRootDir}/${keyAppIdPar}/${keyDayPar}/${keyHourPar}/${srcfileName}"
        }

        override def equals(that: Any): Boolean = {
            if (that == null) {
                false
            } else if (!that.isInstanceOf[LogWriteCacheKey]) {
                false
            } else {
                val other = that.asInstanceOf[LogWriteCacheKey]
                val eq = other.appId == this.appId && other.timeKey == this.timeKey && other.fileFlag == this.fileFlag && other.parId == this.parId
                eq
            }
        }

    }

    class LogWriteCacheItem(pFilePath: String, pWriter: FSDataOutputStream, pLastOpTs: Long) {
        var filePath: String = pFilePath
        var writer: FSDataOutputStream = pWriter
        var lastOpTs: Long = pLastOpTs
        var OpBytes: Long = 0
        var OpCount: Long = 0
        val readyForCommit = new AtomicBoolean(false)
        var isCommitted = new AtomicBoolean(false)
        val writeLock = new ReentrantReadWriteLock().writeLock()

        val fileName: String = {
            val idx = pFilePath.lastIndexOf('/')
            pFilePath.substring(idx + 1)
        }

        //记录更改,且如条件满足则设置提交标致
        def changeOpBytes(byteCount: Int): Unit = {
            OpBytes = OpBytes + byteCount
            OpCount = OpCount + 1

            if (commitSizeByte > 0 && OpBytes >= commitSizeByte) {
                readyForCommit.set(true)
            } else if (commitOpCount > 0 && OpCount >= commitOpCount) {
                readyForCommit.set(true)
            } else if (System.currentTimeMillis() - lastOpTs >= commitTimeMillSec) {
                readyForCommit.set(true)
            }

            lastOpTs = System.currentTimeMillis()
        }


    }


}

object HdfsMsgSink {
    val LOG = LoggerFactory.getLogger(this.getClass)

    val DEFAULT_CODEC: String = "org.apache.hadoop.io.compress.GzipCodec";

    def compressFileWithTry(srcFilePath: String, targetFilePath: String, codec: String, conf: Configuration, tryCount: Int): Option[String] = {
        var finalPath = "";
        val maxRunCount = Math.max(1, tryCount + 1);
        var currCount = 0;
        while (currCount < maxRunCount) {
            currCount = currCount + 1
            try {
                finalPath = compressFile(srcFilePath, targetFilePath, codec, conf)
            } catch {
                case ex: Throwable => {
                    finalPath = ""
                    LOG.error(s"[${currCount}/${maxRunCount}]compress file error. ${srcFilePath} -> ${targetFilePath}", ex)
                }
            }
            if (finalPath != "") {
                currCount = maxRunCount
            }
        }
        if (finalPath == "") {
            return None
        } else {
            return Some(finalPath)
        }
    }

    //压缩文件
    def compressFile(srcFilePath: String, targetFilePath: String, codec: String, conf: Configuration): String = {
        val codecClass = Class.forName(codec)
        val fs = FileSystem.get(conf);
        val codecObj = ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[CompressionCodec];

        val targetFilePathObj = if (!targetFilePath.endsWith(codecObj.getDefaultExtension)) {
            new Path(targetFilePath + codecObj.getDefaultExtension);
        } else {
            new Path(targetFilePath)
        }
        val tmpPath = new Path(srcFilePath + codecObj.getDefaultExtension());
        val outputStream = fs.create(tmpPath, true);
        val in = fs.open(new Path(srcFilePath));
        val out = codecObj.createOutputStream(outputStream);
        try {
            IOUtils.copyBytes(in, out, conf);
            IOUtils.closeStream(out);
            if(!fs.exists(targetFilePathObj.getParent())) {
                fs.mkdirs(targetFilePathObj.getParent())
            }
            fs.rename(tmpPath, targetFilePathObj)
            targetFilePathObj.toUri.getPath
        } catch {
            case ex: Throwable => {
                throw ex
            }
        } finally {
            try {
                if (fs.exists(tmpPath)) {
                    fs.delete(tmpPath, false)
                }
            }
            catch {
                case _: Throwable => {}
            }
            IOUtils.closeStream(in);
        }

    }

    /**
     * java -classpath $classpath cn.whaley.bi.logsys.forest.sinker.HdfsMsgSink
     * /data_warehouse/ods_origin.db/tmp_log_origin
     * /data_warehouse/ods_origin.db/log_origin
     * org.apache.hadoop.io.compress.GzipCodec
     * @param args
     */
    def main(args: Array[String]): Unit = {
        LOG.info(s"args:${args.mkString(",")}")
        if (args.length < 2) {
            System.out.println("require 2 args. (inPath:String,outDir:String)");
            System.exit(-1)
        }
        val inPath = args(0)
        val outDir = args(1)
        val codec = if (args.length > 2) args(2) else DEFAULT_CODEC

        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        val statuses = fs.globStatus(new Path(inPath))
        val fileNameR = "([a-zA-Z0-9]{32})_(\\d{10})_raw_\\d+_\\d+.*.json$".r
        for (i <- 0 to statuses.length - 1) {
            val status = statuses(i)
            val fileName = status.getPath.getName;
            if (status.isFile && status.getLen > 0) {
                val m = fileNameR.findFirstMatchIn(fileName)
                if (m.isDefined) {
                    val appId = m.get.group(1)
                    val time = m.get.group(2)
                    val day = time.substring(0, 8)
                    val hour = time.substring(8)
                    val targetPath = s"${outDir}/key_appId=${appId}/key_day=${day}/key_hour=${hour}/${fileName}"
                    val finalPath = compressFile(status.getPath.toUri.getPath, targetPath, codec, conf)
                    if(fs.exists(new Path(finalPath))){
                        fs.delete(status.getPath, false)
                        LOG.info(s"process file: ${status.getPath.toUri.getPath} -> ${finalPath}")
                    }
                    else {
                        LOG.error(s"failure , finalPath not exists. ${status.getPath.toUri.getPath} -> ${finalPath}")
                    }

                } else {
                    LOG.info(s"skip file:${status.getPath}")
                }

            } else {
                LOG.warn(s"invalid path:${status.getPath}");
            }
        }
        LOG.info("task completed.")
    }

}
