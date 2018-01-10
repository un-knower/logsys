package cn.whaley.bi.logsys.forest.sinker

import java.net.Socket
import java.util
import java.util.Date

import cn.whaley.bi.logsys.common.{ConfManager, KafkaUtil}
import cn.whaley.bi.logsys.forest.{DBOperationUtils, ProcessResult}
import cn.whaley.bi.logsys.forest.Traits.{InitialTrait, LogTrait, NameTrait}
import cn.whaley.bi.logsys.forest.entity.LogEntity
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable
import collection.JavaConversions._

/**
 * Created by lituo on 17/12/14.
 */
class ConfigurableKafkaMsgSink extends MsgSinkTrait with InitialTrait with NameTrait with LogTrait {

    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager): Unit = {

        val kafkaConf = confManager.getAllConf("kafka-producer", true)

        //实例化kafka实用工具
        bootstrapServers = kafkaConf.get("bootstrap.servers").toString
        InitKafkaUtil()
        if (kafkaUtil == null) {
            throw new Exception(s"invalid broker servers : ${bootstrapServers}")
        }

        //实例化kafka生产者
        kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](kafkaConf)

        //是否保存错误数据
        saveErrorData = confManager.getConfOrElseValue(this.name, "saveErrorData", "true").toBoolean

        dBOperationUtils = new DBOperationUtils("streaming")

        startConfigUpdateThread()

    }

    /**
     * 停止服务
     */
    override def stop(): Unit = {
        if (kafkaProducer != null) {
            kafkaProducer.close()
        }
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
     * 从目标kafka集群中，获取某个源topic最后写入的offset信息
     * @param sourceTopic 源topic
     * @param sourceLatestOffset 源topic的最后偏移信息
     * @param maxMsgCount 目标topic的每个partition最多读取的消息数量
     * @return 源topic相关的offse信（partition，offset），如果没有则返回空Map对象
     */
    override def getTopicLastOffset(sourceTopic: String, sourceLatestOffset: Map[Int, Long], maxMsgCount: Int): Map[Int, Long] = {
        val offsetMap = new mutable.HashMap[Int, Long]
        offsetMap.toMap
    }


    /**
      * 保存监控数据
      *
      * @param monitorInfo 监控数据
      */
    override def saveMonitorInfo(monitorInfo: JSONObject): Unit = {
        val key: Array[Byte] = monitorInfo.getString("time").getBytes()
        val value: Array[Byte] = monitorInfo.toJSONString.getBytes()
        val targetTopic: String = "forest-monitor"
        val record = new ProducerRecord[Array[Byte], Array[Byte]](targetTopic, key, value)
        kafkaProducer.send(record)
    }

    /**
     * 保存正常数据
      *
      * @param datas
     */
    private def saveSuccess(datas: Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])]): Int = {

        var count = 0
//        var errorCount = 0
//        var output = false

        val items = datas.flatMap(data => {
            data._2.result.get.map(log => {
                val topics = filterConfig.values.toArray
                  .filter(c => log.appId.contains(c._1) && isOK(log.logBody, c._3)).map(_._2)
//                if(log.logBody.get("realLogType") == null) {
//                    errorCount = errorCount + 1
//                    if(!output) {
//                        LOG.info(log.logBody.toString)
//                        output = true
//                    }
//                }
                (data._1, log.logBody, topics)  //只获取logBody
            }).filter(_._3.length > 0)
        })
//        LOG.info("没有realLogType条数" + errorCount)
        val produceTime = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")

        items.foreach(item => {
            val message: KafkaMessage = item._1
            val log: JSONObject = item._2
            log.put("forestProduceTime", produceTime)
            val key: Array[Byte] = getKeyFromSource(message).getBytes()
            val value: Array[Byte] = log.toJSONString.getBytes()
            item._3.foreach(item_topic => {
                val targetTopic: String = item_topic
                val record = new ProducerRecord[Array[Byte], Array[Byte]](targetTopic, key, value)
                kafkaProducer.send(record)
            })
            count = count + 1
        })

        count
    }


    /**
     * 保存错误数据
     * @param datas
     */
    private def saveError(datas: Seq[(KafkaMessage, ProcessResult[Seq[LogEntity]])]): Int = {
        var count = 0
        if (saveErrorData) {
            datas.foreach(item => {
                val message: KafkaMessage = item._1
                val syncInfo = buildSyncInfo(item._1)
                val errData = buildErrorData(item._1, item._2)
                val errorTopic: String = getErrTopic(message.topic())

                val key: Array[Byte] = syncInfo.toJSONString.getBytes()
                val value: Array[Byte] = errData.toJSONString.getBytes()
                val record = new ProducerRecord[Array[Byte], Array[Byte]](errorTopic, key, value)
                kafkaProducer.send(record)
                count = count + 1
            })
        }
        count
    }

    private def isOK(target: JSONObject, filter: JSONObject): Boolean = {
        var ok = true
        if (filter != null && filter.keySet().size() > 0) {
            val it = filter.keySet().iterator()
            while (it.hasNext && ok) {
                val key = it.next()
                val targetObj = target.get(key)
                val filterObj = filter.get(key)
                if (filterObj.isInstanceOf[JSONObject]) {
                    if (targetObj != null && targetObj.isInstanceOf[JSONObject]) {
                        ok = ok && isOK(targetObj.asInstanceOf[JSONObject], filterObj.asInstanceOf[JSONObject])
                    } else {
                        ok = false
                    }
                } else {
                    ok = ok && target.containsKey(key) && filter.get(key).equals(target.get(key))
                }
            }
        }
        ok
    }


    private def getErrTopic(sourceTopic: String): String = {
        s"err-${sourceTopic}"
    }


    private def getKeyFromSource(source: KafkaMessage): String = {
        val keyObj = new JSONObject()
        keyObj.put("rawTs", source.timestamp())
        keyObj.put("rawParId", source.partition())
        keyObj.put("rawTopic", source.topic())
        keyObj.put("rawOffset", source.offset())
        keyObj.put("oriTs", System.currentTimeMillis())
        keyObj.toJSONString
    }

    private def getOffsetInfoFromKey(key: String, sourceTopic: String): Option[(Int, Long)] = {
        try {
            val keyObj = JSON.parseObject(key)
            if (keyObj.getString("rawTopic").equals(sourceTopic)) {
                val parId = keyObj.getIntValue("rawParId")
                val offset = keyObj.getLongValue("rawOffset")
                Some((parId, offset))
            } else {
                None
            }
        }
        catch {
            case ex: Throwable => {
                ex.printStackTrace()
                None
            }
        }
    }

    private def InitKafkaUtil() = {
        val array = bootstrapServers.split(",")
        val brokerList =
            for (i <- 0 to array.length - 1) yield {
                val hostAndPort = array(i).split(":")
                try {
                    //进行一次网络测试
                    new Socket(hostAndPort(0), hostAndPort(1).toInt)
                    Some((hostAndPort(0), hostAndPort(1).toInt))
                }
                catch {
                    case e: Throwable => {
                        LOG.error(s"broker is invalid:${array(i)},test failure:${e.getMessage},${e.getCause}")
                        None
                    }
                }
            }
        val list = brokerList.filter(_.isDefined).map(_.get)
        require(!list.isEmpty)
        kafkaUtil = new KafkaUtil(list)
    }

    private def startConfigUpdateThread(): Unit = {

        val thread = new Thread(new Runnable {
            override def run(): Unit = {
                val sqlPrefix = "select * from kafka_topic_distribute where updateTime > "
                while (true) {
                    val date = new Date()
                    try {
                        val sql = sqlPrefix + "'" + DateFormatUtils.format(configLastUpdateTime, "yyyy-MM-dd HH:mm:ss") + "'"
                        val result = dBOperationUtils.selectMapList(sql)
                        if(result != null && result.size() > 0) {
                            updateConfig(result)
                            configLastUpdateTime = date
                            LOG.info("配置更新完成")
                        }
                        Thread.sleep(10 * 1000)
                    } catch {
                        case e: Exception =>
                            LOG.error("读取配置数据库失败，配置最后更新时间：" + configLastUpdateTime, e)
                    }
                }
            }
        })

        thread.start()
    }

    private def updateConfig(newConfig: util.List[util.Map[String, Object]]): Unit = {
        if(newConfig == null || newConfig.size() == 0) {
            return
        }
        newConfig.foreach(c => {
            val id = c.get("id").asInstanceOf[Number].intValue()
            if(!c.get("status").asInstanceOf[Int].equals(1)) {
                filterConfig.remove(id)
                LOG.info("从配置中删除项，id=" + id)
            } else {
                val filter = try {
                    JSON.parseObject(c.get("filter").asInstanceOf[String])
                } catch {
                    case e: Exception =>
                        LOG.error("过滤条件解析json失败, id=" + id)
                        null
                }
                if(filter != null) {
                    val config = (c.get("appIdPrefix").asInstanceOf[String],
                      c.get("destTopic").asInstanceOf[String],
                      filter)
                    filterConfig.put(id, config)
                    LOG.info("新增或修改配置项，id=" + id)

                }
            }
        })
    }

    private var kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]] = null
    private var kafkaUtil: KafkaUtil = null
    private var bootstrapServers: String = null
//    private var targetTopicPrefix = "log-origin"
    //日志过滤器,LogEntity应该是logFilter的一个超集,不能存在属性值不一致
//    private var logFilter: JSONObject = null
    private var saveErrorData: Boolean = true
    private val filterConfig = new mutable.HashMap[Int, (String, String, JSONObject)]
    private var dBOperationUtils: DBOperationUtils = null
    private var configLastUpdateTime: Date = new Date(0)

//
}

object ConfigurableKafkaMsgSink {
    def main(args: Array[String]): Unit = {
        val test = new ConfigurableKafkaMsgSink
        test.dBOperationUtils = new DBOperationUtils("streaming")
        test.startConfigUpdateThread()
    }

}
