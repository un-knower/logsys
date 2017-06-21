package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.entity.LogEntity
import cn.whaley.bi.logsys.log2parquet.traits.{InitialTrait, LogTrait, NameTrait}
import cn.whaley.bi.logsys.log2parquet.utils.PathUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.alibaba.fastjson.{JSON, JSONObject}
/**
 * Created by fj on 16/10/30.
 */
class MsgBatchManager extends InitialTrait with NameTrait with LogTrait {

    private var processorChain: GenericProcessorChain = null
    private val config = new SparkConf()
    private var sparkSession:SparkSession=null
    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    override def init(confManager: ConfManager = new ConfManager(Array("MsgBatchManager.xml"))): Unit = {
        val processorChainName = confManager.getConf(this.name, "processorChain")
        processorChain = instanceFrom(confManager, processorChainName).asInstanceOf[GenericProcessorChain]
        sparkSession=SparkSession.builder().config(config).getOrCreate()
    }

    /**
     * 启动
     */
    def start(): Unit = {
        //加载数据
        val appID=""
        val startDate=""
        val startHour=""
        val partition=2000

        val odsPath = PathUtil.getOdsOriginPath(appID,startDate, startHour)
        val rdd_original = sparkSession.sparkContext.textFile(odsPath, partition)
        //按logType和eventId分类
        val logRdd = rdd_original.map(line=>{
            val json = string2JsonObject(line)
            //processorChain.process(new LogEntity(json))
            // change to  (keyType, json)
        })

        //.filter(_ != null).flatMap(x => x)

        val outputPath=PathUtil.getOdsViewPath(appID, startDate, startHour, "logType", "eventID")

    }

    def string2JsonObject(log:String):JSONObject = {
        try{
             val json = JSON.parseObject(log)
             json
        }catch {
            case e:Exception => {
                 e.printStackTrace()
                null
            }
        }
    }

    /**
     * 关停
     */
    def shutdown(): Unit = {
        if (sparkSession != null) {
            sparkSession.close()
        }
    }
}
