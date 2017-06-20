package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.traits.{InitialTrait, LogTrait, NameTrait}
import cn.whaley.bi.logsys.log2parquet.utils.PathUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.alibaba.fastjson.JSON

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
        rdd_original.map(line=>{
            //
            val json = JSON.parseObject(line)
           // processorChain.process(line)

        })

        //.filter(_ != null).flatMap(x => x)

        val outputPath=PathUtil.getOdsViewPath(appID, startDate, startHour, "logType", "eventID")

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
