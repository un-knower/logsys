package cn.whaley.bi.logsys.log2parquet

import java.io.File
import java.util.Date

import cn.whaley.bi.logsys.log2parquet.constant.Constants
import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * Created by fj on 16/11/20.
 */
class MainObjTest extends LogTrait{

    /*
     *  * MsgProcExecutor --c inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13
*/
    @Test
    def testMainObjTest: Unit = {
        val args = Array("MsgProcExecutor","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13/boikgpokn78sb95ktmsc1bnkechpgj9l_2017061413_raw_7_575892351.json.gz","--c","masterURL=local[2]")
         MainObj.main(args)

    }

    @Test
    def testSparkSession(): Unit ={
        val inputPath="/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13/boikgpokn78sb95ktmsc1bnkechpgj9l_2017061413_raw_7_575892351.json.gz"
        val config = new SparkConf()
        config.setMaster("local[2]")
        val sparkSession: SparkSession = SparkSession.builder().config(config).getOrCreate()
        val rdd=sparkSession.sparkContext.textFile(inputPath, 2)
        println(rdd.count())
    }

    @Test
    def testVar(): Unit ={
        val date=new Date
        //time字段，用来多个azkaban任务一起运行时，区分不同任务写入的目录
        val time=date.getTime
        val outputPathTmp = s"${Constants.ODS_VIEW_HDFS_OUTPUT_PATH_TMP}${File.separator}${time}"
        println("outputPathTmp:"+outputPathTmp)
    }

}
