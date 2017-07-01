package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.log2parquet.traits.LogTrait
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * Created by fj on 16/11/20.
 */
class MainObjTest extends LogTrait{

    /*
     *  * MsgProcExecutor --c prop.inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13
*/
    @Test
    def testMsgProce: Unit = {
        val args = Array("MsgProcExecutor","--c","inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13/boikgpokn78sb95ktmsc1bnkechpgj9l_2017061413_raw_7_575892351.json.gz")
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

}
