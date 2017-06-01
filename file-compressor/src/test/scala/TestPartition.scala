import cn.whaley.bi.logsys.filecompressor.SparkTask

import org.junit.Test
import org.apache.spark.{ SparkConf, SparkContext}


/**
 * Created by fj on 17/5/31.
 */
class TestPartition {

    @Test
    def test1():Unit={
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[4]")
        sparkConf.setAppName("test")
        val context = new SparkContext(sparkConf)
        val srcFilePaths=Array(("n1","f1"),("n2","f2"),("n3","f3"));
        val rdd = context.makeRDD(srcFilePaths)

        rdd.partitionBy(new SparkTask.TaskPartitioner(srcFilePaths.map(_._2))).foreachPartition(par=>{
            par.foreach(print(_))
        })
    }

}
