package cn.whaley.bi.traits

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by fj on 16/9/28.
 *
 * 可实例化SQLContext的公共特质
 */
trait SQLContextTrait {
    /**
     * 获取SQLContext实例
     * @return
     */
    val sqlContext: SQLContext = {
        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sparkCtx = new SparkContext(conf)
        new SQLContext(sparkCtx)
    }
}
