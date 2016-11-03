import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
 * Created by fj on 16/10/27.
 */
trait SqlContextTrait {

    lazy val sqlContext: SQLContext = {
        val conf = new SparkConf()
        conf.setMaster("local").setAppName(this.getClass.getSimpleName)
        val sparkContext = new SparkContext(conf)
        new SQLContext(sparkContext)
    }
}
