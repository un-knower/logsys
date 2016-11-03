import org.junit.Test

/**
 * Created by fj on 16/11/1.
 */
class LogDataTest {

    @Test
    def test1: Unit = {
        val data1 = getCheckDirData("medusa-processed-log")
        val data2 = getCheckDirData("medusa-pre-log")

        for (i <- 0 to data1.length - 1) {
            if (i < data2.length) {
                println(s"${data1(i)},${data2(i)}")
            } else {
                println(s"${data1(i)}, }")
            }
        }
    }

    def getCheckDirData(filter: String): Array[(Long,Long)] = {
        val data = scala.io.Source.fromFile("/Users/fj/check_dir.log").getLines()
        val sizes = data.filter(item => item.indexOf(filter) > 0)
            .map(item => {
            val fs = item.split("\t")
            fs(fs.length - 1).toLong / 1024 / 1024
        }).toArray
        val arrs = for (i <- 0 to sizes.length - 1) yield {
            if (i > 0) {
                (sizes(i), sizes(i) - sizes(i - 1))
            } else {
                (sizes(i), 0L)
            }
        }
        arrs.toArray
    }

    def printLogInfo(filter: String): Unit = {
        val data = scala.io.Source.fromFile("/Users/fj/check_dir.log").getLines()
        val sizes = data.filter(item => item.indexOf(filter) > 0)
            .map(item => {
            val fs = item.split("\t")
            fs(fs.length - 1).toLong / 1024 / 1024
        }).toArray
        for (i <- 0 to sizes.length - 1) {
            if (i > 0) {
                println(s"${sizes(i)} : ${sizes(i) - sizes(i - 1)}")
            } else {
                println(s"${sizes(i)} : 0")
            }
        }
    }
}
