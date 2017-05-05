import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.forest.GenericProcessorChain
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/11/17.
 */
class GenericProcessorChainTest {

    @Test
    def testProcessor: Unit = {
        val chain = new GenericProcessorChain()
        val confManager = new ConfManager(Array("GenericProcessorChain.xml"))
        chain.init(confManager)


        val stream = this.getClass.getClassLoader.getResourceAsStream("boikgpokn78sb95kjhfrendoj8ilnoi7.log")
        val source = scala.io.Source.fromInputStream(stream)
        val filelines = source.getLines().toArray
        val lines = new ArrayBuffer[String]()
        for (i <- 0 to 0) {
            lines.append(filelines: _*)
        }

        val from = System.currentTimeMillis()

        for (i <- 0 to 0) {

            val from2 = System.currentTimeMillis()

            val result =
                lines.flatMap(item => {
                    val bytes = item.getBytes
                    val ret = chain.process(bytes)
                    if (ret.hasErr) {
                        if (ret.ex.isDefined) {
                            ret.ex.get.printStackTrace()
                        }
                    }
                    require(ret.hasErr == false)

                    //println(ret.result.get.length)

                    ret.result.get.foreach(item=>println(item.toJSONString))
                    println("")

                    ret.result.get

                })

            println(s"${i}:ts:${System.currentTimeMillis() - from2},${result.length}")

        }

        println(s"${lines.length}:ts:${System.currentTimeMillis() - from}")

    }
}
