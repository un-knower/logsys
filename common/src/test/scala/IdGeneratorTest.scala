
import java.util.concurrent.{CountDownLatch, TimeUnit, Executors}

import cn.whaley.bi.logsys.common.IdGenerator
import org.junit.Test

/**
 * Created by fj on 16/11/4.
 */
class IdGeneratorTest {

    @Test
    def testNextId: Unit = {
        val generator = IdGenerator.defaultInstance
        println(generator.nextId())
        println(generator.nextId())
        println(generator.nextId())
        println(generator.nextId())
    }

    @Test
    def testNextId2: Unit = {
        val generator = IdGenerator.defaultInstance
        val threadCount = 20
        val testCount = 1000000
        val threadTestCount = testCount / threadCount
        val latch = new CountDownLatch(threadCount)
        val executor = Executors.newFixedThreadPool(threadCount)
        val idArrays = new Array[String](testCount)

        val from = System.currentTimeMillis()

        for (j <- 0 to threadCount - 1) {
            val runnable = new Runnable {
                override def run(): Unit = {
                    for (i <- 0 to threadTestCount - 1) {
                        val id = generator.nextId()
                        val index = j * threadTestCount + i
                        idArrays(index) = id
                    }
                    latch.countDown()
                }
            }
            executor.submit(runnable)
        }
        executor.shutdown()
        executor.awaitTermination(10, TimeUnit.SECONDS)

        latch.await()

        val keyCount = idArrays.distinct.length

        println(s"ts:${(System.currentTimeMillis() - from) / 1000},idCount:${idArrays.length},keyCount:${keyCount}")

        require(keyCount == testCount)


    }

}
