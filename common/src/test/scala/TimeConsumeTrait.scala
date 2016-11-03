/**
 * Created by fj on 16/10/30.
 */
trait TimeConsumeTrait {

    def timeConsumeTest(times: Int, f: () => Unit): Long = {
        val from = System.currentTimeMillis()
        for (i <- 1 to times) {
            f()
        }
        System.currentTimeMillis() - from
    }

}
