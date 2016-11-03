import org.junit.Test

/**
 * Created by fj on 16/10/27.
 */
class JSONTest extends SqlContextTrait {

    @Test
    def test1: Unit = {
        val path = this.getClass.getClassLoader.getResource("data.json").getPath
        val df = sqlContext.read.json(path)
        df.printSchema()
    }
}
