import org.junit.Test

/**
 * Created by fj on 2017/6/29.
 */
class RegexTest {

    @Test
    def test1(): Unit ={
        val patten=  java.util.regex.Pattern.compile("userId",java.util.regex.Pattern.CASE_INSENSITIVE)
        val r=new scala.util.matching.Regex(patten)
        println(patten.matcher("userid").find())
    }
}
