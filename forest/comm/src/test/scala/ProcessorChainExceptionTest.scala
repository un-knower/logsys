import cn.whaley.bi.logsys.forest.{ProcessResultCode, ProcessResult, ProcessorChainException}
import org.junit.Test

/**
 * Created by fj on 16/11/25.
 */
class ProcessorChainExceptionTest {

    @Test
    def testProcessorChainException: Unit ={
        val result=Array(new ProcessResult("test",ProcessResultCode.break,"",Some("dsfd")))
        val ex=new ProcessorChainException(("",""),result)

        require(ex.isInstanceOf[ProcessorChainException[AnyRef]])

        println("test completed")
    }
}
