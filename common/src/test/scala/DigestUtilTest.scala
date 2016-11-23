import cn.whaley.bi.logsys.common.{NumberUtil, DigestUtil}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/11/8.
 */
class DigestUtilTest {

    @Test
    def testBase64: Unit = {
        //0000015842499322|7f00000118477ed2|0001000000000000 -> AAABWEJJkyJ/AAABGEd+0gAB
        val v1 = NumberUtil.longToByte8(0x0000015842499322L)
        val v2 = NumberUtil.longToByte8(0x7f00000118477ed2L)
        val v3 = NumberUtil.unsignedShortToByte2(0x0001)

        val byteBuffer = new ArrayBuffer[Byte]()
        byteBuffer.append(v1: _*)
        byteBuffer.append(v2: _*)
        byteBuffer.append(v3: _*)

        println(DigestUtil.getBase64Str(byteBuffer.toArray))
    }

    @Test
    def testMd5: Unit = {
        val md5 = DigestUtil.getMD5Str32("dsdsdsdsd")
        println(md5)
    }
}
