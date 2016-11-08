import java.io.FileInputStream

import cn.whaley.bi.logsys.common.DigestUtil
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class MsgSignTest {
    @Test
    def testMd5Sign: Unit = {
        val path = "/Users/fj/workspace/whaley/projects/WhaleyLogSys/gathersys/src/log_nginx/test/data.json"
        val key = "92DOV+sOk160j=430+DM!ZzESf@XkEsn#cKanpB$KFB6%D8z4C^xg7cs6&7wn0A4A*iR9M3j)pLs]ll5E9aFlU(dE0[QKxHZzC" +
            ".CaO/2Ym3|Tk<YyGZR>WuRUmI?x2s:Cg;YEA-hZubmGnWgXE"
        val ts = "123456"


        val read = new FileInputStream(path)
        val byteBuffer = new ArrayBuffer[Byte]
        byteBuffer.append(ts.getBytes(): _*)
        var byte = 0
        while (byte != -1) {
            byte = read.read()
            if (byte != -1) {
                byteBuffer.append(byte.toByte)
            }
        }
        byteBuffer.append(key.getBytes(): _*)

        val md5 = DigestUtil.getMD5Str32(byteBuffer.toArray)

        println(md5)

    }

    @Test
    def testMd5Sign2: Unit = {
        val key = "92DOV+sOk160j=430+DM!ZzESf@XkEsn#cKanpB$KFB6%D8z4C^xg7cs6&7wn0A4A*iR9M3j)pLs]ll5E9aFlU(dE0[QKxHZzC" +
            ".CaO/2Ym3|Tk<YyGZR>WuRUmI?x2s:Cg;YEA-hZubmGnWgXE"
        val ts = "123456"
        val body = "本次分享将首先对Apache Kylin进行基本介绍；接下来介绍1.5.x最"
        val verificationStr = s"$ts$body$key"

        val md5 = DigestUtil.getMD5Str32(verificationStr)

        println(md5)

    }

    @Test
    def testX: Unit = {
        println(System.currentTimeMillis().toHexString)
        val ts = 1478509910027L.toInt
        println(ts.toHexString)

        println((ts >> 58).toHexString)
        println((ts >> 52).toHexString)
    }
}