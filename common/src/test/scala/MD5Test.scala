import java.io.FileInputStream
import java.security.MessageDigest

import org.junit.Test

import scala.Predef
import scala.collection.mutable.ArrayBuffer

/**
 * Created by fj on 16/10/31.
 */
class MD5Test {


    @Test
    def testMd5: Unit = {
        val digest = MessageDigest.getInstance("md5");
        val read = new FileInputStream("/workspace/data/data.json")
        val byteBuffer = new ArrayBuffer[Byte]
        byteBuffer.append("123456".getBytes():_*)
        var byte=0
        while (byte != -1) {
            byte=read.read()
            if(byte != -1){
                byteBuffer.append(byte.toByte)
            }
        }
        val md5 = digest.digest(byteBuffer.toArray)
        println(Bytes2HexString(md5))
        //1389c88ac3e71734fb4c016193ea39e0
        //1389c88ac3e71734fb4c016193ea39e0
        //ab -T 'text/plain' -p /workspace/data/data.json -c 1 -n 1 -H 'ts: 123456' http://localhost:8081/log4

        //val str=new String(byteBuffer.toArray,"utf-8")
        //val data="本次分享将首先对Apache Kylin进行基本介绍；接下来介绍1.5.x最".getBytes //776bc7629688c04baab6b4dc38308bc0
        //val md5 = digest.digest(data)

    }

    val hex: Array[Byte] = "0123456789abcdef".getBytes();

    def Bytes2HexString(b: Array[Byte]): String = {
        val buff = new Array[Byte](2 * b.length);
        for (i <- 0 to b.length - 1) {
            buff(2 * i) = hex((b(i) >> 4) & 0x0f);
            buff(2 * i + 1) = hex(b(i) & 0x0f);
        }
        new String(buff);
    }

}
