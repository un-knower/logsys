package cn.whaley.bi.logsys.common

import java.security.MessageDigest

/**
 * Created by fj on 16/11/2.
 * 签名实用类
 */
class DigestUtil {

}

object DigestUtil {
    private val hex: Array[Byte] = "0123456789abcdef".getBytes();

    /**
     * 获取value值的32位MD值
     * @param value
     * @return
     */
    def getMD5Str32(value: String): String = {
        val digest = MessageDigest.getInstance("md5");
        val md5 = digest.digest(value.getBytes())
        val md5Str32 = Bytes2HexString(md5)
        md5Str32
    }

    /**
     * 获取value值的16位MD值
     * @param value
     * @return
     */
    def getMD5Str16(value: String): String = {
        val md5Str32 = getMD5Str32(value)
        md5Str32.substring(8, 24)
    }

    /**
     * 获取字节数组的十六进字符串表示
     * @param b
     * @return
     */
    def Bytes2HexString(b: Array[Byte]): String = {
        val buff = new Array[Byte](2 * b.length);
        for (i <- 0 to b.length - 1) {
            buff(2 * i) = hex((b(i) >> 4) & 0x0f);
            buff(2 * i + 1) = hex(b(i) & 0x0f);
        }
        new String(buff);
    }
}