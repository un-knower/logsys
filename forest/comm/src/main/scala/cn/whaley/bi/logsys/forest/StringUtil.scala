package cn.whaley.bi.logsys.forest

import java.net.URLDecoder

import com.alibaba.fastjson.JSONObject

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


/**
 * Created by fj on 16/11/8.
 *
 * Nginx日志内容解析实用类
 */
object StringUtil {

    private def isHexByte(chr: Byte): Boolean = {
        ((chr >= 65 && chr <= 70)
            || (chr >= 97 && chr <= 102)
            || (chr >= 48 && chr <= 57)
            )
    }


    private def hexByteToByte(char: Byte): Byte = {
        //A=65,F=70,a=97,f=102,0=48,9=57
        char match {
            case 65 => 10
            case 66 => 11
            case 67 => 12
            case 68 => 13
            case 69 => 14
            case 70 => 15
            case 97 => 10
            case 98 => 11
            case 99 => 12
            case 100 => 13
            case 101 => 14
            case 102 => 15
            case 48 => 0
            case 49 => 1
            case 50 => 2
            case 51 => 3
            case 52 => 4
            case 53 => 5
            case 54 => 6
            case 55 => 7
            case 56 => 8
            case 57 => 9
        }
    }

    private def pairToByte(high: Byte, low: Byte): Byte = {
        (hexByteToByte(high) * 16 + hexByteToByte(low)).toByte
    }

    /**
     * 将一个字符串以特定正则表达式进行分隔，且过滤掉空字符串，去除分隔后的字符串中的换行回车，以及首位空格
     * @param str
     * @return
     */
    def splitStr(str: String, splitRex: String): Seq[String] = {
        str.split(splitRex)
            .map(item => item.trim.replace("\n", "").replace("\r", ""))
            .filter(item => item.length > 0)
    }

    /**
     * 解析nginx编码字符串为字符串对象
     * @param str
     * @return
     */
    def decodeNgxStrToString(str: String): String = {
        val bytes = decodeNgxStrToBytes(str)
        val values = new String(bytes, "UTF-8")

        values
    }

    /**
     * 解析nginx编码字符对象为字节UTF-8数组
     * @param str
     * @return
     */
    def decodeNgxStrToBytes(str: String): Array[Byte] = {
        var buf = new Array[Byte](str.length)
        var p = 0
        val len = str.length
        val end = len - 3
        var skip = 0
        var index = -1
        val bytes = str.getBytes
        for (i <- 0 to len - 1) {
            if (skip > 0) {
                skip = skip - 1
            } else {
                if (i < end && bytes(i) == '\\') {
                    if (bytes(i + 1) == 'x'
                        && isHexByte(bytes(i + 2))
                        && isHexByte(bytes(i + 3))) {
                        index = index + 1
                        buf(index) = pairToByte(bytes(i + 2), bytes(i + 3))
                        skip = 3
                    }
                } else {
                    index = index + 1
                    buf(index) = bytes(i)
                    skip = 0
                }
            }
        }
        buf.slice(0, index + 1)

    }


    /**
     * 将指定字符串左侧填充或左侧截取到指定固定长度
     * @param value
     * @param pad 不足指定长度时的填充字符
     * @param len 指定长度
     * @return
     */
    def fixLeftLen(value: String, pad: Char, len: Int): String = {
        val v = if (value.length < len) {
            val buf = new ArrayBuffer[Char]
            for (i <- 1 to len - value.length) {
                buf.append(pad)
            }
            new String(buf.toArray) + value
        } else {
            value
        }
        v.substring(v.length - len, len)
    }


}


