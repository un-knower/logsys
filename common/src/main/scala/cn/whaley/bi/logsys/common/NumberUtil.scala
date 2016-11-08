package cn.whaley.bi.logsys.common

/**
 * Created by fj on 16/11/8.
 */
class NumberUtil {

}

object NumberUtil {

    /**
     * int整数转换为4字节的byte数组
     *
     * @param i
     * 整数
     * @return byte数组
     */
    def intToByte4(i: Int): Array[Byte] = {
        val targets = new Array[Byte](4);
        targets(3) = (i & 0xFF).toByte;
        targets(2) = (i >> 8 & 0xFF).toByte;
        targets(1) = (i >> 16 & 0xFF).toByte;
        targets(0) = (i >> 24 & 0xFF).toByte;
        targets;
    }

    /**
     * long整数转换为8字节的byte数组
     *
     * @param lo
     * long整数
     * @return byte数组
     */
    def longToByte8(lo: Long): Array[Byte] = {
        val targets = new Array[Byte](8);
        for (i <- 0 to 7) {
            val offset = (targets.length - 1 - i) * 8;
            targets(i) = ((lo >>> offset) & 0xFF).toByte;
        }
        return targets;
    }

    /**
     * short整数转换为2字节的byte数组
     *
     * @param s
     * short整数
     * @return byte数组
     */
    def unsignedShortToByte2(s: Int): Array[Byte] = {
        val targets = new Array[Byte](2);
        targets(0) = (s >> 8 & 0xFF).toByte;
        targets(1) = (s & 0xFF).toByte;
        return targets;
    }

    /**
     * byte数组转换为无符号short整数
     *
     * @param bytes
     * byte数组
     * @return short整数
     */
    def byte2ToUnsignedShort(bytes: Array[Byte]): Int = {
        return byte2ToUnsignedShort(bytes, 0);
    }

    /**
     * byte数组转换为无符号short整数
     *
     * @param bytes
     * byte数组
     * @param off
     * 开始位置
     * @return short整数
     */
    def byte2ToUnsignedShort(bytes: Array[Byte], off: Int): Int = {
        val high = bytes(off);
        val low = bytes(off + 1);
        return (high << 8 & 0xFF00) | (low & 0xFF);
    }

    /**
     * byte数组转换为int整数
     *
     * @param bytes
     * byte数组
     * @param off
     * 开始位置
     * @return int整数
     */
    def byte4ToInt(bytes: Array[Byte], off: Int): Int = {
        val b0 = bytes(off) & 0xFF;
        val b1 = bytes(off + 1) & 0xFF;
        val b2 = bytes(off + 2) & 0xFF;
        val b3 = bytes(off + 3) & 0xFF;
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }
}
