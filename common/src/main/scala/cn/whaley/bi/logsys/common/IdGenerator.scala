package cn.whaley.bi.logsys.common

import java.lang.management.ManagementFactory
import java.net.{InetAddress, NetworkInterface}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer


/**
 * Id生成器,按照分布式算法产生全局唯一的字符串，该类是线程安全的
 *
 * @param seed 种子值
 */
class IdGenerator(seed: Int) {

    private val atomicSeq = new AtomicLong(seed)

    def this() = {
        this(0)
    }

    /**
     * 下一个Id值
     * @return
     */
    def nextId(): String = {

        val currSec = System.currentTimeMillis() //当前系统时间戳
        val ipAddress = IdGenerator.localNetworkAddress.getAddress //IP地址
        val processId = IdGenerator.currProcessId //进程号

        //下一个序列值，最大为Int.MaxValue,达到最大值后，重新从0开始编号
        val seq = atomicSeq.getAndIncrement()
        val nextValue = if (seq <= Int.MaxValue) {
            seq.toInt
        } else {
            var isReInit = false;
            while (!isReInit) {
                val curr = atomicSeq.get()
                val newSeed = (curr % Int.MaxValue)
                isReInit = atomicSeq.compareAndSet(curr, newSeed)
            }
            atomicSeq.getAndIncrement().toInt
        }

        //18字节的Base64编码（24个字符）
        val bytes = new ArrayBuffer[Byte]()
        bytes.append(NumberUtil.longToByte8(currSec): _*)
        bytes.append(ipAddress: _*)
        bytes.append(NumberUtil.unsignedShortToByte2(processId): _*)
        bytes.append(NumberUtil.intToByte4(nextValue): _*)

        DigestUtil.getBase64Str(bytes.toArray)

    }
}

object IdGenerator {

    /**
     * 全局默认实例
     * @return
     */
    lazy val defaultInstance = new IdGenerator(0)

    /**
     * 获取当前进程ID
     * @return
     */
    lazy val currProcessId: Int = {
        val name = ManagementFactory.getRuntimeMXBean.getName
        val pid = name.substring(0, name.indexOf("@")).toInt
        pid
    }

    /**
     * 获取当前机器ip地址
     */
    lazy val localNetworkAddress: InetAddress = {
        val netInterfaces = NetworkInterface.getNetworkInterfaces
        var ip: InetAddress = null
        while (netInterfaces.hasMoreElements) {
            val ni: NetworkInterface = netInterfaces.nextElement
            val addresses = ni.getInetAddresses
            while (addresses.hasMoreElements) {
                val next = addresses.nextElement
                if (!next.isLoopbackAddress && next.getHostAddress.indexOf(':') == -1) {
                    ip = next
                }
            }
        }
        ip
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
