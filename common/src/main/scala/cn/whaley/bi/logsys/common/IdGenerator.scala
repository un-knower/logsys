package cn.whaley.bi.logsys.common

import java.lang.management.ManagementFactory
import java.net.{InetAddress, NetworkInterface}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer


/**
 * Id生成器,该类是线程安全的
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
        val prefix = this.getNextPrefix()
        val seq = this.getNextSeqStr()
        s"${prefix}${seq}"
    }

    /**
     * 获取下一个Id值的前缀值
     * @return
     */
    def getNextPrefix(): String = {
        val currSec = System.currentTimeMillis()  //当前系统时间戳
        val ipAddress = IdGenerator.localNetworkAddress.getAddress //IP地址
        val processId = IdGenerator.currProcessId //进程号

        val part0 = IdGenerator.fixLeftLen("%X".format(currSec), '0', 12)
        val part1 = (IdGenerator.fixLeftLen("%X".format(ipAddress(0)), '0', 2)
            + IdGenerator.fixLeftLen("%X".format(ipAddress(1)), '0', 2)
            + IdGenerator.fixLeftLen("%X".format(ipAddress(2)), '0', 2)
            + IdGenerator.fixLeftLen("%X".format(ipAddress(3)), '0', 2)
            )
        val part2 = IdGenerator.fixLeftLen("%X".format(processId), '0', 4)
        s"${part0}${part1}${part2}"
    }

    /**
     * 获取下一个Id值的序列值
     * @return
     */
    def getNextSeqStr(): String = {
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
        IdGenerator.fixLeftLen("%X".format(nextValue), '0', 8)
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
        val name: String = ManagementFactory.getRuntimeMXBean.getName
        val pid: Integer = name.substring(0, name.indexOf("@")).toInt
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
