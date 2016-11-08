package cn.whaley.bi.logsys.common


/**
 * Created by fj on 16/11/2.
 *
 * AppId实用类
 * @param orgCodeNoise 作为噪音与组织代码一起参与哈希计算
 * @param productCodeNoise 作为噪音与产品线代码一起参与哈希计算
 * @param appCodeNoise 作为噪音与应用代码一起参与哈希计算
 */
class AppIdUtil(orgCodeNoise: String, productCodeNoise: String, appCodeNoise: String) {
    def this() = {
        this("", "", "")
    }

    /**
     * 产生一个32位字节长度appId
     * @param orgCode 组织代码
     * @param productCode 产品线代码（最多8位）
     * @param appCode 应用代码(最大8位）
     * @return
     */
    def createAppId(orgCode: String, productCode: String, appCode: String): String = {
        s"${hashCodeForOrgId(orgCode)}${hashCodeForProductCode(productCode)}${hashCodeForAppCode(appCode)}"
    }

    /**
     * 组织代码的16位长度哈希字符串
     * @param orgId
     */
    private def hashCodeForOrgId(orgId: String): String = {
        val md532 = DigestUtil.getMD5Str32(orgId + orgCodeNoise)
        val part1 = AppIdUtil.hashCode16To8(md532.substring(0, 16))
        val part2 = AppIdUtil.hashCode16To8(md532.substring(16, 32))
        part1 + part2
    }

    /**
     * 产品线代码的8位长度哈希字符串
     * @param productCode
     * @return
     */
    private def hashCodeForProductCode(productCode: String): String = {
        val hashCode16 = DigestUtil.getMD5Str16(productCode + productCodeNoise)
        AppIdUtil.hashCode16To8(hashCode16)
    }

    /**
     * 应用代码的8位长度哈希字符串
     * @param appCode
     * @return
     */
    private def hashCodeForAppCode(appCode: String): String = {
        val hashCode16 = DigestUtil.getMD5Str16(appCode + appCodeNoise)
        AppIdUtil.hashCode16To8(hashCode16)
    }


}

object AppIdUtil {
    /*
    val chars: Array[Byte] = {
        val values = new Array[Byte](32)
        val g = 'g'.toInt
        val G = 'G'.toInt
        for (i <- 0 to 15) {
            values(i) = (g + i).toByte
        }
        for (i <- 0 to 15) {
            values(16 + i) = (G + i).toByte
        }
        values
    }
    */
    val chars: Array[Byte] = {
        val values = new Array[Byte](32)
        for (i <- 0 to 9) {
            values(i) = ('0' + i).toByte
        }
        for (i <- 0 to 21) {
            values(i + 10) = ('a' + i).toByte
        }
        values
    }

    def hashCode16To8(hashCode16: String): String = {
        val values = new Array[Byte](8)
        val codeIndex = hashCode16.toLowerCase.map(chr => {
            if (chr >= '0' && chr <= '9') {
                chr - '0'
            } else {
                (chr - 'a') + 10
            }
        })
        for (i <- 0 to 7) {
            val index = codeIndex(i) + codeIndex(i + 8) + 1
            values(i) = chars(index)
        }
        new String(values)
    }
}
