package cn.whaley.bi.logsys.common

/**
 * Created by fj on 17/4/6.
 * AppId构造器
 */
object AppIdCreator {
    /**
     * args[0]: orgCode: String
     * args[1]: productCode: String
     * args[2]: appCode: String
     * @param args
     */
    def main(args: Array[String]): Unit = {
        if (args.length != 3) {
            System.err.println("必须提供3个参数[orgCode,productCode,appCode]")
            System.exit(1)
        }
        val util = new AppIdUtil
        val appId = util.createAppId(args(0), args(1),args(2))
        System.out.println(appId)
        System.exit(0)
    }
}
