package cn.whaley.bi.logsys.forest

import cn.whaley.bi.logsys.forest.Traits.ExecutedTrait

/**
 * Created by fj on 16/10/30.
 *
 * forest程序主入口
 */
object MainObj {
    /**
     * 程序入口
     * @param args
     * 第一个参数为运行的类名,之后的所有参数为该类所需要的入参列表,如:
     * MsgProcExecutor -s 20160601
     * 类名以.开头，代表与cn.whaley.bi.logsys.forest的相对路径
     * 类名不以.开头，且包含.则代表全路径名
     * 类名不以.开头，且不包含.则代表cn.whaley.bi.logsys.forest下的类
     */
    def main(args: Array[String]) {
        require(args.length >= 1)

        val cls = args(0)
        val index = cls.indexOf('.')
        val clsQualityName = if (index < 0) {
            s"${this.getClass.getPackage.getName}.${cls}"
        } else if (index == 0) {
            s"${this.getClass.getPackage.getName}${cls}"
        } else {
            cls
        }

        val clz = Class.forName(clsQualityName)
        val executedTrait = clz.newInstance().asInstanceOf[ExecutedTrait]

        val execArgs = args.toList.tail.toArray
        executedTrait.execute(execArgs)
    }

}
