package cn.whaley.bi

import cn.whaley.bi.traits.ExecutedTrait

/**
 * Created by fj on 16/9/28.
 */
object MainObj {

    /**
     * 程序入口
     * @param args
     * 第一个参数为运行的类名,之后的所有参数为该类所需要的入参列表,如:
     * MoviePlayStatics -s 20160601
     */
    def main(args: Array[String]) {
        require(args.length >= 1)

        val cls = args(0)
        val clsQualityName = if (cls.startsWith("com.moretv")) {
            cls
        } else {
            s"${this.getClass.getPackage.getName}.medusa.${cls}"
        }

        val clz = Class.forName(clsQualityName)
        val executedTrait = clz.newInstance().asInstanceOf[ExecutedTrait]

        val execArgs = args.toList.tail.toArray
        executedTrait.execute(execArgs)
    }
}
