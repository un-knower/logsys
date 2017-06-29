package cn.whaley.bi.logsys.log2parquet

import cn.whaley.bi.logsys.log2parquet.traits.ExecutedTrait
import org.slf4j.LoggerFactory

/**
 * Created by michael on 2017/6/22.
 *
 * log2parquet程序主入口
 */
object MainObj {

    val LOG=LoggerFactory.getLogger(this.getClass)

    /**
      * 程序入口
      *
      * @param args
      * 第一个参数为运行的类名,之后的所有参数为该类所需要的入参列表,如:
      * 处理多个appId
      * MsgProcExecutor --c prop.inputPath=/data_warehouse/ods_origin.db/log_origin
      * 或者具体到某个appId的某个小时
      * MsgProcExecutor --c prop.inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13
      * 类名以.开头，代表与cn.whaley.bi.logsys.log2parquet的相对路径
      * 类名不以.开头，且包含.则代表全路径名
      * 类名不以.开头，且不包含.则代表cn.whaley.bi.logsys.log2parquet下的类
     */
    def main(args: Array[String]) {
        require(args.length >= 1)

        println("args:")
        for (elem<-args){
            println(elem)
        }



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
        println("execArgs:")
        for (elem<-execArgs){
            println(elem)
        }
        executedTrait.execute(execArgs)
    }
}
