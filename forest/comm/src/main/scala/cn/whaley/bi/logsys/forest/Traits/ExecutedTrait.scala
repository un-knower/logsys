package cn.whaley.bi.logsys.forest.Traits

/**
 * Created by fj on 16/11/10.
 *
 * 可执行类的公共特质
 */
trait ExecutedTrait {
    def execute(args: Array[String]): Unit

    def shutdown(wait:Boolean=true):Unit
}
