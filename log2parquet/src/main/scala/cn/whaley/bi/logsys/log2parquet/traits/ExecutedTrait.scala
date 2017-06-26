package cn.whaley.bi.logsys.log2parquet.traits

/**
 * Created by michael on 2017/6/22.
 *
 * 可执行类的公共特质
 */
trait ExecutedTrait {
    def execute(args: Array[String]): Unit

    def shutdown(wait:Boolean=true):Unit
}
