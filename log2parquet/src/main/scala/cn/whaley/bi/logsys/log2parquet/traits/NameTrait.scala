package cn.whaley.bi.logsys.log2parquet.traits

/**
 * Created by michael on 2017/6/22.
 *
 * 名称Trait
 */
trait NameTrait {

    val name = this.getClass.getSimpleName
}
