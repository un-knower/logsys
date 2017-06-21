package cn.whaley.bi.logsys.log2parquet.traits

/**
 * Created by fj on 16/11/10.
 *
 * 名称Trait
 */
trait NameTrait {

    val name = this.getClass.getSimpleName
}
