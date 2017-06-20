package cn.whaley.bi.logsys.log2parquet.traits

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.utils.StringUtil

/**
 * Created by fj on 16/11/10.
 *
 * 可初始化Trait
 */
trait InitialTrait {
    /**
     * 初始化方法
     * 如果初始化异常，则应该抛出异常
     */
    def init(confManager: ConfManager): Unit

    def instanceFrom(confManager: ConfManager, confName: String): InitialTrait = {
        val include = confManager.getConf(s"${confName}.include")
        if (include != null && include.trim != "") {
            confManager.addConfResource(StringUtil.splitStr(include, ","): _*)
        }
        val cls = confManager.getConf(s"${confName}.class")
        val clz = Class.forName(cls)
        val instance = clz.newInstance().asInstanceOf[InitialTrait]
        instance.init(confManager)
        instance
    }

}
