package cn.whaley.bi.logsys.log2parquet.processor

/**
  * Created by michael on 2017/6/14.
  */
object ChainUtil {
  def getChain(chain:String) = {
    val chainArr = chain.split(",")
    chainArr.map(x => {
      val clazz = Class.forName(x)
      clazz.newInstance().asInstanceOf[Processor]
    })
  }
}
