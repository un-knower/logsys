package cn.whaley.bi.logsys.log2parquet.processor

/**
  * Created by michael on 2017/6/14.
  */
class ProcessorChain(val chainStr: String) {
  private val chain: Array[Processor] = init

  def init = {
    val initChain = ChainUtil.getChain(chainStr)
    initChain.foreach(x => x.init())
    initChain
  }

  def length = chain.length

  def execute() = {
    chain.foreach(p => {
        p.process()

    })
  }

  def destroy() = chain.foreach(x => x.destroy())
}
